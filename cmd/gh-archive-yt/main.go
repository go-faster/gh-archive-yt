package main

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"time"

	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/app"
	"github.com/go-faster/sdk/zctx"
	"github.com/mergestat/timediff"
	"github.com/opentracing/opentracing-go"
	"github.com/spf13/cobra"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelBridge "go.opentelemetry.io/otel/bridge/opentracing"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	ytzap "go.ytsaurus.tech/library/go/core/log/zap"
	"go.ytsaurus.tech/yt/go/migrate"
	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yson/yson2json"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/ythttp"
	"golang.org/x/sync/errgroup"

	"github.com/go-faster/gh-archive-yt/internal/gh"
)

type Service struct {
	token   string
	lg      *zap.Logger
	table   ypath.Path
	yc      yt.Client
	batches chan []gh.Event

	fetchedCount metric.Int64Counter
	missCount    metric.Int64Counter

	rateLimitRemaining atomic.Int64
	rateLimitUsed      atomic.Int64
	rateLimitReset     atomic.Float64
	targetRate         atomic.Float64

	tracer        trace.Tracer
	httpTransport *otelhttp.Transport
}

type Event struct {
	ID   int64  `yson:"id"`
	Time uint64 `yson:"ts"`
	Data any    `yson:"body"`
}

func (Event) Schema() schema.Schema {
	return schema.Schema{
		UniqueKeys: true,
		Columns: []schema.Column{
			{Name: "ts", ComplexType: schema.TypeTimestamp, SortOrder: schema.SortAscending},
			{Name: "id", ComplexType: schema.TypeInt64, SortOrder: schema.SortAscending},
			{Name: "body", ComplexType: schema.Optional{Item: schema.TypeAny}},
		},
	}
}

func (c *Service) Migrate(ctx context.Context) error {
	tables := map[ypath.Path]migrate.Table{
		c.table: {
			Schema: Event{}.Schema(),
			Attributes: map[string]any{
				"optimize_for":      "scan",
				"compression_codec": "zstd_5",
			},
		},
	}

	rt := migrate.DeleteDataAfterTTL(24 * time.Hour * 14) // 2 weeks
	rt.FillAttrs(tables[c.table].Attributes)

	if err := migrate.EnsureTables(ctx, c.yc, tables, migrate.OnConflictDrop(ctx, c.yc)); err != nil {
		return errors.Wrap(err, "ensure tables")
	}

	return nil
}

func (c *Service) Send(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case batch := <-c.batches:
			if err := func() (err error) {
				ctx, span := c.tracer.Start(ctx, "Send",
					trace.WithAttributes(
						attribute.Int("batch_size", len(batch)),
					),
				)
				defer func() {
					if err != nil {
						span.RecordError(err)
					}
					span.End()
				}()

				bw := c.yc.NewRowBatchWriter()
				for _, e := range batch {
					if err := bw.Write(Event{
						ID:   e.ID,
						Time: uint64(e.CreatedAt.Unix()),
						Data: yson2json.RawMessage{JSON: json.RawMessage(e.Raw)},
					}); err != nil {
						return errors.Wrap(err, "write row")
					}
				}
				if err := bw.Commit(); err != nil {
					return errors.Wrap(err, "commit")
				}
				if err := c.yc.InsertRowBatch(ctx, c.table, bw.Batch(), &yt.InsertRowsOptions{}); err != nil {
					return errors.Wrap(err, "insert rows")
				}

				return nil
			}(); err != nil {
				return errors.Wrap(err, "send batch")
			}
		}
	}
}

func (c *Service) Poll(ctx context.Context) error {
	const (
		perPage  = 100
		maxPages = 10
	)

	client := gh.NewClient(&http.Client{Transport: c.httpTransport}, c.token)
	latestMet := make(map[int64]struct{})
	lg := c.lg.Named("poll")

	var etag string
Fetch:
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		var rt gh.RateLimit
		var newEvents []gh.Event
		var start time.Time

		currentMet := make(map[int64]struct{})
		for i := 0; i <= maxPages; i++ {
			start = time.Now()
			p := gh.Params{
				Page:    i + 1, // first page is 1
				PerPage: perPage,
			}
			if i == 0 {
				p.Etag = etag
			}
			lg.Info("Fetching events", zap.Int("page", p.Page))
			res, err := client.Events(ctx, p)
			if err != nil {
				return errors.Wrap(err, "failed to fetch events")
			}
			if res.NotModified {
				lg.Info("Not modified", zap.Duration("duration", time.Since(start)))
				continue Fetch
			}
			if res.Unprocessable {
				lg.Warn("Unable to resolve missing events")
				c.missCount.Add(ctx, 1)
				break
			}

			// Updating rate-limit to sleep later.
			rt = res.RateLimit
			c.rateLimitRemaining.Store(int64(rt.Remaining))
			c.rateLimitUsed.Store(int64(rt.Used))
			c.rateLimitReset.Store(time.Until(rt.Reset).Seconds())

			// Searching for new events.
			// The currentMet contains events from previous Fetch loop.
			for _, ev := range res.Data {
				if _, ok := currentMet[ev.ID]; ok {
					continue
				}
				currentMet[ev.ID] = struct{}{}
				if _, ok := latestMet[ev.ID]; !ok {
					newEvents = append(newEvents, ev)
				}
			}
			if etag == "" || len(newEvents) < (p.PerPage*p.Page) {
				if i == 0 {
					etag = res.Etag
				}
				break
			}
			// All events are new, fetching next page.
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case c.batches <- newEvents:
			// Insert events in background.
			c.fetchedCount.Add(ctx, int64(len(newEvents)))
		}

		// Calculating next sleep time to avoid rate limit.
		now := time.Now()
		var targetRate time.Duration
		if rt.Remaining < 10 {
			lg.Warn("Rate limit", zap.Int("remaining", rt.Remaining))
			targetRate = rt.Reset.Sub(now) + time.Second
		} else {
			targetRate = time.Until(rt.Reset) / time.Duration(rt.Remaining)
		}
		c.targetRate.Store(targetRate.Seconds())
		duration := time.Since(start)
		sleep := targetRate - duration
		if sleep <= 0 {
			sleep = 0
		}
		lg.Info("Events",
			zap.Duration("duration", duration),
			zap.Int("new_count", len(newEvents)),
			zap.Int("remaining", rt.Remaining),
			zap.Int("used", rt.Used),
			zap.Duration("reset", rt.Reset.Sub(now)),
			zap.String("reset_human", timediff.TimeDiff(rt.Reset)),
			zap.Duration("sleep", sleep),
			zap.Duration("target_rate", targetRate),
		)
		select {
		case <-time.After(sleep):
			latestMet = currentMet
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func main() {
	app.Run(func(ctx context.Context, lg *zap.Logger, m *app.Metrics) error {
		// Setting OpenTelemetry/OpenTracing Bridge.
		// https://github.com/open-telemetry/opentelemetry-go/tree/main/bridge/opentracing#opentelemetryopentracing-bridge
		otelTracer := m.TracerProvider().Tracer("yt")
		bridgeTracer, wrapperTracerProvider := otelBridge.NewTracerPair(otelTracer)
		opentracing.SetGlobalTracer(bridgeTracer)

		// Override for context propagation.
		otel.SetTracerProvider(wrapperTracerProvider)

		// Initializing metrics.
		meter := m.MeterProvider().Meter("")
		fetchedCount, err := meter.Int64Counter("events_fetched_count")
		if err != nil {
			return errors.Wrap(err, "failed to create counter")
		}
		fetchedCount.Add(ctx, 0) // init
		missCount, err := meter.Int64Counter("fetch_miss")
		if err != nil {
			return errors.Wrap(err, "failed to create counter")
		}
		missCount.Add(ctx, 0) // init
		targetRate, err := meter.Float64ObservableGauge("fetch_target_rate_seconds")
		if err != nil {
			return errors.Wrap(err, "failed to create gauge")
		}

		rateLimitRemaining, err := meter.Int64ObservableGauge("github_rate_limit_remaining")
		if err != nil {
			return errors.Wrap(err, "failed to create gauge")
		}
		rateLimitUsed, err := meter.Int64ObservableGauge("github_rate_limit_used")
		if err != nil {
			return errors.Wrap(err, "failed to create gauge")
		}
		rateLimitReset, err := meter.Float64ObservableGauge("github_rate_limit_reset_seconds")
		if err != nil {
			return errors.Wrap(err, "failed to create gauge")
		}

		yc, err := ythttp.NewClient(&yt.Config{
			Logger: &ytzap.Logger{L: zctx.From(ctx).Named("yt")},
			Tracer: bridgeTracer,
		})
		if err != nil {
			return errors.Wrap(err, "yt")
		}
		s := &Service{
			batches:      make(chan []gh.Event, 5),
			lg:           lg,
			token:        os.Getenv("GITHUB_TOKEN"),
			missCount:    missCount,
			fetchedCount: fetchedCount,

			table: ypath.Path("//go-faster").Child("github_events"),
			yc:    yc,

			tracer: wrapperTracerProvider.Tracer(""),
			httpTransport: otelhttp.NewTransport(http.DefaultTransport,
				otelhttp.WithTracerProvider(m.TracerProvider()),
				otelhttp.WithMeterProvider(m.MeterProvider()),
			),
		}
		if _, err := meter.RegisterCallback(func(ctx context.Context, observer metric.Observer) error {
			observer.ObserveInt64(rateLimitRemaining, s.rateLimitRemaining.Load())
			observer.ObserveInt64(rateLimitUsed, s.rateLimitUsed.Load())
			observer.ObserveFloat64(rateLimitReset, s.rateLimitReset.Load())
			observer.ObserveFloat64(targetRate, s.targetRate.Load())
			return nil
		}, rateLimitRemaining, rateLimitUsed, rateLimitReset, targetRate); err != nil {
			return errors.Wrap(err, "failed to register callback")
		}

		rootCmd := &cobra.Command{
			SilenceUsage:  true,
			SilenceErrors: true,

			Use: "gh-archive-yt",
			RunE: func(cmd *cobra.Command, args []string) error {
				if err := s.Migrate(ctx); err != nil {
					return errors.Wrap(err, "migrate")
				}
				g, ctx := errgroup.WithContext(ctx)
				g.Go(func() error {
					return s.Poll(ctx)
				})
				g.Go(func() error {
					return s.Send(ctx)
				})
				return g.Wait()
			},
		}
		return rootCmd.ExecuteContext(ctx)
	})
}
