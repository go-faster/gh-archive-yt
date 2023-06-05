package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/app"
	"github.com/go-faster/sdk/zctx"
	"github.com/go-faster/yt/migrate"
	"github.com/go-faster/yt/schema"
	"github.com/go-faster/yt/ypath"
	"github.com/go-faster/yt/yson/yson2json"
	"github.com/go-faster/yt/yt"
	"github.com/go-faster/yt/yt/ythttp"
	"github.com/mergestat/timediff"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	ytzap "go.ytsaurus.tech/library/go/core/log/zap"
	"golang.org/x/sync/errgroup"

	"github.com/go-faster/gh-archive-yt/internal/gh"
)

type Service struct {
	token       string
	lg          *zap.Logger
	table       ypath.Path
	staticTable ypath.Path
	yc          yt.Client
	batches     chan []gh.Event

	fetchedCount metric.Int64Counter
	missCount    metric.Int64Counter

	rateLimitRemaining atomic.Int64
	rateLimitUsed      atomic.Int64
	rateLimitReset     atomic.Float64
	targetRate         atomic.Float64
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
				"dynamic":           true,
				"optimize_for":      "scan",
				"compression_codec": "zstd_5",
			},
		},
		c.staticTable: {
			Schema: Event{}.Schema(),
			Attributes: map[string]any{
				"dynamic":           false,
				"optimize_for":      "scan",
				"compression_codec": "zstd_5",
			},
		},
	}
	if err := EnsureTables(ctx, c.yc, tables, migrate.OnConflictDrop(ctx, c.yc)); err != nil {
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
			for _, e := range batch {
				bw := c.yc.NewRowBatchWriter()
				if err := bw.Write(Event{
					ID:   e.ID,
					Time: uint64(e.CreatedAt.Unix()),
					Data: yson2json.RawMessage{JSON: json.RawMessage(e.Raw)},
				}); err != nil {
					return errors.Wrap(err, "write row")
				}
				if err := bw.Commit(); err != nil {
					return errors.Wrap(err, "commit")
				}
				if err := c.yc.InsertRowBatch(ctx, c.table, bw.Batch(), &yt.InsertRowsOptions{}); err != nil {
					return errors.Wrap(err, "insert rows")
				}
			}
		}
	}
}

func (c *Service) Poll(ctx context.Context) error {
	const (
		perPage  = 100
		maxPages = 10
	)

	client := gh.NewClient(http.DefaultClient, c.token)
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

func (c *Service) FromDynamicToStatic(ctx context.Context) error {
	ts, err := c.lastTS(ctx)
	if err != nil {
		return err
	}

	query := fmt.Sprintf("* FROM [%s] WHERE ts > %d ORDER BY ts DESC LIMIT 500", c.table.String(), ts)

	r, err := c.yc.SelectRows(ctx, query, &yt.SelectRowsOptions{})
	if err != nil {
		return err
	}
	defer func() {
		_ = r.Close()
	}()

	for r.Next() {
		var event Event

		if err := r.Scan(&event); err != nil {
			return errors.Wrap(err, "scan")
		}

		wr, err := c.yc.WriteTable(ctx, c.staticTable.Rich().SetAppend(), &yt.WriteTableOptions{})
		if err != nil {
			return err
		}

		if err := wr.Write(event); err != nil {
			_ = wr.Rollback()
			return err
		}

		if err := wr.Commit(); err != nil {
			_ = wr.Rollback()
			return err
		}
	}

	if err := r.Err(); err != nil {
		return errors.Wrap(err, "iter err")
	}

	return nil
}

func (c *Service) lastTS(ctx context.Context) (uint64, error) {
	var res int64

	if err := c.yc.GetNode(ctx, c.staticTable.Attr("row_count"), &res, &yt.GetNodeOptions{}); err != nil {
		return 0, errors.Wrap(err, "get node")
	}

	if res == 0 {
		return 0, nil
	}

	res--

	path := c.staticTable.Rich().AddRange(ypath.StartingFrom(ypath.RowIndex(res)))

	r, err := c.yc.ReadTable(ctx, path.YPath(), &yt.ReadTableOptions{})
	if err != nil {
		return 0, errors.Wrap(err, "read table")
	}

	var e Event

	for r.Next() {
		if err := r.Scan(&e); err != nil {
			return 0, errors.Wrap(err, "scan")
		}
	}
	defer func() {
		_ = r.Close()
	}()

	return e.Time, nil
}

func (c *Service) eventsTTL(ctx context.Context) error {
	ttl := time.Now().Add(-(time.Minute)).Unix()

	tablePath := fmt.Sprintf("%s[:(ts,%d)]", c.staticTable.String(), ttl)
	spec := map[string]any{
		"table_path": tablePath,
	}

	if _, err := c.yc.StartOperation(ctx, yt.OperationErase, spec, &yt.StartOperationOptions{}); err != nil {
		return err
	}

	return nil
}

func main() {
	app.Run(func(ctx context.Context, lg *zap.Logger, m *app.Metrics) error {
		g, ctx := errgroup.WithContext(ctx)

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
			Logger: &ytzap.Logger{L: zctx.From(ctx)},
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

			table:       ypath.Path("//go-faster").Child("github_events"),
			staticTable: ypath.Path("//go-faster").Child("github_events_static"),
			yc:          yc,
		}
		if err := s.Migrate(ctx); err != nil {
			return errors.Wrap(err, "migrate")
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

		g.Go(func() error {
			return s.Poll(ctx)
		})
		g.Go(func() error {
			return s.Send(ctx)
		})
		g.Go(func() error {
			for range time.Tick(50 * time.Second) {
				if err := s.FromDynamicToStatic(context.Background()); err != nil {
					lg.Error("from dynamic", zap.NamedError("err", err))
				}
				if err := s.eventsTTL(context.Background()); err != nil {
					lg.Error("events ttl", zap.NamedError("err", err))
				}
			}
			return nil
		})
		return g.Wait()
	})
}

// EnsureTables is migrate.EnsureTables but without hardcoded dynamic attribute.
func EnsureTables(
	ctx context.Context,
	yc yt.Client,
	tables map[ypath.Path]migrate.Table,
	onConflict migrate.ConflictFn,
) error {
	for path, table := range tables {
		var attrs struct {
			Schema      schema.Schema `yson:"schema"`
			TabletState string        `yson:"expected_tablet_state"`
			Dynamic     bool          `yson:"dynamic"`
		}
		isDynamic := true

	retry:
		ok, err := yc.NodeExists(ctx, path, nil)
		if err != nil {
			return err
		}

		if !ok {
			attrs := make(map[string]interface{})
			for k, v := range table.Attributes {
				attrs[k] = v
			}

			attrs["schema"] = table.Schema
			if dynamic, ok := attrs["dynamic"]; ok {
				val, ok := dynamic.(bool)
				if ok {
					isDynamic = val
				}
			}

			rc := migrate.DeleteDataAfterTTL(5 * time.Minute)
			rc.FillAttrs(attrs)

			if _, err = yc.CreateNode(ctx, path, yt.NodeTable, &yt.CreateNodeOptions{
				Recursive:  true,
				Attributes: attrs,
			}); err != nil {
				return err
			}
		} else {
			opts := &yt.GetNodeOptions{Attributes: []string{
				"schema",
				"expected_tablet_state",
				"dynamic",
			}}

			if err := yc.GetNode(ctx, path.Attrs(), &attrs, opts); err != nil {
				return err
			}

			fixUniqueKeys := func(s schema.Schema) schema.Schema {
				if len(s.Columns) > 0 && s.Columns[0].SortOrder != schema.SortNone {
					s.UniqueKeys = true
				}

				return s
			}

			if !attrs.Schema.Equal(fixUniqueKeys(table.Schema)) {
				switch err = onConflict(path, attrs.Schema, table.Schema); {
				case errors.As(err, migrate.RetryConflict):
					goto retry

				case err != nil:
					return err
				}
			}
			isDynamic = attrs.Dynamic
		}

		if isDynamic && attrs.TabletState != yt.TabletMounted {
			if err := migrate.MountAndWait(ctx, yc, path); err != nil {
				return err
			}
		}
	}

	return nil
}
