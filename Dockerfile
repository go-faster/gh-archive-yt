FROM gcr.io/distroless/static

ADD gh-archive-yt /usr/local/bin/gh-archive-yt

ENTRYPOINT ["gh-archive-yt"]
