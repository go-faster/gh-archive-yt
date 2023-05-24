# gh-archive-yt [![Go Reference](https://img.shields.io/badge/go-pkg-00ADD8)](https://pkg.go.dev/github.com/go-faster/gh-archive-yt#section-documentation) [![codecov](https://img.shields.io/codecov/c/github/go-faster/gh-archive-yt?label=cover)](https://codecov.io/gh/go-faster/gh-archive-yt) [![experimental](https://img.shields.io/badge/-experimental-blueviolet)](https://go-faster.org/docs/projects/status#experimental)

Archive GitHub events to [YTsaurus](https://ytsaurus.tech/).

<img src="screen.png" alt="screen">

## Setup

### Users

```bash
export YT_CONFIG_PATCHES='{proxy={enable_proxy_discovery=%false}}'
export YT_TOKEN=token
export YT_PROXY=http-yt.go-faster.org

yt create user --attr '{name=gh-archive-yt}'
yt issue-token gh-archive-yt
```

Also create account `gh-archive-yt` in UI and set appropriate quotas.

### Directory

```bash
yt create map_node //go-faster --attributes '{owner=gh-archive-yt; account=gh-archive-yt}'
```

### ACL

```bash
cat _hack/acl.yson | yt set //go-faster/@acl
cat _hack/acl.account.yson | yt set //sys/accounts/gh-archive-yt/@acl
```
