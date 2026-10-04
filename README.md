# raft-lease-coordinator

三节点租约锁后端：基于 HashiCorp Raft 1.7.3（Go 1.27.1）的多进程独占资源协调服务。
每个节点是独立进程，拥有独立数据目录，通过真实 TCP 复制日志，HTTP 对外提供
锁的申请、续租、释放与查询。

## 结构

| 文件 | 职责 |
| --- | --- |
| `internal/fsm/fsm.go` | 租约状态机：申请/续租/释放判定、防护令牌上界、快照与恢复 |
| `internal/node/node.go` | Raft 节点：TCP 传输、BoltDB 日志/任期/投票持久化、首次引导 |
| `internal/api/http.go` | HTTP API：leader 校验、请求校验、提交超时处理 |
| `cmd/lockd/main.go` | 进程入口：解析配置、启动节点、信号退出时关闭网络与存储 |
| `internal/fsm/fsm_test.go` | 状态机自测（争用、令牌单调、过期、快照恢复） |

## 构建与测试

```sh
go build ./...
go test ./internal/...
go build -o bin/lockd ./cmd/lockd
```

## 配置

每个节点需要：节点 ID、Raft 地址、HTTP 地址、数据目录，以及固定的三名投票成员
（含自身，格式 `id=raftAddr=httpAddr`，逗号分隔）。仅在数据目录无任何既有状态时
才引导集群；已有日志/快照的节点不会重新引导。不提供动态成员管理。

## 启动三节点（同机）

```sh
PEERS="n1=127.0.0.1:7101=127.0.0.1:8101,n2=127.0.0.1:7102=127.0.0.1:8102,n3=127.0.0.1:7103=127.0.0.1:8103"
./bin/lockd -id n1 -raft-addr 127.0.0.1:7101 -http-addr 127.0.0.1:8101 -data-dir data/n1 -peers "$PEERS" &
./bin/lockd -id n2 -raft-addr 127.0.0.1:7102 -http-addr 127.0.0.1:8102 -data-dir data/n2 -peers "$PEERS" &
./bin/lockd -id n3 -raft-addr 127.0.0.1:7103 -http-addr 127.0.0.1:8103 -data-dir data/n3 -peers "$PEERS" &
```

## HTTP API

只有 leader 处理操作；follower 返回 `307` 及已知 leader 的 HTTP 地址，绝不在本地成功。
多数派提交并应用后才响应成功；提交超时返回 `503`，结果未确认。

- `POST /acquire` `{"resource":"r","holder":"h","ttl":30}`
  - `ttl` 取 1-300 秒。无有效租约才授予；成功返回 `holder`、`expiry`（Unix 秒）
    和逐资源递增的防护令牌 `token`。
  - 已被占用返回 `409` 及当前持有人信息。
- `POST /renew` `{"resource":"r","holder":"h","token":1,"ttl":30}`
  - 必须匹配持有人与令牌；过期请求拒绝且不影响后来持有人。
  - 截止时间从判定时刻（leader 写入命令的时间）重新计算。
- `POST /release` `{"resource":"r","holder":"h","token":1}`
  - 必须匹配持有人与令牌；释放不清除该资源的令牌上界。
- `GET /query?resource=r`
  - 返回当前有效租约；已到期返回 `{"ok":false}`（空）。

## 一致性设计

- 判定时刻由 leader 写入日志命令（`Command.Now`），副本重放不读取本地时钟；
  所有操作按已提交日志顺序在状态机中判定，并发争用只授予一方。
- 防护令牌按资源单调递增，上界保存在快照中，释放/到期/重启/快照恢复均不回退。
- 日志、任期、投票持久化在 BoltDB（`data/<id>/raft.db`）；快照包含当前租约与
  每资源令牌上界，重启后已提交状态不丢。
- leader 失效后剩余两节点可选举并继续服务；不足多数派时不授予任何锁。

## 演示（curl）

```sh
# 争用：alice 成功，bob 冲突
curl -X POST 127.0.0.1:8101/acquire -d '{"resource":"resA","holder":"alice","ttl":30}'
curl -X POST 127.0.0.1:8101/acquire -d '{"resource":"resA","holder":"bob","ttl":30}'
# 续租（需匹配 token）
curl -X POST 127.0.0.1:8101/renew -d '{"resource":"resA","holder":"alice","token":1,"ttl":60}'
# 查询 / 释放
curl "127.0.0.1:8101/query?resource=resA"
curl -X POST 127.0.0.1:8101/release -d '{"resource":"resA","holder":"alice","token":1}'
# 释放后重取获得更大令牌
curl -X POST 127.0.0.1:8101/acquire -d '{"resource":"resA","holder":"bob","ttl":30}'
```
