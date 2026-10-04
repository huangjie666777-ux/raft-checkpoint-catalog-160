# raft-lease-coordinator

三节点租约锁后端：基于 HashiCorp Raft 1.7.3（Go 1.27.1）的多进程独占资源协调服务。
每个节点是独立进程，拥有独立数据目录，通过真实 TCP 复制日志，HTTP 对外提供
锁的申请、续租、释放与查询。

## 结构

| 文件 | 职责 |
| --- | --- |
| internal/fsm/fsm.go | 租约、屏障与检查点目录状态机：命令判定、防护令牌、快照与恢复 |
| internal/fsm/checkpoint_test.go | 分布式检查点目录自测：绑定、原子登记、发布、代单调、快照恢复 |
| `internal/node/node.go` | Raft 节点：TCP 传输、BoltDB 日志/任期/投票持久化、首次引导 |
| internal/api/http.go | HTTP API：leader 校验、请求校验、租约、屏障与检查点端点、提交超时处理 |
| `cmd/lockd/main.go` | 进程入口：解析配置、启动节点、信号退出时关闭网络与存储 |
| `internal/fsm/fsm_test.go` | 租约状态机自测（争用、令牌单调、过期、快照恢复） |
| `internal/fsm/barrier_test.go` | 屏障自测（幂等创建、会合、租约失效判负、推进、快照恢复、旧快照兼容） |

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
  - 查询本身也是一条 Raft 命令：按已提交日志判定，follower 不读本地状态。

## 阶段屏障协议

命名屏障让 2-16 个并行进程按“轮”会合：全员到齐才完成本轮，推进后才开启下一轮。
屏障与租约共用一条 Raft 日志，判定时刻由 leader 写入命令（`Command.Now`），副本只重放，
不读本地时钟；等待中的租约失效最迟在下一次屏障操作（含查询）时被判定为失败。

- `POST /barrier/create` `{"name":"phase","participants":["w1","w2"]}`
  - 参与者 2-16 个不同 ID，成员固定，轮号从 1 开始。
  - 同名同配置幂等成功；同名异配置返回 `409`。
- `POST /barrier/arrive` `{"name":"phase","round":1,"participant":"w1","resource":"res-w1","holder":"w1","token":1}`
  - 参与者须先用 `/acquire` 取得独立资源租约，再携资源名、持有人与令牌报到。
  - 只接纳与当前有效租约匹配的登记；同轮同一资源不得被两人登记。
  - 同一登记重发幂等不重复计数；未知参与者、旧轮号、同人不同登记一律拒绝。
  - 全员到齐时在同一提交中再次校验全部登记租约，全部有效才标记 `completed`。
- `POST /barrier/advance` `{"name":"phase","round":1}`
  - 必须携带预期轮号；仅 `completed`/`failed` 终态可推进，推进后轮号 +1 并清空到达记录。
  - 并发推进只有一个生效，其余因轮号不匹配被拒绝；终态不倒退。
- `GET /barrier?name=phase`
  - 返回轮号、状态（`waiting`/`completed`/`failed`）、已到达 ID 列表与失败原因。

失败语义：等待期间任一已登记租约被释放、到期或换令牌（资源被重取），本轮判
`failed` 并记录原因，不接受替补登记；推进后才能开始新一轮。

快照同时保存屏障配置、轮号、状态与到达登记，随租约一同恢复；不含屏障字段的旧
快照照常兼容。重启与换主不丢状态，也不接纳旧轮请求。

## 分布式检查点目录

检查点按任务名和正整数代号标识，创建时绑定一个已有屏障的当前等待轮。一个屏障轮至多绑定一个检查点；相同任务、代号和绑定重发幂等，改变绑定返回冲突。只有成功发布才推进任务最新代，后续发布代号必须高于最新已发布代号。

- POST /checkpoints：请求 task、generation、barrier；返回绑定轮和发布状态。GET /checkpoints?task=job&generation=1 查询指定检查点。
- POST /checkpoints/register：请求 checkpoint 身份、participant、resource、holder、token、uri、sha256、size。sha256 为 64 位小写十六进制，size 为非负字节数；服务只保存元数据，不访问分片。
- 登记命令在同一个 Raft 提交中写入屏障到达和候选分片；任一侧失败则两者都不落状态。相同请求幂等，任一字段改变返回冲突。
- 已绑定检查点的屏障轮拒绝旧 POST /barrier/arrive，不能绕过分片齐全判定。
- POST /checkpoints/publish：绑定轮仍存在且 completed，每个成员都有相互匹配的到达和分片元数据才发布。
- 未到齐、失败或已推进的未发布轮拒绝；发布清单不可变，并原子更新任务最新代。相同发布重试返回原清单；之后推进或租约失效不改变结果，失败候选不覆盖旧清单。
- GET /checkpoints/latest?task=job：返回任务最新已发布清单，shards 按参与者 ID 排序，供重启后的计算任务读取完整分片。

快照保存屏障轮绑定、候选分片、已发布清单与任务最新代；不含这些字段的旧快照仍可恢复。多租约同时失效时按屏障名和参与者 ID 排序判定，副本生成一致的失败原因。

## 一致性设计

- 判定时刻由 leader 写入日志命令（`Command.Now`），副本重放不读取本地时钟；
  所有操作按已提交日志顺序在状态机中判定，并发争用只授予一方。
- 防护令牌按资源单调递增，上界保存在快照中，释放/到期/重启/快照恢复均不回退。
- 日志、任期、投票持久化在 BoltDB（`data/<id>/raft.db`）；快照包含当前租约与
  每资源令牌上界，重启后已提交状态不丢。
- leader 失效后剩余两节点可选举并继续服务；不足多数派时不授予任何锁。

## 演示（curl）

### 租约

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

### 屏障会合与失败推进

```sh
# 创建两人屏障（幂等；异配置返回 409）
curl -X POST 127.0.0.1:8101/barrier/create -d '{"name":"phase","participants":["w1","w2"]}'
# 各自先取租约
curl -X POST 127.0.0.1:8101/acquire -d '{"resource":"res-w1","holder":"w1","ttl":120}'
curl -X POST 127.0.0.1:8101/acquire -d '{"resource":"res-w2","holder":"w2","ttl":120}'
# 携令牌报到；第二人到齐即 completed
curl -X POST 127.0.0.1:8101/barrier/arrive -d '{"name":"phase","round":1,"participant":"w1","resource":"res-w1","holder":"w1","token":1}'
curl -X POST 127.0.0.1:8101/barrier/arrive -d '{"name":"phase","round":1,"participant":"w2","resource":"res-w2","holder":"w2","token":1}'
# 带预期轮号推进到第 2 轮
curl -X POST 127.0.0.1:8101/barrier/advance -d '{"name":"phase","round":1}'

# 失败场景：w1 报到后租约被释放，下一次屏障操作判定本轮 failed
curl -X POST 127.0.0.1:8101/barrier/arrive -d '{"name":"phase","round":2,"participant":"w1","resource":"res-w1","holder":"w1","token":1}'
curl -X POST 127.0.0.1:8101/release -d '{"resource":"res-w1","holder":"w1","token":1}'
curl "127.0.0.1:8101/barrier?name=phase"   # status=failed，含 fail_reason
curl -X POST 127.0.0.1:8101/barrier/advance -d '{"name":"phase","round":2}'  # 失败后仍可推进
```

### 检查点登记、发布与恢复查询

先创建参与者为 w1/w2 的屏障 job-phase，取得两人的独立租约，然后：

1. 创建并绑定检查点：
   curl -X POST 127.0.0.1:8101/checkpoints -d '{"task":"job","generation":1,"barrier":"job-phase"}'
2. w1 登记：
   curl -X POST 127.0.0.1:8101/checkpoints/register -d '{"task":"job","generation":1,"participant":"w1","resource":"job-w1","holder":"w1","token":1,"uri":"s3://bucket/job/1/w1","sha256":"1111111111111111111111111111111111111111111111111111111111111111","size":4096}'
3. w2 登记；第二条到齐后屏障 completed：
   curl -X POST 127.0.0.1:8101/checkpoints/register -d '{"task":"job","generation":1,"participant":"w2","resource":"job-w2","holder":"w2","token":1,"uri":"s3://bucket/job/1/w2","sha256":"2222222222222222222222222222222222222222222222222222222222222222","size":8192}'
4. 发布并查询恢复所需清单：
   curl -X POST 127.0.0.1:8101/checkpoints/publish -d '{"task":"job","generation":1}'
   curl "127.0.0.1:8101/checkpoints?task=job&generation=1"
   curl "127.0.0.1:8101/checkpoints/latest?task=job"
