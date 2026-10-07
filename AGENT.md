# AGENT.md

AI agent working guide for the datasync-demo project. Read this first before making any changes.

## Project Overview

MySQL 数据同步工具：轮询源库 `user` 表增量数据，批量 Upsert 写入目标库 `user_copy` 表。支持全量同步 + 增量同步自动切换，同步位点持久化在 Redis，服务重启可断点续传。

**适用场景**：学习轮询式数据同步的实现思路。生产环境大数据量场景应优先选择 Canal / Flink CDC 等 binlog 方案。

## Tech Stack

- Go 1.24（CI 使用 1.26）
- go-zero v1.10.3（配置加载 + Redis 客户端）
- GORM v1.25.11（ORM，使用 `clause.OnConflict` 实现 Upsert）
- MySQL 8.0（源库 `source_db` + 目标库 `target_db`）
- Redis 6.x+（同步状态与位点持久化）

## Build & Run

```bash
# 拉取依赖
go mod tidy

# 编译
go build -o datasync .

# 运行（默认读取 etc/datasync.yaml）
go run . -f etc/datasync.yaml

# 代码检查
go vet ./...
```

CI 在 `.github/workflows/go.yml`，触发条件：push/PR 到 main 分支。

## Project Structure

```
datasync.go              # 程序入口：加载配置 → 初始化 Redis → 创建 SyncService → 启动轮询 → 优雅退出
etc/datasync.yaml        # 配置文件：源/目标库 DSN、Redis 连接、轮询间隔
internal/config/config.go  # go-zero 配置结构体
model/
  sourcemodel.go         # SourceUser 模型 + 增量/全量查询（ListSourceUserByTime）
  targetmodel.go         # TargetUser 模型 + 批量 Upsert（BatchUpsertTargetUser）
service/
  syncer.go              # 核心同步逻辑：StartPoll 轮询 + SyncUserWorker 同步执行
  cursor.go              # 旧版 JSON 位点（LoadCursor/SaveCursor）——已废弃，当前不再调用
utils/chunk.go           # 泛型切片分块工具（尚未接入写库路径）
test_data_user.sql       # 批量测试数据（id 5~32，含逻辑删除与边界数据）
```

## Architecture

### 同步模式切换

Redis key `full_sync_finished` 控制走全量还是增量：

- **全量同步**（`full_sync_finished` 不存在或非 `"1"`）：按 `id ASC` 分页扫描全表，使用独立游标 `full_sync_last_id`
- **增量同步**（`full_sync_finished = "1"`）：按 `update_time, id` 双游标分页，使用 `last_sync_id` + `last_sync_time`
- 全量扫描完毕（某轮查询为空）→ 自动 `SET full_sync_finished "1"` + `DEL full_sync_last_id`，下一轮切换增量

### 调用链

```
main → NewSyncService (连接 DB) → StartPoll (goroutine)
                                    ├── loadIncrementPoint() 每轮从 Redis 加载增量位点
                                    └── SyncUserWorker
                                          ├── 读 Redis: full_sync_finished → 判断全量/增量
                                          ├── 全量: 读 full_sync_last_id (默认0)
                                          │   增量: 用传入的 lastId/lastSyncTime
                                          ├── ListSourceUserByTime (查询源表)
                                          ├── BatchUpsertTargetUser (批量写入目标表)
                                          ├── 写 Redis: last_sync_id + last_sync_time
                                          └── 全量: 额外写 full_sync_last_id
```

### Redis Keys

| Key | 用途 |
|---|---|
| `full_sync_finished` | `"1"` = 全量已完成走增量；不存在 = 走全量 |
| `full_sync_last_id` | 全量同步游标，全量完成后自动删除 |
| `last_sync_id` | 增量同步 id 位点 |
| `last_sync_time` | 增量同步 update_time 位点 |
| `sync:user:cursor` | （已废弃）旧版 JSON 位点，代码不再调用 |

重置全部位点到重新全量：`DEL full_sync_finished full_sync_last_id last_sync_id last_sync_time`

## Code Conventions

- **语言**：注释和日志使用中文，代码标识符使用英文
- **commit**：Conventional Commits，`fix:` / `feat:` 前缀 + 中文描述，body 用 `-` 列举要点
- **错误处理**：Redis 读写需检查 err，失败时记日志并提前返回；GORM 操作通过 `fmt.Errorf` 包装错误上下文
- **位点推进**：只有同步成功（BatchUpsert + Redis 写入均成功）才推进游标，失败时下轮 tick 自动重试
- **Go 版本**：本地 1.24，CI 1.26。不要使用超出 1.24 的语言特性

## Key Design Decisions

### 全量同步使用独立游标

全量同步不读 `last_sync_id`（可能残留旧增量值导致从中间开始漏同步历史数据），而是使用独立的 `full_sync_last_id`（默认 0 从头扫描）。这是历史数据漏同步问题的核心修复点。

### 全量同步期间不更新外层增量游标

`StartPoll` 中全量同步完成后不更新 `lastId`/`lastUpdateTime` 局部变量，每轮重新从 Redis `loadIncrementPoint()` 加载。全量同步的真正游标在 Redis `full_sync_last_id`，外层局部变量在全量期间不参与查询。

### 全量→增量切换时 last_sync_time 的取值

全量完成后 `last_sync_time` = 最大 id 行的 `update_time`（非全局 MAX(update_time)）。首次增量轮询可能重复处理少量已同步数据，Upsert 幂等不影响正确性。用 MAX(update_time) 有丢数据风险（全量扫描期间被更新的行可能被漏掉），安全性优先。

## Known Limitations

- 无分布式锁，多实例部署会重复同步
- 源库物理 `DELETE` 不会传导（业务使用 `is_deleted` 逻辑删除）
- 强依赖 `update_time` 正确维护，时间回拨会造成漏同步
- 单轮固定 500 条（`syncer.go` 中的 `batchSize`），积压时需多轮追赶
- `utils.Chunk` 分块工具已实现但尚未接入写库路径
- `service/cursor.go` 的 `LoadCursor`/`SaveCursor`（`sync:user:cursor` JSON key）已废弃为死代码，清理待办

## Database Schema

源表 `source_db.user` 和目标表 `target_db.user_copy` 结构对齐，DDL 见 README.md。源表需建索引 `KEY idx_update_time (update_time, id)` 与增量查询条件一致。

## When Modifying

- 改同步逻辑时注意 Redis 读写错误分支的返回值一致性
- `SyncUserWorker` 的返回值 `(int64, time.Time, bool, error)` 分别是 `lastId, lastSyncTime, isFullSync, err`，调用方依赖前两个推进游标
- 全量/增量的查询逻辑在 `model/sourcemodel.go` 的 `ListSourceUserByTime`，由 `fullsync bool` 参数切换
- Upsert 冲突列为 `id`，`UpdateAll: true`，修改目标表结构时注意对齐
