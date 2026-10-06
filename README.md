# datasync-demo

基于 **go-zero + GORM** 实现的 MySQL 增量数据同步 Demo：使用 `update_time + id` 双游标轮询源库增量数据，批量 Upsert 写入目标库，同步位点持久化在 Redis。

> ⚠️ 本项目用于学习「轮询式增量同步」的实现思路；生产环境大数据量场景请优先选择 Canal / Flink CDC 等 binlog 方案。

## Features

- 全量同步 + 增量同步自动切换：Redis `full_sync_finished` 标记全量是否完成，首次启动自动走全量，全量完成后切换增量
- 全量同步使用独立游标 `full_sync_last_id`，不受增量位点 `last_sync_id` 旧值影响，避免历史数据漏同步
- 双游标 `update_time + id` 分页（增量模式），避免同一时间戳多条数据被 `LIMIT` 截断后丢失
- GORM `clause.OnConflict` Upsert：主键存在则更新，不存在则插入
- 逻辑删除随行同步（`is_deleted` / `delete_time` 一并写入目标表）
- Redis 持久化同步位点，服务重启不会从头开始全量同步
- 写入失败时位点不推进，下一轮定时任务自动重试
- go-zero 配置加载 + 系统信号监听实现优雅退出

## Tech Stack

| 组件 | 版本 |
| --- | --- |
| Go | 1.24（CI 使用 1.26） |
| go-zero | v1.10.3 |
| GORM | v1.25.11 |
| MySQL | 8.0 |
| Redis | 6.x 及以上 |

## Project Structure

```
datasync-demo/
├── etc
│   └── datasync.yaml           # 配置文件
├── internal
│   └── config
│       └── config.go           # 配置结构体定义
├── model
│   ├── sourcemodel.go          # SourceUser 模型与增量查询
│   └── targetmodel.go          # TargetUser 模型与批量 Upsert
├── service
│   ├── cursor.go               # Redis 位点读写
│   └── syncer.go               # 同步服务与轮询任务
├── utils
│   └── chunk.go                # 泛型切片分块工具
├── .github/workflows/go.yml    # GitHub Actions 构建与测试
├── datasync.go                 # 程序入口
├── go.mod / go.sum
└── README.md
```

## Sync Flow

### 全量同步（首次启动 / 重置后）

1. `full_sync_finished` 不存在或不为 `"1"` → 进入全量同步模式
2. 从 Redis 读取 `full_sync_last_id`（不存在则从 0 开始），查询 `id > ? ORDER BY id ASC LIMIT 500`
3. 批量 Upsert 写入目标表
4. 取本批最后一条的 `id` 写回 `full_sync_last_id`，同时更新 `last_sync_id` / `last_sync_time`
5. 某轮查询结果为空 → 全量同步完成，`SET full_sync_finished "1"`，`DEL full_sync_last_id`

### 增量同步（全量完成后）

1. `full_sync_finished = "1"` → 进入增量同步模式
2. 从 Redis 读取 `last_sync_id` / `last_sync_time` 位点
3. 增量查询：`update_time > ? OR (update_time = ? AND id > ?)`，按 `update_time, id` 升序，单轮 `LIMIT 500`
4. 批量 Upsert 写入目标表（冲突列 `id`，`UpdateAll: true`）
5. 取本批最后一条的 `update_time` / `id` 更新 Redis 位点
6. 任一步失败只记录日志，本批位点不推进，等待下一个定时 tick 重试

源表索引建议与查询条件保持一致：`KEY idx_update_time (update_time, id)`。

## Database Schema

### 源表 `source_db.user`

```sql
CREATE TABLE `user` (
  `id` bigint NOT NULL AUTO_INCREMENT COMMENT '主键',
  `name` varchar(64) NOT NULL DEFAULT '' COMMENT '姓名',
  `phone` varchar(32) NOT NULL DEFAULT '' COMMENT '手机号',
  `update_time` datetime NOT NULL COMMENT '更新时间，同步依赖字段',
  `delete_time` datetime NULL COMMENT '逻辑删除时间',
  `is_deleted` tinyint NOT NULL DEFAULT '0' COMMENT '0未删除 1已逻辑删除',
  PRIMARY KEY (`id`),
  KEY `idx_update_time` (`update_time`,`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

### 目标表 `target_db.user_copy`

```sql
CREATE TABLE `user_copy` (
  `id` bigint NOT NULL COMMENT '主键，与源表 id 对齐',
  `name` varchar(64) NOT NULL DEFAULT '',
  `phone` varchar(32) NOT NULL DEFAULT '',
  `update_time` datetime NOT NULL,
  `delete_time` datetime NULL,
  `is_deleted` tinyint NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

## Configuration

配置示例见 `etc/datasync.yaml`：

| 配置项 | 说明 |
| --- | --- |
| `Name` / `Host` / `Port` | go-zero `rest.RestConf` 必需字段，Demo 未启动 HTTP 服务 |
| `SourceDB.Dsn` | 源库连接串，需带 `parseTime=True&loc=Local` |
| `TargetDB.Dsn` | 目标库连接串 |
| `RedisConfig` | go-zero Redis 连接配置，Demo 为单节点 `type: node` |
| `Sync.Interval` | 轮询间隔，单位毫秒 |
| `Sync.LastSyncId` | 预留的初始位点；当前实现始终被 Redis 位点覆盖 |

## Run

```bash
# 拉取依赖
go mod tidy

# 启动程序（默认读取 etc/datasync.yaml）
go run . -f etc/datasync.yaml

# 编译
go build -o datasync .
```

验证同步效果：向 `source_db.user` 插入或更新若干行（注意维护 `update_time`），等待一个轮询周期后查询 `target_db.user_copy`。

## Redis Keys

| key | 说明 |
| --- | --- |
| `full_sync_finished` | 全量同步是否完成，`"1"` = 已完成；不存在或非 `"1"` = 未完成，走全量同步 |
| `full_sync_last_id` | 全量同步游标，记录已同步到的最大 `id`；全量完成后自动删除 |
| `last_sync_id` | 增量同步游标，记录上一批最后一条的 `id` |
| `last_sync_time` | 增量同步游标，记录上一批最后一条的 `update_time` |
| `sync:user:cursor` | （已废弃）旧的 JSON 格式位点，当前实现不再使用 |

重置位点、重新触发全量同步：

```bash
DEL full_sync_finished full_sync_last_id last_sync_id last_sync_time
```

单独重跑全量同步（保留增量位点）：

```bash
DEL full_sync_finished full_sync_last_id
```

## Known Limitations

- 未实现分布式锁，多实例部署会重复执行同一批同步任务
- 只同步源表中发生变更的行，源库物理 `DELETE` 不会传导到目标库（业务请使用 `is_deleted` 逻辑删除）
- 强依赖业务正确维护 `update_time`，时间回拨或批量刷数会造成漏同步或长时间追赶
- 单轮固定 500 条（`service/syncer.go` 中的 `batchSize`），数据积压较多时需多轮追赶；`utils.Chunk` 已提供分块工具但尚未接入写库路径
- 全量同步完成后切换增量时，`last_sync_time` 为最大 `id` 行的 `update_time`（非全局最大 `update_time`），首次增量轮询可能重复处理少量已同步数据（Upsert 幂等，不影响正确性）

## License

Apache-2.0，详见 [LICENSE](LICENSE)。