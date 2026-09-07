# gozero-data-sync-demo

基于 go‑zero + GORM 实现 MySQL 增量数据同步 Demo。
采用 **update_time + id 双游标轮询同步**，支持逻辑删除同步、Redis位点持久化、分布式锁防止多实例重复执行。

> ⚠️ 本项目仅用于学习轮询增量同步实现；生产大数据场景优先选择 Canal / Flink‑CDC。

## Features

- ✅ 双游标 `update_time + id`，解决同一时间戳多条数据丢失问题
- ✅ GORM OnConflict Upsert：存在则更新，不存在则插入
- ✅ 支持源表逻辑删除同步（`is_deleted` + `delete_time`）
- ✅ Redis持久化同步位点，服务重启不会从头全量同步
- ✅ Redis分布式锁，多实例部署避免重复同步任务
- ✅ 写入失败位点不推进，定时器自动重试
- ✅ go‑zero 工程规范，监听系统信号实现优雅退出

## Tech Stack

- Go 1.22+
- go‑zero
- GORM v2
- MySQL 8.0
- Redis

## Project Structure

```
gozero-data-sync-demo/
├── etc
│ └── sync.yaml # 配置文件
├── config
│ └── config.go # 配置结构体定义
├── model
│ └── user.go # SourceUser / TargetUser 模型、查询、批量 Upsert
├── service
│ └── sync_service.go # 同步服务、Redis 位点读写、分布式锁、轮询任务
├── go.mod
├── go.sum
├── main.go
└── README.md
```

## Database Schema

### 源表 source_db.`user`

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

### 目标表 target_db.`target_user`

```sql
CREATE TABLE `target_user` (
  `id` bigint NOT NULL COMMENT '主键，与源表id对齐',
  `name` varchar(64) NOT NULL DEFAULT '',
  `phone` varchar(32) NOT NULL DEFAULT '',
  `update_time` datetime NOT NULL,
  `delete_time` datetime NULL,
  `is_deleted` tinyint NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```



## Run

```go
# 拉取依赖
go mod tidy

# 启动程序
go run main.go -f etc/sync.yaml
```



## Redis Key

| key                | desc                                                         |
| ------------------ | ------------------------------------------------------------ |
| `sync:user:cursor` | 同步位点，json 存储 `last_sync_id`、`last_sync_time`；SetCtx 覆盖更新，永不过期 |
| `lock:sync:user`   | 同步分布式锁，防止多实例重复执行，锁过期 30s                 |

重置位点，触发全量重新同步：

```
DEL sync:user:cursor
```

## 