package service

import (
	"context"
	"encoding/json"
	"time"

	"github.com/zeromicro/go-zero/core/stores/redis"
)

type SyncCursor struct {
	LastSyncId   int64     `json:"last_sync_id"`
	LastSyncTime time.Time `json:"last_sync_time"`
}

const redisCursorKey = "sync:user:cursor"

// LoadCursor 从redis加载位点；没有key返回初始位点
func LoadCursor(ctx context.Context, rds *redis.Redis) (SyncCursor, error) {
	value, err := rds.GetCtx(ctx, redisCursorKey)
	if err != nil {
		return SyncCursor{}, err
	}

	if value == "" {
		// 初始位点，根据你的业务设置
		return SyncCursor{
			LastSyncId:   0,
			LastSyncTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		}, nil
	}

	var cur SyncCursor
	err = json.Unmarshal([]byte(value), &cur)
	return cur, err
}

// SaveCursor 保存位点到redis，只有同步成功才调用
func SaveCursor(ctx context.Context, rds *redis.Redis, cur SyncCursor) error {
	data, err := json.Marshal(cur)
	if err != nil {
		return err
	}
	// 不设置过期时间，位点永久保存
	return rds.SetCtx(ctx, redisCursorKey, string(data))
}
