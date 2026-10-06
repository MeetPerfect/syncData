package service

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"datasync-demo/model"

	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/stores/redis"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

type SyncService struct {
	sourceDB     *gorm.DB
	targetDB     *gorm.DB
	rds          *redis.Redis
	intervalMs   int64
	lastSyncId   int64
	lastSyncTime time.Time
}

func NewSyncService(sourceDsn, targetDsn string, rds *redis.Redis, intervalMs, lastSyncId int64) (*SyncService, error) {
	srcDb, err := gorm.Open(mysql.Open(sourceDsn), &gorm.Config{})
	if err != nil {
		return nil, fmt.Errorf("open source db: %w", err)
	}
	tarDb, err := gorm.Open(mysql.Open(targetDsn), &gorm.Config{})
	if err != nil {
		return nil, fmt.Errorf("open target db: %w", err)
	}
	return &SyncService{
		sourceDB:   srcDb,
		targetDB:   tarDb,
		rds:        rds,
		intervalMs: intervalMs,
		// lastSyncId/lastSyncTime 由 StartPoll 从 Redis 加载，
		// SyncUserWorker 每轮成功后更新，这里初始化零值即可
	}, nil
}

// StartPoll 启动定时轮询同步
func (s *SyncService) StartPoll(ctx context.Context) {
	batchSize := 500
	ticker := time.NewTicker(time.Duration(s.intervalMs) * time.Millisecond)
	defer ticker.Stop()

	// 加载增量同步位点（全量同步的游标在 SyncUserWorker 内部从 full_sync_last_id 读取）
	lastIdStr, _ := s.rds.GetCtx(ctx, "last_sync_id")
	lastId, _ := strconv.ParseInt(lastIdStr, 10, 64)
	lastUpdateTimeStr, _ := s.rds.GetCtx(ctx, "last_sync_time")
	lastUpdateTime, _ := time.Parse(time.DateTime, lastUpdateTimeStr)

	for range ticker.C {
		select {
		case <-ctx.Done():
			logx.Info("同步轮询任务退出, ctx取消")
			return
		case <-ticker.C:
			newLastId, newSyncTime, _, err := s.SyncUserWorker(ctx, s.sourceDB, s.targetDB, lastId, lastUpdateTime, batchSize)
			if err != nil {
				logx.Error("同步用户失败", logx.Field("err", err),
					logx.Field("lastSyncId", lastId),
					logx.Field("lastSyncTime", lastUpdateTime))
				continue
			}
			lastId = newLastId
			lastUpdateTime = newSyncTime
			logx.Infof("用户同步完成，更新同步位点 time=%v, lastId=%d", lastUpdateTime, lastId)
		}
	}
}

// lastSyncTime 同步位点：上一次最大update_time，业务可以存在redis/db持久化
// isFullSync=true时使用独立的 full_sync_last_id 游标，避免 last_sync_id 残留旧值导致漏同步历史数据
func (s *SyncService) SyncUserWorker(ctx context.Context, sourceDB, targetDB *gorm.DB, lastId int64, lastSyncTime time.Time, batchSize int) (int64, time.Time, bool, error) {
	// 1. 读取redis判断全量是否完成
	fullSyncFinished, _ := s.rds.GetCtx(ctx, "full_sync_finished")
	isFullSync := fullSyncFinished != "1"

	// 全量同步使用独立游标 full_sync_last_id（默认0从头开始），
	// 不受 last_sync_id 旧值影响——这是历史旧数据漏同步的根因
	var queryLastId int64
	var queryLastTime time.Time
	if isFullSync {
		fullSyncLastIdStr, _ := s.rds.GetCtx(ctx, "full_sync_last_id")
		queryLastId, _ = strconv.ParseInt(fullSyncLastIdStr, 10, 64) // key不存在时返回0
	} else {
		queryLastId = lastId
		queryLastTime = lastSyncTime
	}

	// 2. 查询源表
	var sourceList []model.SourceUser
	sourceList, err := model.ListSourceUserByTime(sourceDB, queryLastTime, queryLastId, batchSize, isFullSync)
	if err != nil {
		return lastId, lastSyncTime, isFullSync, err
	}
	if len(sourceList) == 0 {
		if isFullSync {
			// 全量同步完成，切换到增量模式
			_ = s.rds.SetCtx(ctx, "full_sync_finished", "1")
			_, _ = s.rds.DelCtx(ctx, "full_sync_last_id")
		}
		return queryLastId, lastSyncTime, isFullSync, nil
	}
	// 3. 批量写入目标表
	err = model.BatchUpsertTargetUser(targetDB, sourceList)
	if err != nil {
		return lastId, lastSyncTime, isFullSync, err
	}

	// 4. 更新同步位点
	lastItem := sourceList[len(sourceList)-1]
	s.lastSyncId = lastItem.Id
	s.lastSyncTime = lastItem.UpdateTime

	logx.Infof("本轮同步成功，同步条数:%d, newId:%d, newTime:%v", len(sourceList), s.lastSyncId, s.lastSyncTime)

	// 5. 保存同步位点到redis
	_ = s.rds.SetCtx(ctx, "last_sync_id", strconv.FormatInt(s.lastSyncId, 10))
	_ = s.rds.SetCtx(ctx, "last_sync_time", s.lastSyncTime.Format(time.DateTime))
	if isFullSync {
		_ = s.rds.SetCtx(ctx, "full_sync_last_id", strconv.FormatInt(s.lastSyncId, 10))
	}
	return s.lastSyncId, s.lastSyncTime, isFullSync, nil
}
