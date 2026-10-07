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
	sourceDB   *gorm.DB
	targetDB   *gorm.DB
	rds        *redis.Redis
	intervalMs int64
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
	}, nil
}

// StartPoll 启动定时轮询同步
func (s *SyncService) StartPoll(ctx context.Context) {
	batchSize := 500
	ticker := time.NewTicker(time.Duration(s.intervalMs) * time.Millisecond)
	defer ticker.Stop()

	// 外层内存位点
	lastId := int64(0)
	lastUpdateTime := time.Time{}

	// 每次轮询同步时，从Redis加载上一次的同步位点
	loadIncrementPoint := func() {
		val, err := s.rds.GetCtx(ctx, "last_sync_id")
		if err == nil && val != "" {
			lastId, _ = strconv.ParseInt(val, 10, 64)
		}
		val, err = s.rds.GetCtx(ctx, "last_sync_time")
		if err == nil && val != "" {
			lastUpdateTime, _ = time.Parse(time.DateTime, val)
		}
	}
	loadIncrementPoint()

	for range ticker.C {
		// 每次轮询同步时，从Redis加载上一次的同步位点

		select {
		case <-ctx.Done():
			logx.Info("同步轮询任务退出, ctx取消")
			return
		case <-ticker.C:
			// 加载上一次的同步位点
			loadIncrementPoint()
			newLastId, newSyncTime, isFullSync, err := s.SyncUserWorker(ctx, s.sourceDB, s.targetDB, lastId, lastUpdateTime, batchSize)
			if err != nil {
				logx.Error("同步用户失败", logx.Field("err", err),
					logx.Field("lastSyncId", lastId),
					logx.Field("lastSyncTime", lastUpdateTime))
				continue
			}
			// 只有增量更新外层位点
			if !isFullSync {
				lastId = newLastId
				lastUpdateTime = newSyncTime
			}
			logx.Infof("用户同步完成，更新同步位点 time=%v, lastId=%d", lastUpdateTime, lastId)
		}
	}
}

// lastSyncTime 同步位点：上一次最大update_time，业务可以存在redis/db持久化
// isFullSync=true时使用独立的 full_sync_last_id 游标，避免 last_sync_id 残留旧值导致漏同步历史数据
func (s *SyncService) SyncUserWorker(ctx context.Context, sourceDB, targetDB *gorm.DB, lastId int64, lastSyncTime time.Time, batchSize int) (int64, time.Time, bool, error) {
	// 1. 读取redis判断全量是否完成
	fullSyncFinished, err := s.rds.GetCtx(ctx, "full_sync_finished")
	isFullSync := true
	if err == nil && fullSyncFinished == "1" {
		isFullSync = false
	} else if err != nil {
		logx.Error("读取全量同步状态失败", logx.Field("err", err))
		return lastId, lastSyncTime, isFullSync, err
	}
	// 全量同步使用独立游标 full_sync_last_id（默认0从头开始），
	// 不受 last_sync_id 旧值影响——这是历史旧数据漏同步的根因
	var queryLastId int64
	var queryLastTime time.Time
	if isFullSync {
		// 全量同步使用独立的游标 full_sync_last_id，避免受 last_sync_id 旧值影响
		fullSyncLastIdStr, err := s.rds.GetCtx(ctx, "full_sync_last_id")
		if err == nil && fullSyncLastIdStr != "" {
			queryLastId, _ = strconv.ParseInt(fullSyncLastIdStr, 10, 64)
		} else {
			queryLastId = 0
		}
	} else {
		queryLastId = lastId
		queryLastTime = lastSyncTime
	}

	// 2. 查询源表
	var sourceList []model.SourceUser
	sourceList, err = model.ListSourceUserByTime(sourceDB, queryLastTime, queryLastId, batchSize, isFullSync)
	if err != nil {
		return lastId, lastSyncTime, isFullSync, fmt.Errorf("查询源表失败: %w", err)
	}
	if len(sourceList) == 0 {
		if isFullSync {
			// 全量同步完成,切换到增量模式,标记状态,清理全量游标key
			err = s.rds.SetCtx(ctx, "full_sync_finished", "1")
			if err != nil {
				logx.Error("设置全量同步完成状态失败", logx.Field("err", err))
				return queryLastId, lastSyncTime, isFullSync, err
			}
			_, _ = s.rds.DelCtx(ctx, "full_sync_last_id")
		}
		return queryLastId, lastSyncTime, isFullSync, nil
	}
	// 3. 批量写入目标表
	err = model.BatchUpsertTargetUser(targetDB, sourceList)
	if err != nil {
		return queryLastId, queryLastTime, isFullSync, fmt.Errorf("batch upsert target user: %w", err)
	}

	// 4. 更新同步位点
	lastItem := sourceList[len(sourceList)-1]
	roundLastId := lastItem.Id
	roundLastTime := lastItem.UpdateTime

	logx.Infof("本轮同步成功，同步条数:%d, newId:%d, newTime:%v", len(sourceList), roundLastId, roundLastTime)

	// 5. 保存同步位点到redis
	err = s.rds.SetCtx(ctx, "last_sync_id", strconv.FormatInt(roundLastId, 10))
	if err != nil {
		logx.Error("保存last_sync_id失败", logx.Field("err", err))
		return roundLastId, roundLastTime, isFullSync, err
	}
	err = s.rds.SetCtx(ctx, "last_sync_time", roundLastTime.Format(time.DateTime))
	if err != nil {
		logx.Error("保存last_sync_time失败", logx.Field("err", err))
		return roundLastId, roundLastTime, isFullSync, err
	}
	if isFullSync {
		err = s.rds.SetCtx(ctx, "full_sync_last_id", strconv.FormatInt(roundLastId, 10))
		if err != nil {
			logx.Error("保存full_sync_last_id失败", logx.Field("err", err))
			return roundLastId, roundLastTime, isFullSync, err
		}
	}
	return roundLastId, roundLastTime, isFullSync, nil
}
