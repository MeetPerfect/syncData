package service

import (
	"context"
	"fmt"
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
	ctx := context.Background()
	// 读取同步位点
	cursor, err := LoadCursor(ctx, rds)
	if err != nil {
		return nil, fmt.Errorf("load cursor err:%w", err)
	}
	srcDb, err := gorm.Open(mysql.Open(sourceDsn), &gorm.Config{})
	if err != nil {
		return nil, err
	}
	tarDb, err := gorm.Open(mysql.Open(targetDsn), &gorm.Config{})
	if err != nil {
		return nil, err
	}
	return &SyncService{
		sourceDB:     srcDb,
		targetDB:     tarDb,
		rds:          rds,
		intervalMs:   intervalMs,
		lastSyncId:   cursor.LastSyncId,
		lastSyncTime: cursor.LastSyncTime,
	}, nil
}

// StartPoll 启动定时轮询同步
func (s *SyncService) StartPoll(ctx context.Context) {
	batchSize := 500
	ticker := time.NewTicker(time.Duration(s.intervalMs) * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		select {
		case <-ctx.Done():
			logx.Info("同步轮询任务退出, ctx取消")
			return
		case <-ticker.C:
			newSyncTime, err := s.SyncUserWorker(s.sourceDB, s.targetDB, s.lastSyncTime, batchSize)
			if err != nil {
				logx.Error("同步用户失败", logx.Field("err", err),
					logx.Field("lastSyncId", s.lastSyncId),
					logx.Field("lastSyncTime", s.lastSyncTime))
				continue
			}
			s.lastSyncTime = newSyncTime
			newCursor := SyncCursor{
				LastSyncId:   s.lastSyncId,
				LastSyncTime: s.lastSyncTime,
			}
			err = SaveCursor(ctx, s.rds, newCursor)
			if err != nil {
				logx.Error("保存同步位点失败", logx.Field("err", err))
				// Redis写失败：这里业务决策：
				// 方案A：continue，内存继续跑，但是redis没落地，重启会回退重复同步（常用）
				// 方案B：直接return终止任务，防止数据不一致
				continue
			}
			logx.Infof("用户同步完成，更新同步位点 time=%v, lastId=%d", s.lastSyncTime, s.lastSyncId)
		}
	}
}

// SyncUserWorker 用户增量同步任务
// lastSyncTime 同步位点：上一次最大update_time，业务可以存在redis/db持久化
func (s *SyncService) SyncUserWorker(sourceDB, targetDB *gorm.DB, lastSyncTime time.Time, batchSize int) (newSyncTime time.Time, err error) {
	var sourceList []model.SourceUser
	sourceList, err = model.ListSourceUserByTime(sourceDB, lastSyncTime, s.lastSyncId, batchSize)
	if err != nil {
		return
	}
	if len(sourceList) == 0 {
		return lastSyncTime, nil
	}

	err = model.BatchUpsertTargetUser(targetDB, sourceList)
	if err != nil {
		return lastSyncTime, fmt.Errorf("批量upsert失败,err:%w", err)
	}
	lastItem := sourceList[len(sourceList)-1]
	lastSyncTime = lastItem.UpdateTime
	s.lastSyncId = lastItem.Id
	logx.Infof("本轮同步成功，同步条数:%d, newId:%d, newTime:%v", len(sourceList), s.lastSyncId, newSyncTime)
	return lastSyncTime, nil
}
