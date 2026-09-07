package service

import (
	"context"
	"fmt"
	"time"

	"datasync-demo/model"

	"datasync-demo/utils"

	"github.com/zeromicro/go-zero/core/logx"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

type SyncService struct {
	sourceDB     *gorm.DB
	targetDB     *gorm.DB
	intervalMs   int64
	lastSyncId   int64
	updateTime   time.Time
	lastSyncTime time.Time
}

func NewSyncService(sourceDsn, targetDsn string, intervalMs, lastSyncId int64) (*SyncService, error) {
	srcDb, err := gorm.Open(mysql.Open(sourceDsn), &gorm.Config{})
	if err != nil {
		return nil, err
	}
	tarDb, err := gorm.Open(mysql.Open(targetDsn), &gorm.Config{})
	if err != nil {
		return nil, err
	}
	return &SyncService{
		sourceDB:   srcDb,
		targetDB:   tarDb,
		intervalMs: intervalMs,
		lastSyncId: lastSyncId,
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
		}
		logx.Infof("用户同步完成，更新同步位点 time=%v, lastId=%d", s.lastSyncTime, s.lastSyncId)
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

	// 2. 切片分块
	// chunks := utils.Chunk(sourceList, batchSize)
	// for idx, chunk := range chunks {
	// 	err = model.BatchUpsertTargetUser(targetDB, chunk)
	// 	if err != nil {
	// 		// 失败：可以打error日志，根据业务选择：直接返回错误 / 记录失败批次，跳过后续
	// 		return lastSyncTime, fmt.Errorf("第%d批同步失败,size=%d,err:%w", idx, len(chunk), err)
	// 	}
	// }
	// 直接写入，DB已经limit，不需要再次chunk分块
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

func (s *SyncService) doSync() {
	// 1.拉取增量
	// list, err := model.ListSourceUser(s.sourceDB, s.lastSyncId)
	list, err := model.ListSourceUserByTime(s.sourceDB, s.updateTime, s.lastSyncId, 500)
	if err != nil {
		fmt.Printf("pull source data err: %v\n", err)
		return
	}
	if len(list) == 0 {
		return
	}

	fmt.Printf("get sync count:%d, lastSyncId:%d\n", len(list), s.lastSyncId)

	// 2.批量upsert到目标库
	// for _, item := range list {
	// 	err := model.UpsertTargetUser(s.targetDB, &item)
	// 	if err != nil {
	// 		fmt.Printf("sync id=%d err:%v\n", item.Id, err)
	// 		continue
	// 	}
	// 	// 更新位点
	// 	if item.Id > s.lastSyncId {
	// 		s.lastSyncId = item.Id
	// 	}
	// }
	chunks := utils.Chunk(list, 500)
	for idx, chunk := range chunks {
		err = model.BatchUpsertTargetUser(s.targetDB, chunk)
		if err != nil {
			// 失败：可以打error日志，根据业务选择：直接返回错误 / 记录失败批次，跳过后续
			fmt.Errorf("第%d批同步失败,size=%d,err:%w", idx, len(chunk), err)
		}
	}
	// err = model.BatchUpsertTargetUser(s.targetDB, list)
	// if err != nil {
	// 	fmt.Printf("batch sync err:%v\n", err)
	// 	return
	// }
	s.lastSyncId = list[len(list)-1].Id
	fmt.Printf("sync finish, new lastSyncId:%d\n", s.lastSyncId)
}
