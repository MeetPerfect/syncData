package service

import (
	"context"
	"fmt"
	"time"

	"datasync-demo/model"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

type SyncService struct {
	sourceDB   *gorm.DB
	targetDB   *gorm.DB
	intervalMs int64
	lastSyncId int64
	updateTime time.Time
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
	ticker := time.NewTicker(time.Duration(s.intervalMs) * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			fmt.Println("sync service exit")
			return
		case <-ticker.C:
			s.doSync()
		}
	}
}

func (s *SyncService) doSync() {
	// 1.拉取增量
	// list, err := model.ListSourceUser(s.sourceDB, s.lastSyncId)
	list, err := model.ListSourceUserByTime(s.sourceDB, s.updateTime, s.lastSyncId, 100)
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
	err = model.BatchUpsertTargetUser(s.targetDB, list)
	if err != nil {
		fmt.Printf("batch sync err:%v\n", err)
		return
	}
	s.lastSyncId = list[len(list)-1].Id
	fmt.Printf("sync finish, new lastSyncId:%d\n", s.lastSyncId)
}
