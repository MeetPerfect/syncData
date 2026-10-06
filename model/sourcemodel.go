package model

import (
	"time"

	"gorm.io/gorm"
)

type SourceUser struct {
	Id         int64      `gorm:"column:id"`
	Name       string     `gorm:"column:name"`
	Phone      string     `gorm:"column:phone"`
	UpdateTime time.Time  `gorm:"column:update_time"`
	DeleteTime *time.Time `gorm:"column:delete_time"`
	IsDeleted  int8       `gorm:"column:is_deleted"`
}

func (SourceUser) TableName() string {
	return "user"
}

// 查询大于lastId的增量数据
func ListSourceUser(db *gorm.DB, lastId int64) ([]SourceUser, error) {
	var list []SourceUser
	err := db.Where("id > ?", lastId).Order("id asc").Find(&list).Error
	return list, err
}

// 查询大于lastUpdate的增量数据
func ListSourceUserByTime(db *gorm.DB, lastUpdate time.Time, lastId int64, batchSize int, fullsync bool) ([]SourceUser, error) {
	var list []SourceUser
	tx := db.Model(&SourceUser{})
	if fullsync {
		// 全量同步，忽略lastUpdate和lastId
		tx = tx.Where("id > ?", lastId).Order("id ASC").Limit(batchSize)
	} else {
		// 增量同步，按update_time和id进行分页查询
		tx = tx.Where("update_time > ? OR (update_time = ? AND id > ?)", lastUpdate, lastUpdate, lastId).
			Order("update_time,id").
			Limit(batchSize)
	}
	err := tx.Find(&list).Error
	return list, err
}
