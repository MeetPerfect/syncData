package model

import (
	"time"

	"gorm.io/gorm"
)

type SourceUser struct {
	Id         int64  `gorm:"column:id"`
	Name       string `gorm:"column:name"`
	Phone      string `gorm:"column:phone"`
	UpdateTime string `gorm:"column:update_time"`
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

// model
func ListSourceUserByTime(db *gorm.DB, lastUpdate time.Time) ([]SourceUser, error) {
	var list []SourceUser
	err := db.Where("update_time > ?", lastUpdate).Order("update_time,id").Find(&list).Error
	return list, err
}
