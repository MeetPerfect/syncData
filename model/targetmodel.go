package model

import (
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type TargetUser struct {
	Id         int64      `gorm:"column:id"`
	Name       string     `gorm:"column:name"`
	Phone      string     `gorm:"column:phone"`
	UpdateTime time.Time  `gorm:"column:update_time"`
	DeleteTime *time.Time `gorm:"column:delete_time"`
	IsDeleted  int8       `gorm:"column:is_deleted"`
}

func (TargetUser) TableName() string {
	return "user_copy"
}

// Upsert：存在则更新，不存在插入
func UpsertTargetUser(db *gorm.DB, data *SourceUser) error {
	tu := TargetUser{
		Id:         data.Id,
		Name:       data.Name,
		Phone:      data.Phone,
		UpdateTime: data.UpdateTime,
		DeleteTime: data.DeleteTime,
		IsDeleted:  data.IsDeleted,
	}
	return db.Clauses(
		clause.OnConflict{
			Columns:   []clause.Column{{Name: "id"}},
			UpdateAll: true,
		}).Create(&tu).Error
}

// 批量
func BatchUpsertTargetUser(db *gorm.DB, list []SourceUser) error {
	var targetList []TargetUser
	for _, item := range list {
		targetUser := TargetUser{
			Id:         item.Id,
			Name:       item.Name,
			Phone:      item.Phone,
			UpdateTime: item.UpdateTime,
			DeleteTime: item.DeleteTime,
			IsDeleted:  item.IsDeleted,
		}
		targetList = append(targetList, targetUser)
	}

	return db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "id"}},
		UpdateAll: true,
	}).Create(&targetList).Error
}
