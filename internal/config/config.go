package config

import (
	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/rest"
)

type Config struct {
	rest.RestConf

	SourceDB struct {
		Dsn string
	}
	TargetDB struct {
		Dsn string
	}
	Sync struct {
		Interval   int64
		LastSyncId int64
	}
	RedisConfig redis.RedisConf
}
