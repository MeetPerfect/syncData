package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"datasync-demo/internal/config"
	"datasync-demo/service"
	"github.com/zeromicro/go-zero/core/conf"
)

var configFile = flag.String("f", "etc/datasync.yaml", "config file")

func main() {
	flag.Parse()

	var c config.Config
	conf.MustLoad(*configFile, &c)

	// 初始化同步服务
	syncSvc, err := service.NewSyncService(
		c.SourceDB.Dsn,
		c.TargetDB.Dsn,
		c.Sync.Interval,
		c.Sync.LastSyncId,
	)
	if err != nil {
		log.Fatalf("init sync service err:%v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go syncSvc.StartPoll(ctx)

	log.Println("data sync demo start...")

	// 优雅退出
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch
	log.Println("receive stop signal")
	cancel()
}
