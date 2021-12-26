package main

import (
	"bufio"
	"context"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis/v8"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

var (
	log *zap.SugaredLogger
	_   sync.WaitGroup
)

func setupLogger() error {
	hook := lumberjack.Logger{
		Filename:   "log/test.log",
		MaxSize:    10, // megabytes
		MaxBackups: 5,
		MaxAge:     28,    //days
		Compress:   false, // disabled by default
	}
	writer := zapcore.NewMultiWriteSyncer(zapcore.AddSync(os.Stdout), zapcore.AddSync(&hook))
	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder   // 修改时间戳的格式
	encoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder // 日志级别使用大写显示
	encoder := zapcore.NewConsoleEncoder(encoderConfig)     // 设置 console 编码器

	core := zapcore.NewCore(encoder, writer, zapcore.InfoLevel) // 设置日志的默认级别
	logger := zap.New(core, zap.AddCaller())
	log = logger.Sugar()

	return nil
}

func initClient() (*redis.Client, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "redis-cn02zljq32jffvirx.redis.volces.com:6379",
		Password: "dmp_group2",
		DB:       0,
		PoolSize: 2000,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := rdb.Ping(ctx).Result(); err != nil {
		return rdb, err
	}
	return rdb, nil
}

func addToRedis(filename string) error {
	// defer wg.Done()

	log.Infof("crowd %s starts to sync to redis", filename)

	rdb, err := initClient()
	if err != nil {
		log.Fatalf("Redis connect error: %s", err)
	}
	defer func(rdb *redis.Client) {
		err := rdb.Close()
		if err != nil {
			log.Warnf("redis close failed: %s", err)
		}
	}(rdb)

	pipe := rdb.Pipeline()
	defer func(pipe redis.Pipeliner) {
		err := pipe.Close()
		if err != nil {
			log.Warnf("redis pipline close failed: %s", err)
		}
	}(pipe)

	crowdfile := "data" + "/" + filename
	file, err := os.Open(crowdfile)
	if err != nil {
		log.Fatalf("open file fail: %s", err)
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Warnf("file close failed: %s", err)
		}
	}(file)

	iterator := 0
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		iterator++
		ids := strings.Split(scanner.Text(), ",") // [cid, uid]
		pipe.SAdd(context.Background(), ids[1], ids[0])

		if iterator == 10000 {
			_, err := pipe.Exec(context.Background())
			if err != nil {
				log.Fatalf("%s pipeline error:%s", filename, err)
			}
			iterator = 0
		}
	}
	if iterator != 0 {
		_, err := pipe.Exec(context.Background())
		if err != nil {
			log.Fatalf("%s pipeline error: %s", filename, err)
		}
		iterator = 0
	}

	log.Infof("crowd %s synced to redis", filename)

	return nil
}

func matchCrowd(cid, uid string) bool {
	var err error
	var rdb *redis.Client
	if rdb, err = initClient(); err != nil {
		log.Warnf("Redis connect error: %s", err)
	}
	defer func(rdb *redis.Client) {
		err := rdb.Close()
		if err != nil {
			log.Warnf("redis closed failed: %s", err)
		}
	}(rdb)
	return rdb.SIsMember(context.Background(), cid, uid).Val()
}

func isMatch(c *gin.Context) {
	cid := c.Query("cid")
	uid := c.Query("uid")
	c.String(http.StatusOK, "%t", matchCrowd(uid, cid))
}

func init() {
	if err := setupLogger(); err != nil {
		log.Fatalf("setupLogger err: %v", err)
	}
}

func main() {
	rd, err := ioutil.ReadDir("data")
	if err != nil {
		log.Fatalf("read dir fail: %s", err)
	}

	for _, fi := range rd {
		if err := addToRedis(fi.Name()); err != nil {
			log.Fatalf("%s sync to redis failed: %s", fi.Name(), err)
		}
	}

	r := gin.Default()

	r.GET("/matchCrowd", func(c *gin.Context) {
		isMatch(c)
	})

	if err := r.Run(":8080"); err != nil {
		log.Fatalf("server start failed: %s", err)
	} else {
		log.Info("server start succeed")
	}
}
