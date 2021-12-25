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
	log     *zap.SugaredLogger
	wg      sync.WaitGroup
	datadir string
)

func setupLogger() error {
	hook := lumberjack.Logger{
		Filename:   "log/test.log",
		MaxSize:    10, // megabytes
		MaxBackups: 5,
		MaxAge:     28,    //days
		Compress:   false, // disabled by default
	}
	// writer := zapcore.AddSync(os.Stdout)  // 设置日志输出的设备，这里还是使用标准输出，也可以传一个 File 类型让它写入到文件
	writer := zapcore.NewMultiWriteSyncer(zapcore.AddSync(os.Stdout), zapcore.AddSync(&hook))
	// encoder := zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig())  // 设置编码器，即日志输出的格式，默认提供了 json 和 console 两种编码器，这里我们还是使用 json 的编码器
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
		PoolSize: 10,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := rdb.Ping(ctx).Result(); err != nil {
		return rdb, err
	}
	return rdb, nil
}

func addToRedis(finame string) error {
	// defer wg.Done()

	log.Infof("crowd %s starts to sync to redis", finame)

	rdb, err := initClient()
	if err != nil {
		log.Fatalf("Redis connect error", err)
	}
	defer rdb.Close()

	pipe := rdb.Pipeline()
	defer pipe.Close()

	cfile := datadir + "/" + finame
	file, err := os.Open(cfile)
	if err != nil {
		log.Fatalf("open file fail: %s", err)
	}
	defer file.Close()

	iterator := 0
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		iterator++
		ids := strings.Split(scanner.Text(), ",") // [cid, uid]
		pipe.SAdd(context.Background(), ids[0], ids[1])

		if iterator == 10000 {
			_, err := pipe.Exec(context.Background())
			if err != nil {
				log.Fatalf("%s pipeline error:%s", finame, err)
			}
			// log.Infof("%s add to redis: %d", finame, len(cmds))
			iterator = 0
		}
	}
	if iterator != 0 {
		_, err := pipe.Exec(context.Background())
		if err != nil {
			log.Fatalf("%s pipeline error:%s", err)
		}
		// log.Infof("%s add to redis: %d", finame, len(cmds))
		iterator = 0
	}

	log.Infof("crowd %s synced to redis", finame)

	return nil
}

func matchCrowd(cid, uid string) bool {
	var err error
	var rdb *redis.Client
	if rdb, err = initClient(); err != nil {
		log.Fatalf("Redis connect error: %s", err)
	}
	defer rdb.Close()
	return rdb.SIsMember(context.Background(), cid, uid).Val()
}

func isMatch(c *gin.Context) {
<<<<<<< HEAD
	cid := c.Query("cid")
	uid := c.Query("uid")
	log.Infof("cid: \"%s\" \t uid: \"%s\"", cid, uid)
	if cid == "" || uid == "" {
		c.String(http.StatusOK, "miss params")
	} else {
		c.String(http.StatusOK, "%t", matchCrowd(cid, uid))
=======
	//var p People
	//c.ShouldBind(&p)
	//fmt.Println(p.Cid, " ", p.Uid, "123")
	cid := c.PostForm("cid")
	uid := c.PostForm("uid")
	fmt.Println("cid:"+cid+"  uid:"+uid)
	c.String(http.StatusOK, "%t eee", matchCrowd(cid, uid))
}
func updateCrowd(cid, uid string) {
	var err error
	var rdb *redis.Client
	ctx := context.Background()
	if rdb, err = initClient(); err != nil {
		log.Printf("Redis connect error")
>>>>>>> dev
	}
}
<<<<<<< HEAD

func init() {
	datadir = "data"
	if err := setupLogger(); err != nil {
		log.Fatalf("setupLogger err: %v", err)
	}
=======
func update(c *gin.Context) {
	cid := c.PostForm("cid")
	uid := c.PostForm("uid")
	fmt.Println("cid:"+cid+"  uid:"+uid)
	updateCrowd(cid, uid)
>>>>>>> dev
}

func main() {

	rd, err := ioutil.ReadDir(datadir)
	if err != nil {
		log.Fatalf("read dir fail: %s", err)
	}

	for _, fi := range rd {
		// wg.Add(1)
		addToRedis(fi.Name())
	}

	wg.Wait()

	r := gin.Default()

	r.GET("/ping", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"message": "Ping OK",
		})
	})

	r.GET("/matchCrowd", func(c *gin.Context) {
		isMatch(c)
	})

	log.Info("Server start succeed")
	r.Run(":8080")
}
