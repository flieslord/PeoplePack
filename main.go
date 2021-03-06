package main

import (
	"bufio"
	"context"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis/v8"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

var (
	wg sync.WaitGroup
)

//type People struct {
//	Cid string `json:"cid"`
//	Uid string `json:"uid"`
//}
func setUpLogger() {
	logFileLocation, _ := os.OpenFile("./log/test.log", os.O_CREATE|os.O_APPEND|os.O_RDWR, 0744)
	log.SetOutput(logFileLocation)
}
func initClient() (*redis.Client, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "redis-cn02zljq32jffvirx.redis.volces.com:6379",
		Password: "dmp_group2",
		DB:       0,
		PoolSize: 1000,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := rdb.Ping(ctx).Result()
	if err != nil {
		return rdb, err
	}
	return rdb, nil
}
func addToRedis(lines []string) {
	wg.Add(1)
	var rdb *redis.Client
	var err error
	ctx := context.Background()
	if rdb, err = initClient(); err != nil {
		log.Printf("Redis connect error")
	}
	defer rdb.Close()
	for i := 0; i < len(lines)-1; i++ {
		strs := strings.Split(lines[i], " ")
		rdb.SAdd(ctx, strs[0], strs[1])
	}
	wg.Done()
}
func matchCrowd(cid, uid string) bool {
	var err error
	var rdb *redis.Client
	ctx := context.Background()
	if rdb, err = initClient(); err != nil {
		log.Printf("Redis connect error")
	}
	defer rdb.Close()
	return rdb.SIsMember(ctx, cid, uid).Val()
}
func isMatch(c *gin.Context) {
	//var p People
	//c.ShouldBind(&p)
	//fmt.Println(p.Cid, " ", p.Uid, "123")
	cid := c.PostForm("cid")
	uid := c.PostForm("uid")
	c.String(http.StatusOK, "%t eee", matchCrowd(cid, uid))
}
func updateCrowd(cid, uid string) {
	var err error
	var rdb *redis.Client
	ctx := context.Background()
	if rdb, err = initClient(); err != nil {
		log.Printf("Redis connect error")
	}
	go func(cid string, uid string) {
		wg.Add(1)
		file, _ := os.OpenFile("./data/test.txt", os.O_APPEND, 0744)
		writer := bufio.NewWriter(file)
		fmt.Fprintln(writer, cid+" "+uid)
		writer.Flush()
	}(cid, uid)
	wg.Wait()
	defer rdb.Close()
	rdb.SAdd(ctx, cid, uid)
}
func update(c *gin.Context) {
	cid := c.PostForm("cid")
	uid := c.PostForm("uid")
	updateCrowd(cid, uid)
}
func main() {
	setUpLogger()
	r := gin.Default()
	file, err := os.Open("./test.txt")
	if err != nil {
		log.Printf("File open error")
	}
	defer file.Close()
	lines := []string{}
	iterator := 0
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		iterator++
		if iterator <= 2000 {
			lines = append(lines, line)
		} else {
			iterator = 0
			lines = nil
			go addToRedis(lines)
		}
	}
	if lines != nil {
		go addToRedis(lines)
	}
	wg.Wait()
	r.POST("/matchCrowd", func(c *gin.Context) {
		isMatch(c)
	})
	r.POST("/updateCrowd", func(c *gin.Context) {
		update(c)
	})
	r.GET("/ping", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"message": "Ping OK",
		})
	})
	log.Printf("Server start succeed")
	r.Run(":8080")
}
