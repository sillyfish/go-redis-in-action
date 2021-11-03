// @文件名: conn
// @作者: 邓一鸣
// @创建时间: 2021/10/9 2:38 下午
// @描述:
package redisConn

import (
	"context"
	"log"

	"github.com/go-redis/redis/v8"
)

const (
	Addr     = "localhost:6379"
	Password = ""
	DB       = 15
)

func ConnectRedis() *redis.Client {
	client := redis.NewClient(&redis.Options{
		Addr:     Addr,
		Password: Password,
		DB:       DB,
	})
	if _, err := client.Ping(context.Background()).Result(); err != nil {
		log.Fatalf("Connect to redis client failed, err: %v\n", err)
	}
	return client
}
