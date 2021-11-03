// @文件名: counter
// @作者: 邓一鸣
// @创建时间: 2021/11/3 11:35 上午
// @描述:
package ch5

import (
	"time"

	"github.com/go-redis/redis/v8"
)

type Counter struct {
	RC *redis.Client
}

func NewCounter(rc *redis.Client) *Counter {
	return &Counter{RC: rc}
}

func (c *Counter) Reset() {
	c.RC.FlushDB(ctx)
}

func (c *Counter) Update(name string, count int, now time.Time) error {
	return nil
}

func (c *Counter) Get(name string, precision int) error {
	return nil
}
