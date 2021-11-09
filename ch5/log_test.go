// @文件名: log_test.go
// @作者: 邓一鸣
// @创建时间: 2021/11/2 4:30 下午
// @描述:
package ch5

import (
	"fmt"
	"redis-in-action/redisConn"
	"redis-in-action/util"
	"testing"

	as "github.com/stretchr/testify/assert"
)

var logger *Log
var counter *Counter
var stat *Stat

func TestMain(m *testing.M) {
	logger = NewLog(redisConn.ConnectRedis())
	defer logger.Reset()
	counter = NewCounter(redisConn.ConnectRedis())
	defer counter.Reset()
	stat = NewStat(redisConn.ConnectRedis())
	defer stat.Reset()
	m.Run()
}

func TestRecent(t *testing.T) {
	assert := as.New(t)

	print("Let's write a few logs to recent logs")
	for i := 0; i < 5; i++ {
		if err := logger.Recent("test", fmt.Sprintf("this is message %d", i), Info, nil); err != nil {
			t.Error("recent log failed:", err)
		}
	}
	recent, err := logger.RC.LRange(ctx, fmt.Sprintf(util.KeyLogRecent, "test", Info), 0, -1).Result()
	if err != nil {
		t.Error("list recent logs failed:", err)
	}
	t.Log("The current recent message log has this many messages:", len(recent))
	t.Log("Those messages includes:", recent[:])
	assert.GreaterOrEqual(len(recent), 5)
}

func TestCommon(t *testing.T) {
	assert := as.New(t)

	t.Log("Let's write some items to the common log")
	for i := 1; i < 6; i++ {
		for j := 0; j < i; j++ {
			if err := logger.Common("test", fmt.Sprintf("message-%d", i), Info); err != nil {
				t.Error("common logs failed:", err)
			}
		}
	}
	common, err := logger.RC.ZRevRangeWithScores(ctx, fmt.Sprintf(util.KeyLogCommon, "test", Info), 0, -1).Result()
	if err != nil {
		t.Error("list common logs failed:", err)
	}
	t.Log("The current number of common messages is:", len(common))
	t.Log("Those common messages are:", common[:])
	assert.GreaterOrEqual(len(common), 5)
}
