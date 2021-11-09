// @文件名: stat_test.go
// @作者: 邓一鸣
// @创建时间: 2021/11/8 2:58 下午
// @描述:
package ch5

import (
	"math/rand"
	"redis-in-action/util"
	"strconv"
	"testing"
	"time"

	as "github.com/stretchr/testify/assert"
)

func TestStat(t *testing.T) {
	t.Log("Let's add some data for our statistics!")
	for i := 0; i < 5; i++ {
		stats, err := stat.Update("temp", "example", float64(5+rand.Intn(10)))
		if err != nil {
			t.Error("update stats failed:", err)
		}
		t.Log("We have some aggregate statistics:", stats)
	}
	st, err := stat.Get("temp", "example")
	if err != nil {
		t.Error("get stats failed:", err)
	}
	t.Logf("Which we can fetch manually:%#v", st)
	as.GreaterOrEqual(t, st.Count, float64(5))
}

func TestAccessTime(t *testing.T) {
	assert := as.New(t)
	t.Log("Let's calculate some access times...")
	for i := 0; i < 10; i++ {
		if err := stat.AccessTime("req-"+strconv.Itoa(i), func() {
			mss := time.Duration(500 + 1000*rand.Float64())
			time.Sleep(time.Millisecond * mss)
		}); err != nil {
			t.Error("test access time failed:", err)
		}
	}
	result, err := stat.RC.ZRevRangeWithScores(ctx, util.KeySlowestAccessTime, 0, -1).Result()
	if err != nil {
		return
	}
	t.Logf("The slowest access times are: %#v", result)
	assert.GreaterOrEqual(len(result), 10)

	t.Log("Let's use the callback version...")
	for i := 0; i < 5; i++ {
		if err = stat.AccessTime("cbreq-"+strconv.Itoa(i), func() {
			mss := time.Duration(1000 + 1000*rand.Float64())
			time.Sleep(time.Millisecond * mss)
		}); err != nil {
			t.Error("test access time failed:", err)
		}
	}
	result, err = stat.RC.ZRevRangeWithScores(ctx, util.KeySlowestAccessTime, 0, -1).Result()
	if err != nil {
		return
	}
	t.Logf("The slowest access times are: %#v", result)
	assert.GreaterOrEqual(len(result), 10)
}
