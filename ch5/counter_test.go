// @文件名: counter_test.go
// @作者: 邓一鸣
// @创建时间: 2021/11/3 5:21 下午
// @描述:
package ch5

import (
	"math/rand"
	"testing"
	"time"

	as "github.com/stretchr/testify/assert"
)

func TestCounter(t *testing.T) {
	assert := as.New(t)
	t.Log("Let's update some counters for now and a little in the future")
	now := time.Now()
	var i time.Duration
	for i = 0; i < 10; i++ {
		if err := counter.Update("test", rand.Int63n(5)+1, now.Add(time.Second*i)); err != nil {
			t.Error("counter update failed:", err)
		}
	}
	counters, err := counter.Get("test", 1)
	if err != nil {
		t.Error("counter get failed:", err)
	}
	t.Log("We have some per-second counters:", len(counters))
	assert.GreaterOrEqual(len(counters), 10)
	counters, err = counter.Get("test", 5)
	if err != nil {
		t.Error("counter get failed:", err)
	}
	t.Log("We have some per-5-second counters:", len(counters))
	t.Logf("These counters include: %+v", counters)
	assert.GreaterOrEqual(len(counters), 2)

	t.Log("Let's clean out some counters by setting our sample count to 0:", counters)
	counterLeft := make(chan int64, 1)
	go counter.Clean(counterLeft, func() time.Time {
		return time.Now().Add(time.Hour * 24 * 2)
	})
	counterLeft <- 0
	time.Sleep(time.Second)
	counters, err = counter.Get("test", 86400)
	if err != nil {
		t.Error("counter get failed:", err)
	}
	t.Log("Did we clean out all of the counters?", len(counters) == 0)
	assert.Len(counters, 0)
}
