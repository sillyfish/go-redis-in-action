// @文件名: ch2_test.go
// @作者: 邓一鸣
// @创建时间: 2021/10/12 4:31 下午
// @描述:
package ch2

import (
	"context"
	"fmt"
	"redis-in-action/redisConn"
	"redis-in-action/util"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	as "github.com/stretchr/testify/assert"
)

var ch2 *Ch2

func TestCh2_LoginCookies(t *testing.T) {
	assert := as.New(t)
	ch := make(chan int64)
	ch2.CleanSessions(ch)

	token := uuid.NewString()
	if err := ch2.UpdateToken(token, "username", "itemX"); err != nil {
		t.Error("update token failed, err:", err)
	}
	t.Logf("We just logged-in/updated token:%s", token)
	t.Log("For user: username")

	username, err := ch2.CheckToken(token)
	if err != nil {
		t.Error("check token failed, err:", err)
	}
	t.Logf("What username do we get when we look-up that token? %s", username)
	assert.Equal("username", username)

	t.Log("Let's drop the maximum number of cookies to 0 to clean them out")

	ch <- 0
	time.Sleep(2 * time.Second)

	count, err := ch2.RC.HLen(context.Background(), util.KeyLogin).Result()
	if err != nil {
		t.Error("check number of sessions failed, err:", err)
	}
	t.Logf("The current number of sessions still available is: %d", count)
	assert.Equal(0, int(count))
}

func TestCh2_ShoppingCartCookies(t *testing.T) {
	assert := as.New(t)
	ch := make(chan int64)
	ch2.CleanSessions(ch)

	token := uuid.NewString()
	t.Log("We'll refresh our session...")
	if err := ch2.UpdateToken(token, "username", "itemX"); err != nil {
		t.Error("update token failed, err:", err)
	}
	t.Log("And add an item to the shopping cart")
	if err := ch2.Add2Cart(token, "itemY", 3); err != nil {
		t.Error("add cart failed, err:", err)
	}
	cart, err := ch2.RC.HGetAll(context.Background(), fmt.Sprintf(util.KeyCartToken, token)).Result()
	if err != nil {
		t.Error("get cart failed, err:", err)
	}
	t.Logf("Our shopping cart currently has:%v", cart)

	assert.True(len(cart) >= 1)

	t.Log("Let's clean out our sessions and carts")
	ch <- 0
	time.Sleep(2 * time.Second)

	cart, err = ch2.RC.HGetAll(context.Background(), fmt.Sprintf(util.KeyCartToken, token)).Result()
	if err != nil {
		t.Error("get cart failed, err:", err)
	}
	t.Logf("Our shopping cart now contains:%v", cart)

	assert.True(len(cart) == 0)
}

func TestCh2_CacheRequest(t *testing.T) {
	assert := as.New(t)

	if err := ch2.UpdateToken(uuid.NewString(), "username", "itemX"); err != nil {
		t.Error("update token cart failed, err:", err)
	}
	url := "http://test.com/?item=itemX"
	t.Logf("We are going to cache a simple request against %s", url)
	result, err := ch2.CacheRequest(url, func(request string) string {
		return "content for " + request
	})
	if err != nil {
		t.Error("cache request failed, err:", err)
	}
	t.Logf("We got initial content: %s", result)

	assert.Equal("content for http://test.com/?item=itemX", result)

	t.Log("To test that we've cached the request, we'll pass a bad callback")
	result2, err := ch2.CacheRequest(url, nil)
	if err != nil {
		t.Error("cache request failed, err:", err)
	}
	t.Logf("We ended up getting the same response! %s", result2)

	assert.Equal(result, result2)
}

func TestCh2_CacheRows(t *testing.T) {
	assert := as.New(t)

	t.Log("First, let's schedule caching of itemX every 5 seconds")
	if err := ch2.ScheduleRowCache("itemX", 5); err != nil {
		t.Error("schedule row cache failed, err:", err)
	}
	schedule, err := ch2.RC.ZRangeWithScores(ctx, util.KeySchedule, 0, -1).Result()
	if err != nil {
		t.Error("range score failed, err:", err)
	}
	t.Logf("Our schedule looks like: %+v", schedule)
	assert.True(len(schedule) > 0)

	t.Log("We'll start a caching thread that will cache the data...")
	ch2.CacheRows()
	time.Sleep(time.Second)

	rowItemX := fmt.Sprintf(util.KeyInvRow, "itemX")
	row, err := ch2.RC.Get(ctx, rowItemX).Result()
	if err != nil {
		t.Error("get row failed, err:", err)
	}
	t.Logf("Our cached data looks like %s", row)
	assert.NotEqual("", row)

	t.Log("We'll check again in 5 seconds...")
	time.Sleep(5 * time.Second)

	row2, err := ch2.RC.Get(ctx, rowItemX).Result()
	if err != nil {
		t.Error("get row failed, err:", err)
	}
	t.Logf("Notice that the data has changed...%s", row2)
	assert.NotEqual("", row2)
	assert.NotEqual(row, row2)

	t.Log("Let's force un-caching")
	if err = ch2.ScheduleRowCache("itemX", -1); err != nil {
		t.Error("schedule row cache failed, err:", err)
	}
	time.Sleep(time.Second)
	row, err = ch2.RC.Get(ctx, rowItemX).Result()
	assert.Equal(redis.Nil, err)
	t.Logf("The cache was cleared? %v", row == "")
	assert.Equal("", row)
}

func TestMain(m *testing.M) {
	ch2 = NewCh2(redisConn.ConnectRedis())
	defer ch2.Reset()
	m.Run()
}

func Test_requestItemId(t *testing.T) {
	tests := []struct {
		name    string
		request string
		want    string
	}{
		{"含有item为itemX", "https://sf.com/?item=itemX", "itemX"},
		{"不含有item为itemX", "https://sf.com/", ""},
		{"动态url请求", "https://sf.com/?item=itemX&_=1234536", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := requestItemId(tt.request); got != tt.want {
				t.Errorf("requestItemId() = %v, want %v", got, tt.want)
			}
		})
	}
}
