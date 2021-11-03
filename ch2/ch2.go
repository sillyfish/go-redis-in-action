// @文件名: ch2
// @作者: 邓一鸣
// @创建时间: 2021/10/12 3:14 下午
// @描述:
package ch2

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/url"
	"redis-in-action/util"
	"time"

	"github.com/go-redis/redis/v8"
)

var ctx = context.Background()

type Ch2 struct {
	RC *redis.Client
}

type Row struct {
	Id     string
	Data   string
	Cached float64
}

func NewRow(id string) *Row {
	return &Row{
		Id:     id,
		Data:   "data to cache...",
		Cached: util.TimeNowUnix(),
	}
}

func NewCh2(rc *redis.Client) *Ch2 {
	return &Ch2{RC: rc}
}

func (c *Ch2) CheckToken(token string) (string, error) {
	return c.RC.HGet(ctx, util.KeyLogin, token).Result()
}

func (c *Ch2) UpdateToken(token, user, item string) error {
	rc := c.RC
	ts := util.TimeNowUnix()
	if _, err := rc.HSet(ctx, util.KeyLogin, token, user).Result(); err != nil {
		return err
	}
	if _, err := rc.ZAdd(ctx, util.KeyRecent, &redis.Z{
		Score:  ts,
		Member: token,
	}).Result(); err != nil {
		return err
	}
	if item != "" {
		viewedTokenKey := fmt.Sprintf(util.KeyViewedToken, token)
		if _, err := rc.ZAdd(ctx, viewedTokenKey, &redis.Z{
			Score:  ts,
			Member: item,
		}).Result(); err != nil {
			return err
		}
		if _, err := rc.ZRemRangeByRank(ctx, viewedTokenKey, 0, -26).Result(); err != nil {
			return err
		}
		if _, err := rc.ZIncrBy(ctx, util.KeyViewed, -1, item).Result(); err != nil {
			return err
		}
	}
	return nil
}

func (c *Ch2) CleanSessions(ch chan int64) {
	var limit int64 = 1e7
	rc := c.RC
	go func() {
		limit = <-ch
	}()
	go func() {
		for {
			size, err := rc.ZCard(ctx, util.KeyRecent).Result()
			if err != nil || size <= limit {
				time.Sleep(time.Second)
				continue
			}
			end := math.Min(float64(size-limit), 100)
			tokens, err := rc.ZRange(ctx, util.KeyRecent, 0, int64(end)).Result()
			if err != nil {
				time.Sleep(time.Second)
				continue
			}
			delKeys := make([]string, 0, 2*len(tokens))
			ts := make([]interface{}, 0, len(tokens))
			for _, token := range tokens {
				ts = append(ts, token)
				delKeys = append(delKeys,
					fmt.Sprintf(util.KeyViewedToken, token),
					fmt.Sprintf(util.KeyCartToken, token))
			}
			if _, err = rc.Del(ctx, delKeys...).Result(); err != nil {
				time.Sleep(time.Second)
				continue
			}
			if _, err = rc.HDel(ctx, util.KeyLogin, tokens...).Result(); err != nil {
				time.Sleep(time.Second)
				continue
			}
			if _, err = rc.ZRem(ctx, util.KeyRecent, ts...).Result(); err != nil {
				time.Sleep(time.Second)
				continue
			}
		}
	}()
}

func (c *Ch2) Add2Cart(session, item string, count int) error {
	rc := c.RC
	cartTokenKey := fmt.Sprintf(util.KeyCartToken, session)
	if count <= 0 {
		if _, err := rc.HDel(ctx, cartTokenKey, item).Result(); err != nil {
			return err
		}
	} else {
		rc.HSet(ctx, cartTokenKey, item, count)
	}
	return nil
}

func (c *Ch2) CacheRequest(request string, callback func(string) string) (string, error) {
	rc := c.RC
	if !c.canCache(request) {
		return callback(request), nil
	}

	cacheRequestKey := fmt.Sprintf(util.KeyCacheRequest, request)
	content, err := rc.Get(ctx, cacheRequestKey).Result()
	if err != nil {
		if err == redis.Nil {
			content = callback(request)
			rc.SetEX(ctx, cacheRequestKey, content, 300*time.Second)
		} else {
			return "", err
		}
	}

	return content, nil
}

func (c *Ch2) ScheduleRowCache(rowId string, delay float64) error {
	rc := c.RC
	if _, err := rc.ZAdd(ctx, util.KeyDelay, &redis.Z{Score: delay, Member: rowId}).Result(); err != nil {
		return err
	}
	if _, err := rc.ZAdd(ctx, util.KeySchedule, &redis.Z{Score: util.TimeNowUnix(), Member: rowId}).Result(); err != nil {
		return err
	}
	return nil
}

func (c *Ch2) CacheRows() {
	rc := c.RC
	go func() {
		for {
			sleep := func() bool {
				zs, err := rc.ZRangeWithScores(ctx, util.KeySchedule, 0, 0).Result()
				if err != nil {
					return true
				}
				if len(zs) == 0 || zs[0].Score > util.TimeNowUnix() {
					return true
				}
				rowId := zs[0].Member.(string)
				delay, err := rc.ZScore(ctx, util.KeyDelay, rowId).Result()
				if err != nil {
					return true
				}
				invRowKey := fmt.Sprintf(util.KeyInvRow, rowId)
				if delay <= 0 {
					if _, err = rc.ZRem(ctx, util.KeyDelay, rowId).Result(); err != nil {
						return true
					}
					if _, err = rc.ZRem(ctx, util.KeySchedule, rowId).Result(); err != nil {
						return true
					}
					if _, err = rc.Del(ctx, invRowKey).Result(); err != nil {
						return true
					}
					return false
				}
				if _, err = rc.ZAdd(ctx, util.KeySchedule, &redis.Z{
					Score:  util.TimeNowUnix() + delay,
					Member: rowId,
				}).Result(); err != nil {
					return true
				}
				rowJson, err := json.Marshal(NewRow(rowId))
				if err != nil {
					return true
				}
				if _, err = rc.Set(ctx, invRowKey, rowJson, 0).Result(); err != nil {
					return true
				}
				return true
			}()
			if sleep {
				time.Sleep(5 * time.Second / 100)
			}
		}
	}()
}

func (c *Ch2) RescaleViewed() {
	rc := c.RC
	go func() {
		for {
			if _, err := rc.ZRemRangeByRank(ctx, util.KeyViewed, 20000, -1).Result(); err != nil {
				continue
			}
			if rc.ZInterStore(ctx, util.KeyViewed, &redis.ZStore{
				Keys:    []string{util.KeyViewed},
				Weights: []float64{0.5},
			}).Err() != nil {
				continue
			}
			time.Sleep(300 * time.Second)
		}
	}()
}

func (c *Ch2) canCache(request string) bool {
	itemId := requestItemId(request)
	if itemId == "" {
		return false
	}
	rank, err := c.RC.ZRank(ctx, util.KeyViewed, itemId).Result()
	if err != nil {
		return false
	}
	return rank < 10000
}

func requestItemId(request string) string {
	uri, err := url.ParseRequestURI(request)
	if err != nil {
		return ""
	}
	query, err := url.ParseQuery(uri.RawQuery)
	if err != nil {
		return ""
	}
	if _, ok := query["_"]; ok {
		return ""
	}
	queries := query["item"]
	if len(queries) == 0 {
		return ""
	}
	return queries[0]
}

func (c *Ch2) Reset() {
	c.RC.FlushDB(ctx)
}
