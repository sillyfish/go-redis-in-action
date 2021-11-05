// @文件名: counter
// @作者: 邓一鸣
// @创建时间: 2021/11/3 11:35 上午
// @描述:
package ch5

import (
	"fmt"
	"math"
	"redis-in-action/util"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
)

var Precision = []int{1, 5, 60, 60 * 5, 60 * 60, 60 * 60 * 3, 60 * 60 * 24}

type Counter struct {
	RC *redis.Client
}

func NewCounter(rc *redis.Client) *Counter {
	return &Counter{RC: rc}
}

func (c *Counter) Reset() {
	c.RC.FlushDB(ctx)
}

func (c *Counter) Update(name string, count int64, now time.Time) error {
	pipe := c.RC.Pipeline()
	for _, prec := range Precision {
		pnow := strconv.Itoa(int(now.Unix()) / prec * prec)
		hash := fmt.Sprintf(util.PrecName, prec, name)
		pipe.ZAdd(ctx, util.KeyKnown, &redis.Z{
			Score:  float64(count),
			Member: hash,
		})
		pipe.HIncrBy(ctx, fmt.Sprintf(util.KeyCountHash, hash), pnow, count)
	}
	_, err := pipe.Exec(ctx)
	return err
}

type Datum struct {
	Time  int64
	Count int64
}

func (c *Counter) Get(name string, precision int) ([]Datum, error) {
	hash := fmt.Sprintf(util.KeyCountPrecName, precision, name)
	result, err := c.RC.HGetAll(ctx, hash).Result()
	if err != nil {
		return nil, err
	}
	//var keys []string
	data := make([]Datum, 0, len(result))
	for key, value := range result {
		now, err := strconv.ParseInt(key, 10, 64)
		if err != nil {
			return nil, err
		}
		count, _ := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return nil, err
		}
		data = append(data, Datum{
			Time:  now,
			Count: count,
		})
	}
	sort.Slice(data, func(i, j int) bool {
		return data[i].Time < data[j].Time
	})
	return data, nil
}

func (c *Counter) Clean(countLeftChan <-chan int64, timeNow func() time.Time) {
	var passes int
	rc := c.RC
	var countLeft int64 = 100
	for {
		start := timeNow()
		var index int64
		for index < rc.ZCard(ctx, util.KeyKnown).Val() {
			result, err := rc.ZRange(ctx, util.KeyKnown, index, index).Result()
			index++
			if err != nil || len(result) == 0 {
				break
			}
			hash := result[0]
			prec, err := strconv.Atoi(strings.Split(hash, ":")[0])
			if err != nil {
				break
			}
			if prec > 60 && passes%(prec/60) > 0 {
				continue
			}

			hkey := fmt.Sprintf(util.KeyCountHash, hash)
			select {
			case countLeft = <-countLeftChan:
			default:
			}
			cutoff := timeNow().Unix() - countLeft*int64(prec)
			samples, err := rc.HKeys(ctx, hkey).Result()
			if err != nil {
				break
			}
			remove := make([]string, 0, len(samples))
			for _, sample := range samples {
				st, err := strconv.ParseInt(sample, 10, 64)
				if err != nil {
					continue
				}
				if st <= cutoff {
					remove = append(remove, strconv.FormatInt(st, 10))
				}
			}
			if len(remove) > 0 {
				if err = rc.HDel(ctx, hkey, remove...).Err(); err != nil {
					break
				}
				if len(remove) == len(samples) {
					if err = rc.Watch(ctx, func(tx *redis.Tx) error {
						if tx.HLen(ctx, hkey).Val() > 0 {
							return nil
						}
						_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
							if err = pipe.ZRem(ctx, util.KeyKnown, hash).Err(); err != nil {
								return err
							}
							index -= 1
							return nil
						})
						return err
					}, hkey); err != nil && err != redis.TxFailedErr {
						break
					}
				}
			}
		}

		passes++
		time.Sleep(time.Second * time.Duration(math.Max(60-time.Since(start).Seconds(), 1)))
	}
}
