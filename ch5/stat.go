// @文件名: stat
// @作者: 邓一鸣
// @创建时间: 2021/11/5 4:15 下午
// @描述:
package ch5

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"redis-in-action/util"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
)

type Stat struct {
	RC *redis.Client
}

func NewStat(rc *redis.Client) *Stat {
	return &Stat{RC: rc}
}

func (s *Stat) Reset() {
	s.RC.FlushDB(ctx)
}

type St struct {
	Min     float64 `json:"min"`
	Max     float64 `json:"max"`
	Count   float64 `json:"count"`
	Sum     float64 `json:"sum"`
	SumSQ   float64 `json:"sumsq"`
	Average float64 `json:"-"`
	StdDev  float64 `json:"_"`
}

func NewSt(zs []redis.Z) (*St, error) {
	st := &St{}
	js := "{"
	for _, z := range zs {
		if js != "{" {
			js += ", "
		}
		if prop, ok := z.Member.(string); ok {
			js += fmt.Sprintf(util.JsonStats, prop, z.Score)
		}
	}
	js += "}"
	log.Println("js:", js)
	if err := json.Unmarshal([]byte(js), st); err != nil {
		return nil, err
	}
	count := st.Count
	if count == 0 {
		return nil, nil
	}
	sum := st.Sum
	st.Average = sum / count

	sumsq := st.SumSQ
	st.StdDev = math.Sqrt((sumsq - sum*sum/count) / math.Max(count-1, 1))
	return st, nil
}

func (s *Stat) Update(ctxString, ty string, value float64) ([]float64, error) {
	destination := fmt.Sprintf(util.KeyStats, ctxString, ty)
	startKey := destination + util.KeyStartSuf

	ctxTimeout, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	results := make([]float64, 0, 3)

	for {
		if err := s.RC.Watch(ctxTimeout, func(tx *redis.Tx) error {
			hour := time.Now().Hour()

			existing, err := tx.Get(ctxTimeout, startKey).Result()
			var eHour int
			if err != redis.Nil {
				if err != nil {
					return err
				}
				eHour, err = strconv.Atoi(existing)
				if err != nil {
					return err
				}
			}
			cmders, err := tx.TxPipelined(ctxTimeout, func(pipe redis.Pipeliner) error {
				if err == redis.Nil {
					if err = pipe.Set(ctxTimeout, startKey, hour, redis.KeepTTL).Err(); err != nil {
						return err
					}
				} else if eHour < hour {
					if err = pipe.Rename(ctxTimeout, destination, destination+util.KeyLastSuf).Err(); err != nil {
						return err
					}
					if err = pipe.Rename(ctxTimeout, startKey, destination+util.KeyPStartSuf).Err(); err != nil {
						return err
					}
					if err = pipe.Set(ctxTimeout, startKey, hour, redis.KeepTTL).Err(); err != nil {
						return err
					}
				}

				tKey1 := uuid.NewString()
				tKey2 := uuid.NewString()
				pipe.ZAdd(ctxTimeout, tKey1, &redis.Z{
					Score:  value,
					Member: "min",
				})
				pipe.ZAdd(ctxTimeout, tKey2, &redis.Z{
					Score:  value,
					Member: "max",
				})
				pipe.ZUnionStore(ctxTimeout, destination, &redis.ZStore{
					Keys:      []string{destination, tKey1},
					Aggregate: "MIN",
				})
				pipe.ZUnionStore(ctxTimeout, destination, &redis.ZStore{
					Keys:      []string{destination, tKey2},
					Aggregate: "MAX",
				})

				pipe.Del(ctxTimeout, tKey1, tKey2)
				pipe.ZIncrBy(ctxTimeout, destination, 1, "count")
				pipe.ZIncrBy(ctxTimeout, destination, value, "sum")
				pipe.ZIncrBy(ctxTimeout, destination, value*value, "sumsq")
				return nil
			})
			if err != nil {
				return err
			}
			length := len(cmders)
			for i := length - 3; i < length; i++ {
				if fc, ok := cmders[i].(*redis.FloatCmd); ok {
					results = append(results, fc.Val())
				}
			}
			return nil
		}, startKey); err == nil || err == context.DeadlineExceeded {
			break
		} else if err != redis.TxFailedErr {
			return nil, err
		}
		continue
	}
	return results, nil
}

func (s *Stat) Get(ctxString, ty string) (*St, error) {
	destination := fmt.Sprintf(util.KeyStats, ctxString, ty)
	zs, err := s.RC.ZRangeWithScores(ctx, destination, 0, -1).Result()
	if err != nil {
		return nil, err
	}
	return NewSt(zs)
}

func (s *Stat) AccessTime(ctxString string, callback func()) error {
	start := time.Now()

	callback()
	delta := time.Since(start)
	stats, err := s.Update(ctxString, "AccessTime", float64(delta))
	if err != nil {
		return err
	}

	pipe := s.RC.Pipeline()
	pipe.ZAdd(ctx, util.KeySlowestAccessTime, &redis.Z{
		Score:  stats[1] / stats[0],
		Member: ctxString,
	})
	pipe.ZRemRangeByRank(ctx, util.KeySlowestAccessTime, 0, -101)
	_, err = pipe.Exec(ctx)
	return err
}
