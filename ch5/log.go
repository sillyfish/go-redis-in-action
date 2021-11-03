// @文件名: log
// @作者: 邓一鸣
// @创建时间: 2021/11/2 4:13 下午
// @描述:
package ch5

import (
	"context"
	"fmt"
	"log"
	"redis-in-action/util"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"
)

type LogSeverity string

const (
	Debug    LogSeverity = "debug"
	Info     LogSeverity = "info"
	Warning  LogSeverity = "warning"
	Error    LogSeverity = "error"
	Critical LogSeverity = "critical"
)

var ctx = context.Background()

type Log struct {
	RC *redis.Client
}

func NewLog(rc *redis.Client) *Log {
	return &Log{RC: rc}
}

func (l *Log) Reset() {
	log.Println("reset")
	l.RC.FlushDB(ctx)
}

func (l *Log) Recent(name, message string, severity LogSeverity, pipe redis.Pipeliner) error {
	destination := fmt.Sprintf(util.KeyLogRecent, name, string(severity))
	message = time.Now().String() + " " + message
	if pipe == nil {
		pipe = l.RC.Pipeline()
	}
	if err := pipe.LPush(ctx, destination, message).Err(); err != nil {
		return err
	}
	if err := pipe.LTrim(ctx, destination, 0, 99).Err(); err != nil {
		return err
	}
	_, err := pipe.Exec(ctx)
	return err
}

func (l *Log) Common(name, message string, severity LogSeverity) error {
	destination := fmt.Sprintf(util.KeyLogCommon, name, string(severity))
	startKey := destination + util.KeyStartSuf

	ctxTimeout, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	for {
		if err := l.RC.Watch(ctxTimeout, func(tx *redis.Tx) error {
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
			_, err = tx.TxPipelined(ctxTimeout, func(pipe redis.Pipeliner) error {
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

				if err = pipe.ZIncrBy(ctxTimeout, destination, 1, message).Err(); err != nil {
					return err
				}
				return l.Recent(name, message, severity, pipe)
			})
			return err
		}, startKey); err == context.DeadlineExceeded {
			break
		} else if err != redis.TxFailedErr {
			return err
		}
		continue
	}
	return nil
}
