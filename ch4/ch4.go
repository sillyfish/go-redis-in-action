// @文件名: ch4
// @作者: 邓一鸣
// @创建时间: 2021/10/12 3:14 下午
// @描述:
package ch4

import (
	"context"
	"errors"
	"fmt"
	"log"
	"redis-in-action/util"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"
)

var ctx = context.Background()

var (
	ErrNoItem        = errors.New("no item")
	ErrOperateFailed = errors.New("operate failed")
)

type Ch4 struct {
	RC *redis.Client
}

func NewCh4(rc *redis.Client) *Ch4 {
	return &Ch4{RC: rc}
}

func (c *Ch4) Reset() {
	c.RC.FlushDB(ctx)
}

func (c *Ch4) ListItem(itemId, sellerId string, price float64) error {
	rc := c.RC

	ctxTimeout, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	inventory := fmt.Sprintf(util.KeyInventorySeller, sellerId)
	item := fmt.Sprintf(util.ItemSeller, itemId, sellerId)

	for {
		if err := rc.Watch(ctxTimeout, func(tx *redis.Tx) error {
			isMember, err := tx.SIsMember(ctxTimeout, inventory, itemId).Result()
			if err != nil {
				return err
			}
			if !isMember {
				return ErrNoItem
			}

			_, err = tx.TxPipelined(ctxTimeout, func(pipe redis.Pipeliner) error {
				if err = pipe.ZAdd(ctxTimeout, util.KeyMarket, &redis.Z{
					Score:  price,
					Member: item,
				}).Err(); err != nil {
					return err
				}
				return pipe.SRem(ctxTimeout, inventory, itemId).Err()
			})
			return err
		}, inventory); err == context.DeadlineExceeded {
			log.Println("context deadline exceeded")
			break
		} else if err != redis.TxFailedErr {
			return err
		}
		continue
	}
	return nil
}

func (c *Ch4) PurchaseItem(buyerId, itemId, sellerId string, lPrice float64) error {
	rc := c.RC

	ctxTimeout, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	buyer := fmt.Sprintf(util.KeyUser, buyerId)
	seller := fmt.Sprintf(util.KeyUser, sellerId)
	item := fmt.Sprintf(util.ItemSeller, itemId, sellerId)
	inventory := fmt.Sprintf(util.KeyInventorySeller, buyerId)

	for {
		if err := rc.Watch(ctxTimeout, func(tx *redis.Tx) error {
			price, err := tx.ZScore(ctxTimeout, util.KeyMarket, item).Result()
			if err != nil {
				return err
			}
			fs, err := tx.HGet(ctxTimeout, buyer, util.FieldFunds).Result()
			if err != nil {
				return err
			}
			funds, err := strconv.ParseFloat(fs, 64)
			if err != nil {
				return err
			}
			if price != lPrice || price > funds {
				return ErrOperateFailed
			}

			_, err = tx.TxPipelined(ctxTimeout, func(pipe redis.Pipeliner) error {
				if err = pipe.HIncrBy(ctxTimeout, seller, util.FieldFunds, int64(price)).Err(); err != nil {
					return err
				}
				if err = pipe.HIncrBy(ctxTimeout, buyer, util.FieldFunds, int64(-price)).Err(); err != nil {
					return err
				}
				if err = pipe.SAdd(ctxTimeout, inventory, itemId).Err(); err != nil {
					return err
				}
				if err = pipe.ZRem(ctxTimeout, util.KeyMarket, item).Err(); err != nil {
					return err
				}
				return nil
			})
			return err
		}, util.KeyMarket, buyer); err == context.DeadlineExceeded {
			log.Println("context deadline exceeded")
			break
		} else if err != redis.TxFailedErr {
			return err
		}
		continue
	}
	return nil
}
