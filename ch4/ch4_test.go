// @文件名: ch4_test.go
// @作者: 邓一鸣
// @创建时间: 2021/10/12 4:31 下午
// @描述:
package ch4

import (
	"fmt"
	"redis-in-action/redisConn"
	"redis-in-action/util"
	"testing"

	"github.com/go-redis/redis/v8"
	as "github.com/stretchr/testify/assert"
)

var ch4 *Ch4

func TestMain(m *testing.M) {
	ch4 = NewCh4(redisConn.ConnectRedis())
	defer ch4.Reset()
	m.Run()
}

func TestItem(t *testing.T) {
	assert := as.New(t)

	t.Log("We need to set up just enough state so that a user can list an item")
	sellerName := "userX"
	sellerInv := fmt.Sprintf(util.KeyInventorySeller, sellerName)
	item := "itemX"
	rc := ch4.RC

	if err := rc.SAdd(ctx, sellerInv, item).Err(); err != nil {
		t.Error("add item failed, err:", err)
	}
	result, err := rc.SMembers(ctx, sellerInv).Result()
	if err != nil {
		t.Error("list items failed, err:", err)
	}
	t.Logf("The user's inventory has: %+v", result)
	//assert.Greater(len(result), 0)
	assert.NotEmpty(result)

	t.Log("Listing the item...")
	if err = ch4.ListItem(item, sellerName, 10); err != nil {
		t.Error("list item failed, err:", err)
	}
	t.Log("Listing the item succeeded?", err == nil)
	assert.Nil(err)

	market, err := rc.ZRangeWithScores(ctx, util.KeyMarket, 0, -1).Result()
	if err != nil {
		t.Error("list market failed, err:", err)
	}
	t.Log("The market contains:", market)
	//assert.Greater(len(market), 0)
	assert.NotEmpty(market)
	assert.Contains(market, redis.Z{
		Score:  10,
		Member: "itemX.userX",
	})

	//purchase item
	t.Log("We need to set up just enough state so a user can buy an item")
	buyerName := "userY"
	buyer := fmt.Sprintf(util.KeyUser, buyerName)
	buyerInv := fmt.Sprintf(util.KeyInventorySeller, buyerName)
	if err = rc.HSet(ctx, buyer, util.FieldFunds, 125).Err(); err != nil {
		t.Error("set buyer failed, err:", err)
	}
	b, err := rc.HGetAll(ctx, buyer).Result()
	if err != nil {
		t.Error("get buyer failed, err:", err)
	}
	t.Log("The user has some money:", b)
	//assert.Greater(len(b), 0)
	assert.NotEmpty(b)
	assert.NotEmpty(b[util.FieldFunds])

	t.Log("Let's purchase an item")
	if err = ch4.PurchaseItem(buyerName, item, sellerName, 10); err != nil {
		t.Error("purchase item failed, err:", err)
	}
	t.Log("Purchasing an item succeeded?", err == nil)
	assert.Nil(err)
	b, err = rc.HGetAll(ctx, buyer).Result()
	if err != nil {
		t.Error("get buyer failed, err:", err)
	}
	t.Log("Their money is now:", b)
	assert.NotEmpty(b)
	invs, err := rc.SMembers(ctx, buyerInv).Result()
	if err != nil {
		t.Error("get buyer inventory failed, err:", err)
	}
	t.Log("Their inventory is now:", invs)
	assert.NotEmpty(invs)
	assert.Contains(invs, item)
	_, err = rc.ZScore(ctx, util.KeyMarket, fmt.Sprintf(util.ItemSeller, item, sellerName)).Result()
	assert.Equal(err, redis.Nil)
}
