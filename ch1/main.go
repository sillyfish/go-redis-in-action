// @文件名: main
// @作者: 邓一鸣
// @创建时间: 2021/10/9 5:09 下午
// @描述:
package main

import (
	"context"
	"fmt"
	"log"
	"redis-in-action/ch1/repo"
	"redis-in-action/redisConn"
	"redis-in-action/util"
)

var ctx = context.Background()

func main() {
	articleRepo := repo.NewArticleRepo(redisConn.ConnectRedis())
	defer articleRepo.Reset()

	id, err := articleRepo.Post(repo.NewArticle("username", "A title", "http://www.google.com"))
	if err != nil {
		log.Fatalf("post article failed: %v", err)
	}
	log.Printf("We posted a new article with id:%s", id)

	article := fmt.Sprintf(util.KeyArticleId, id)
	articleMap, err := articleRepo.RC.HGetAll(ctx, article).Result()
	if err != nil {
		log.Fatalf("get article failed: %v", err)
	}
	log.Printf("Its HASH looks like:%v", articleMap)

	if err = articleRepo.Vote("other_user", article); err != nil {
		log.Fatalf("vote article failed: %v", err)
	}
	votes, err := articleRepo.RC.HGet(ctx, article, "votes").Result()
	if err != nil {
		return
	}
	log.Printf("We voted for the article, it now has votes: %s", votes)

	_, err = articleRepo.Post(repo.NewArticle("username", "Second title", "http://www.baidu.com"))
	if err != nil {
		log.Fatalf("post article failed: %v", err)
	}

	articles, err := articleRepo.List(1, util.KeyArticleScore)
	if err != nil {
		log.Fatalf("list article failed: %v", err)
	}
	log.Printf("The currently highest-scoring articles are:%+v", articles)

	groupName := "new-group"
	if err = articleRepo.AddRemoveGroup(id, []string{groupName}, nil); err != nil {
		log.Fatalf("add remove group failed: %v", err)
	}
	articles, err = articleRepo.ListInGroup(groupName, 1, util.KeyArticleScore)
	if err != nil {
		log.Fatalf("list article in group failed: %v", err)
	}
	log.Printf("We added the article to a new group, other articles includes:%+v", articles)
}
