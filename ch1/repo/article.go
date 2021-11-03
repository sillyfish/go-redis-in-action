// @文件名: ch1
// @作者: 邓一鸣
// @创建时间: 2021/10/9 2:43 下午
// @描述:
package repo

import (
	"context"
	"fmt"
	"redis-in-action/util"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/mitchellh/mapstructure"
)

type Article struct {
	Id     string  `mapstructure:",omitempty"`
	Title  string  `json:"title" mapstructure:"title"`
	Link   string  `json:"link" mapstructure:"link"`
	Poster string  `json:"poster" mapstructure:"poster"`
	Time   float64 `json:"time" mapstructure:"time"`
	Votes  int     `json:"votes" mapstructure:"votes"`
}

func (a *Article) Map() (map[string]interface{}, error) {
	var m map[string]interface{}
	err := mapstructure.Decode(a, &m)
	return m, err
}

func NewArticle(user, title, link string) *Article {
	return &Article{
		Title:  title,
		Link:   link,
		Poster: user,
		Time:   util.TimeNowUnix(),
		Votes:  1,
	}
}

func NewArticleWithMap(m map[string]string, id string) (*Article, error) {
	article := &Article{}
	articleMap := make(map[string]interface{})
	for k, v := range m {
		var value interface{}
		var err error
		switch k {
		case "time":
			value, err = strconv.ParseFloat(v, 64)
			if err != nil {
				return nil, err
			}
		case "votes":
			value, err = strconv.Atoi(v)
			if err != nil {
				return nil, err
			}
		default:
			value = v
		}
		articleMap[k] = value
	}
	if err := mapstructure.Decode(articleMap, article); err != nil {
		return nil, err
	}
	article.Id = id
	return article, nil
}

var ctx = context.Background()

const (
	OneWeek           = 7 * 24 * 60 * 60 * time.Second
	VoteScore float64 = 432

	ArticlesPerPage int = 25
)

type ArticleRepo struct {
	RC *redis.Client
}

func NewArticleRepo(client *redis.Client) *ArticleRepo {
	return &ArticleRepo{RC: client}
}

func (ar *ArticleRepo) Vote(user, article string) error {
	rc := ar.RC
	aTime, err := rc.ZScore(ctx, util.KeyArticleTime, article).Result()
	if err != nil {
		return err
	}
	if aTime < util.TimeNowUnix()-float64(OneWeek) {
		return nil
	}

	as := strings.Split(article, ":")
	articleId := as[len(as)-1]
	result, err := rc.SAdd(ctx, fmt.Sprintf(util.KeyArticleVoted, articleId), user).Result()
	if err != nil {
		return err
	}

	if result == 0 {
		return nil
	}
	if _, err = rc.ZIncrBy(ctx, util.KeyArticleScore, VoteScore, article).Result(); err != nil {
		return err
	}
	if _, err = rc.HIncrBy(ctx, article, "votes", 1).Result(); err != nil {
		return err
	}
	return nil
}

func (ar *ArticleRepo) Post(article *Article) (id string, err error) {
	rc := ar.RC
	result, err := rc.Incr(ctx, util.KeyArticle).Result()
	if err != nil {
		return
	}
	articleId := strconv.Itoa(int(result))
	votedKey := fmt.Sprintf(util.KeyArticleVoted, articleId)
	if _, err = rc.SAdd(ctx, votedKey, article.Poster).Result(); err != nil {
		return
	}
	if _, err = rc.Expire(ctx, votedKey, OneWeek).Result(); err != nil {
		return
	}

	articleKey := fmt.Sprintf(util.KeyArticleId, articleId)
	//now := float64(time.Now().Unix())
	articleMap, err := article.Map()
	if err != nil {
		return
	}
	if _, err = rc.HSet(ctx, articleKey, articleMap).Result(); err != nil {
		return
	}

	if _, err = rc.ZAdd(ctx, util.KeyArticleScore, &redis.Z{
		Score:  VoteScore + article.Time,
		Member: articleKey,
	}).Result(); err != nil {
		return
	}

	if _, err = rc.ZAdd(ctx, util.KeyArticleTime, &redis.Z{
		Score:  article.Time,
		Member: articleKey,
	}).Result(); err != nil {
		return
	}
	return articleId, nil
}

func (ar *ArticleRepo) List(page int, key string) ([]Article, error) {
	rc := ar.RC
	ids, err := rc.ZRevRange(ctx, key, int64((page-1)*ArticlesPerPage), int64(page*ArticlesPerPage-1)).Result()
	if err != nil {
		return nil, err
	}
	articles := make([]Article, 0, len(ids))
	for _, id := range ids {
		articleMap, err := rc.HGetAll(ctx, id).Result()
		if err != nil {
			return nil, err
		}
		article, err := NewArticleWithMap(articleMap, id)
		if err != nil {
			return nil, err
		}
		articles = append(articles, *article)
	}
	return articles, nil
}

func (ar *ArticleRepo) AddRemoveGroup(articleId string, add, remove []string) error {
	rc := ar.RC
	article := fmt.Sprintf(util.KeyArticleId, articleId)
	for _, a := range add {
		if _, err := rc.SAdd(ctx, fmt.Sprintf(util.KeyGroup, a), article).Result(); err != nil {
			return err
		}
	}
	for _, r := range remove {
		if _, err := rc.SRem(ctx, fmt.Sprintf(util.KeyGroup, r), article).Result(); err != nil {
			return err
		}
	}
	return nil
}

func (ar *ArticleRepo) ListInGroup(group string, page int, key string) ([]Article, error) {
	rc := ar.RC
	groupKey := key + group
	exist, err := rc.Exists(ctx, groupKey).Result()
	if err != nil {
		return nil, err
	}
	if exist == 0 {
		rc.ZInterStore(ctx, groupKey, &redis.ZStore{
			Keys:      []string{fmt.Sprintf(util.KeyGroup, group), key},
			Aggregate: "MAX",
		})
		rc.Expire(ctx, groupKey, 60*time.Second)
	}
	return ar.List(page, groupKey)
}

func (ar *ArticleRepo) Reset() {
	ar.RC.FlushDB(ctx)
}
