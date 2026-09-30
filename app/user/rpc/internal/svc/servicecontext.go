package svc

import (
	"im-platform/app/user/rpc/internal/config"
	"im-platform/app/user/rpc/models"
	"im-platform/common/utils"
	"time"

	_ "github.com/lib/pq"
	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/core/stores/sqlx"
)
type ServiceContext struct {
	Config config.Config
	UsersModel models.UsersModel
	DevicesModel models.DevicesModel
	FriendsModel models.FriendsModel
	FriendAppliesModel models.FriendappliesModel
	FriendGroupsModel models.FriendgroupsModel
	Snokflake *utils.Snowflake
	Redis  redis.Redis
	TokenManager *utils.TokenManager
}

func NewServiceContext(c config.Config) *ServiceContext {
	sqlconn:=sqlx.NewSqlConn("postgres",c.Postgres.DataSource)
	Snokflake,err:= utils.NewSnowflake(c.SnokFlake.WorkNode)
	if err!=nil{
		panic(err)
	}
	rds :=redis.MustNewRedis(c.RedisCache)
	return &ServiceContext{
		Config: c,
		UsersModel: models.NewUsersModel(sqlconn),
		Snokflake: Snokflake,
		DevicesModel: models.NewDevicesModel(sqlconn),
		Redis: *rds,
		FriendsModel: models.NewFriendsModel(sqlconn),
		FriendGroupsModel: models.NewFriendgroupsModel(sqlconn),
		FriendAppliesModel: models.NewFriendappliesModel(sqlconn),
		TokenManager: utils.NewTokenManager(rds, 7*24*time.Hour),
	}
}
