package svc

import (
	"im-platform/app/user/rpc/internal/config"
	"im-platform/app/user/rpc/models"
	"im-platform/common/utils"

	_ "github.com/lib/pq"
	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/core/stores/sqlx"
)
type ServiceContext struct {
	Config config.Config
	UsersModel models.UsersModel
	DevicesModel models.DevicesModel
	Snokflake *utils.Snowflake
	Redis  redis.Redis
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
		UsersModel: models.NewUsersModel(sqlconn,c.Cache),
		Snokflake: Snokflake,
		DevicesModel: models.NewDevicesModel(sqlconn,c.Cache),
		Redis: *rds,
	}
}
