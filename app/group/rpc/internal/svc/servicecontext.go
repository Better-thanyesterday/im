package svc

import (
	"im-platform/app/group/rpc/internal/config"
	"im-platform/app/group/rpc/models"
	"im-platform/common/utils"
	_ "github.com/lib/pq"
	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

type ServiceContext struct {
	Config config.Config
	GroupsModel models.GroupsModel
	GroupMembersModel models.GroupmembersModel
	GroupSettingsModel models.GroupsettingsModel
	GroupAppliesModel models.GroupappliesModel
	Snowflake     *utils.Snowflake
}

func NewServiceContext(c config.Config) *ServiceContext {
	sqlconn:=sqlx.NewSqlConn("postgres",c.Postgres.DataSource)
	group:=models.NewGroupsModel(sqlconn)
	gmember:=models.NewGroupmembersModel(sqlconn)
	gsettings:=models.NewGroupsettingsModel(sqlconn)
	gapplies:=models.NewGroupappliesModel(sqlconn)
	snokflake,err:=utils.NewSnowflakeOrAuto(c.Snowflake.WorkNode)
	if err!=nil{
		panic(err)
	}
	return &ServiceContext{
		Config: c,
		GroupsModel: group,
		Snowflake: snokflake,
		GroupMembersModel: gmember,
		GroupSettingsModel: gsettings,
		GroupAppliesModel: gapplies,
	}
}
