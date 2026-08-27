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
	Snokflake     *utils.Snowflake
}

func NewServiceContext(c config.Config) *ServiceContext {
	sqlconn:=sqlx.NewSqlConn("postgres",c.Postgres.DataSource)
	group:=models.NewGroupsModel(sqlconn)
	gmember:=models.NewGroupmembersModel(sqlconn)
	gsettings:=models.NewGroupsettingsModel(sqlconn)
	gapplies:=models.NewGroupappliesModel(sqlconn)
	snokflake,_:=utils.NewSnowflake(c.SnokFlake.WorkNode)
	return &ServiceContext{
		Config: c,
		GroupsModel: group,
		Snokflake: snokflake,
		GroupMembersModel: gmember,
		GroupSettingsModel: gsettings,
		GroupAppliesModel: gapplies,
	}
}
