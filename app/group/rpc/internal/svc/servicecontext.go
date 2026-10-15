package svc

import (
	_ "github.com/lib/pq"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/stores/sqlx"
	"im-platform/app/group/rpc/internal/config"
	"im-platform/app/group/rpc/models"
	"im-platform/common/mq"
	"im-platform/common/utils"
)

type ServiceContext struct {
	Config             config.Config
	GroupsModel        models.GroupsModel
	GroupMembersModel  models.GroupmembersModel
	GroupSettingsModel models.GroupsettingsModel
	GroupAppliesModel  models.GroupappliesModel
	Snowflake          *utils.Snowflake
	KafkaProducer      *mq.Producer // 群变更事件广播(初始化失败只降级)
}

func NewServiceContext(c config.Config) *ServiceContext {
	sqlconn := sqlx.NewSqlConn("postgres", c.Postgres.DataSource)
	group := models.NewGroupsModel(sqlconn)
	gmember := models.NewGroupmembersModel(sqlconn)
	gsettings := models.NewGroupsettingsModel(sqlconn)
	gapplies := models.NewGroupappliesModel(sqlconn)
	snowflake, err := utils.NewSnowflakeOrAuto(c.Snowflake.WorkNode)
	if err != nil {
		panic(err)
	}
	var producer *mq.Producer
	if saramaCfg, err := mq.BuildSaramaConfig(c.Kafka); err == nil {
		if p, perr := mq.NewProducer(c.Kafka.Brokers, saramaCfg); perr == nil {
			producer = p
		} else {
			logx.Errorf("kafka producer 初始化失败,群变更事件不可用 | err=%v", perr)
		}
	} else {
		logx.Errorf("kafka 配置构建失败,群变更事件不可用 | err=%v", err)
	}
	return &ServiceContext{
		Config:             c,
		GroupsModel:        group,
		Snowflake:          snowflake,
		GroupMembersModel:  gmember,
		GroupSettingsModel: gsettings,
		GroupAppliesModel:  gapplies,
		KafkaProducer:      producer,
	}
}
