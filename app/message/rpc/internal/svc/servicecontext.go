package svc

import (
	"im-platform/app/file/rpc/fileclient"
	"im-platform/app/group/rpc/groupclient"
	"im-platform/app/message/rpc/internal/config"
	"im-platform/app/message/rpc/models"
	pushclient "im-platform/app/push/rpc/pushclient"
	userclient "im-platform/app/user/rpc/userclient"
	"im-platform/common/mq"
	"im-platform/common/utils"

	_ "github.com/lib/pq"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/core/stores/sqlx"
	"github.com/zeromicro/go-zero/zrpc"
)

type ServiceContext struct {
	Config        config.Config
	MessagesModel models.MessagesModel
	DedupModel    *models.DedupModel
	InboxesModel  models.InboxesModel
	SeqModel      models.SeqsModel
	Redis         *redis.Redis
	Snowflake     *utils.Snowflake
	KafkaProducer *mq.Producer
	KafkaConsumer *mq.Consumer
	SeqIdCache    *utils.SeqIdCache
	pushclient.Push
	groupclient.Group
	userclient.User
	fileclient.File
}

func NewServiceContext(c config.Config) *ServiceContext {
	sqlconn := sqlx.NewSqlConn("postgres", c.Postgres.DataSource)
	rds := redis.MustNewRedis(c.RedisCache)
	m := models.NewMessagesModel(sqlconn)
	s := models.NewSeqsModel(sqlconn)
	i := models.NewInboxesModel(sqlconn)
	snowflake, err := utils.NewSnowflakeOrAuto(c.Snowflake.WorkNode)
	if err != nil {
		panic(err)
	}
	// Kafka 生产者（只初始化 Producer，Consumer 在 main 里启动）
	saramaCfg, err := mq.BuildSaramaConfig(c.Kafka)
	if err != nil {
		logx.Errorf("build sarama config failed: %v", err)
		panic(err)
	}
	producer, err := mq.NewProducer(c.Kafka.Brokers, saramaCfg)
	if err != nil {
		logx.Errorf("new kafka producer failed: %v", err)
		panic(err)
	}

	consumers, err := mq.NewConsumer(c.Kafka.Brokers, c.Kafka.Consumer.GroupID, saramaCfg)
	if err != nil {
		logx.Errorf("new kafka consumer failed: %v", err)
		panic(err)
	}
	// msg/seq/inbox 已合并为单 topic 单消费组,配置里只应有一个 GroupID
	if len(consumers) == 0 {
		panic("kafka consumer group id is empty")
	}
	return &ServiceContext{
		Config:        c,
		MessagesModel: m,
		DedupModel:    models.NewDedupModel(rds),
		InboxesModel:  i,
		SeqModel:      s,
		Redis:         rds,
		Snowflake:     snowflake,
		KafkaProducer: producer,
		KafkaConsumer: consumers[0],
		Push:          pushclient.NewPush(zrpc.MustNewClient(c.PushRpc)),
		Group:         groupclient.NewGroup(zrpc.MustNewClient(c.GroupRpc)),
		User:          userclient.NewUser(zrpc.MustNewClient(c.UserRpc)),
		File:          fileclient.NewFile(zrpc.MustNewClient(c.FileRpc)),
		SeqIdCache:    utils.NewSeqIdCache(),
	}
}
