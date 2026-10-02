package svc

import (
	"context"

	"im-platform/app/file/rpc/internal/config"
	"im-platform/app/file/rpc/models"
	"im-platform/common/utils"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	_ "github.com/lib/pq"
	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

type ServiceContext struct {
	Config     config.Config
	FilesModel models.FilesModel
	Snowflake  *utils.Snowflake
	S3         *s3.Client
	Presign    *s3.PresignClient
	Bucket     string
}

func NewServiceContext(c config.Config) *ServiceContext {
	sqlconn := sqlx.NewSqlConn("postgres", c.Postgres.DataSource)
	snowflake, err := utils.NewSnowflakeOrAuto(c.Snowflake.WorkNode)
	if err != nil {
		panic(err)
	}

	// S3 客户端指向 MinIO（S3 兼容 API）：path-style 寻址 + 静态凭证
	// Region 是 SigV4 签名必填项，MinIO 不校验具体值，固定 us-east-1
	awsCfg := aws.Config{
		Region:      "us-east-1",
		Credentials: credentials.NewStaticCredentialsProvider(c.Minio.AccessKey, c.Minio.SecretKey, ""),
	}
	s3Client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(c.Minio.Endpoint)
		o.UsePathStyle = true
	})

	// 桶不存在则创建（私有读写，全部走预签名 URL 访问）
	ctx := context.Background()
	if _, err := s3Client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(c.Minio.Bucket)}); err != nil {
		if _, cerr := s3Client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(c.Minio.Bucket)}); cerr != nil {
			panic(cerr)
		}
	}

	return &ServiceContext{
		Config:     c,
		FilesModel: models.NewFilesModel(sqlconn),
		Snowflake:  snowflake,
		S3:         s3Client,
		Presign:    s3.NewPresignClient(s3Client),
		Bucket:     c.Minio.Bucket,
	}
}
