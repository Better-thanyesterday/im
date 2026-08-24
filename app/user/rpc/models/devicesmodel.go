package models

import (
	"github.com/zeromicro/go-zero/core/stores/cache"
	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

var _ DevicesModel = (*customDevicesModel)(nil)

type (
	// DevicesModel is an interface to be customized, add more methods here,
	// and implement the added methods in customDevicesModel.
	DevicesModel interface {
		devicesModel
	}

	customDevicesModel struct {
		*defaultDevicesModel
	}
)

// NewDevicesModel returns a model for the database table.
func NewDevicesModel(conn sqlx.SqlConn, c cache.CacheConf, opts ...cache.Option) DevicesModel {
	return &customDevicesModel{
		defaultDevicesModel: newDevicesModel(conn, c, opts...),
	}
}
