package models

import "github.com/zeromicro/go-zero/core/stores/sqlx"

var _ DevicesModel = (*customDevicesModel)(nil)

type (
	// DevicesModel is an interface to be customized, add more methods here,
	// and implement the added methods in customDevicesModel.
	DevicesModel interface {
		devicesModel
		withSession(session sqlx.Session) DevicesModel
	}

	customDevicesModel struct {
		*defaultDevicesModel
	}
)

// NewDevicesModel returns a model for the database table.
func NewDevicesModel(conn sqlx.SqlConn) DevicesModel {
	return &customDevicesModel{
		defaultDevicesModel: newDevicesModel(conn),
	}
}

func (m *customDevicesModel) withSession(session sqlx.Session) DevicesModel {
	return NewDevicesModel(sqlx.NewSqlConnFromSession(session))
}
