package logic

import (
	"database/sql"
)

// groupNicknameOf 入群时的默认群昵称 = 群名
func groupNicknameOf(groupName string) sql.NullString {
	return sql.NullString{String: groupName, Valid: groupName != ""}
}

// sqlNullString 空串映射为 NULL
func sqlNullString(s string) sql.NullString {
	return sql.NullString{String: s, Valid: s != ""}
}
