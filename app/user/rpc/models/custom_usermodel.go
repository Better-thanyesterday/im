package models

import (
	"context"
)

// InsertWithoutAccount 插入用户并返回数据库生成的 account
func (m *defaultUsersModel) InsertWithoutAccount(ctx context.Context, data *Users) (int64, error) {
	var account int64

	query := `INSERT INTO users 
		(id, email, birthday, region, gender, signature, avatar, nickname, password_hash, phone, status, created_at, updated_at) 
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, NOW(), NOW()) 
		RETURNING account`
	err := m.QueryRowNoCacheCtx(ctx, &account, query,
				data.Id,
				data.Email,
				data.Birthday,
				data.Region,
				data.Gender,
				data.Signature,
				data.Avatar,
				data.Nickname,
				data.PasswordHash,
				data.Phone,
				data.Status,
			)
	if err != nil {
		return 0, err
	}

	data.Account = account
	return account, nil
}