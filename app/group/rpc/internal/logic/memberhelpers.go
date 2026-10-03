package logic

import (
	"context"
	"errors"
	"fmt"

	"im-platform/app/group/rpc/group"
	"im-platform/app/group/rpc/internal/svc"
	"im-platform/app/group/rpc/models"

	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

// 成员操作共享的取数/权限助手:9 个成员管理 logic 的公共前置

// fetchGroup 取群并校验未解散
func fetchGroup(ctx context.Context, svcCtx *svc.ServiceContext, groupId int64) (*models.Groups, error) {
	g, err := svcCtx.GroupsModel.FindOne(ctx, groupId)
	if err != nil {
		return nil, err
	}
	if g.Status == int64(group.GroupStatus_DISSOLVED) {
		return nil, errors.New("群已解散")
	}
	return g, nil
}

// fetchMember 取成员行,不存在时返回可读错误
func fetchMember(ctx context.Context, svcCtx *svc.ServiceContext, groupId, userId int64) (*models.Groupmembers, error) {
	m, err := svcCtx.GroupMembersModel.FindByGroupAndUser(ctx, groupId, userId)
	if err != nil {
		if errors.Is(err, sqlx.ErrNotFound) {
			return nil, fmt.Errorf("用户 %d 不是群 %d 的成员", userId, groupId)
		}
		return nil, err
	}
	return m, nil
}

// requirePrivilege 要求操作者是群主或管理员
func requirePrivilege(member *models.Groupmembers, action string) error {
	if member.Role != int64(group.GroupRole_OWNER) && member.Role != int64(group.GroupRole_ADMIN) {
		return fmt.Errorf("只有群主或管理员可以%s", action)
	}
	return nil
}

// requireOwner 要求操作者是群主
func requireOwner(member *models.Groupmembers) error {
	if member.Role != int64(group.GroupRole_OWNER) {
		return errors.New("只有群主可以执行此操作")
	}
	return nil
}
