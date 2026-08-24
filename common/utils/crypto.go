package utils

import(
	"fmt"
	"golang.org/x/crypto/bcrypt"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
)
// ==================== 密码工具 ====================

func HashPassword(password string) (string, error) {
	hashedPassword, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err !=nil{
		return "", fmt.Errorf("hash password failed %w" ,err)
	}
	return 	string (hashedPassword) ,nil
}

func VerifyPassword(password, hash string) bool {
	err := bcrypt.CompareHashAndPassword([]byte(hash), []byte(password))
	return err == nil
}


// ==================== Token工具 ====================

// genRandomToken 生成22字符的随机Token
// 16字节随机数 → base64.RawURLEncoding → 22字符（URL安全，无需转义）
func genRandomToken() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

// TokenToJson TokenInfo序列化为JSON（用于日志/调试）
func (t *TokenInfo) ToJson() string {
	b, _ := json.Marshal(t)
	return string(b)
}