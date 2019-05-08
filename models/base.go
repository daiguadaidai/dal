package models

import (
	"github.com/daiguadaidai/dal/utils/types"
)

type DefaultModel struct {
	ID        int64      `json:"id" sql:"type:int unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY"`
	CreatedAt types.Time `json:"created_at" sql:"default:CURRENT_TIMESTAMP"`
	UpdatedAt types.Time `json:"updated_at" sql:"default:CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"`
}
