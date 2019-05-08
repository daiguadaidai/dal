package models

type Cluster struct {
	DefaultModel
	Name       string `gorm:"column:name" json:"name"`
	ListenHost string `gorm:"column:listen_host" json:"listen_host"`
	ListenPort int    `gorm:"column:listen_port" json:"listen_port"`
	Username   string `gorm:"column:username" json:"username"`
	Password   string `gorm:"column:password" json:"password"`
	DBName     string `gorm:"column:db_name" json:"db_name"`
}

func (Cluster) TableName() string {
	return "clusters"
}
