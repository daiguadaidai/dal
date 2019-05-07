package config

import "fmt"

const (
	LOG_LEVEL          = "info"                                    // trace, debug, info, warn, error, critical
	LOG_FORMAT         = "%Date %Time [%Level] %File:%Line %Msg%n" // 输出日志格式
	LOF_FILE_SIZE      = 1073741824                                // 日志文件大小1G
	LOG_FILE_KEEP_SIZE = 10                                        // 日志文件保留个数
)

type LogConfig struct {
	Path         string `toml:"path"`
	Level        string `toml:"level"`
	Format       string `json:"format"`         // 输入日志格式
	FileSize     int64  `json:"file_size"`      // 每个日志文件大小
	FileKeepSize int    `json:"file_keep_size"` // 文件保留个数
}

func (this *LogConfig) Raw() string {
	if len(this.Path) == 0 {
		return this.ToConsole()
	}
	return this.ToFile()
}

func (this *LogConfig) ToConsole() string {
	raw := `
        <seelog type="sync" minlevel="%s">
        	<outputs formatid="main">
                <console />
        	</outputs>
            <formats>
                <format id="main" format="%s"/>
            </formats>
        </seelog>
    `

	return fmt.Sprintf(raw, this.Level, this.Format)
}

func (this *LogConfig) ToFile() string {
	raw := `
        <seelog type="sync" minlevel="%s">
        	<outputs formatid="main">
				<rollingfile type="size" filename="%s" maxsize="%d" maxrolls="%d" />
        	</outputs>
            <formats>
                <format id="main" format="%s"/>
            </formats>
        </seelog>
    `
	return fmt.Sprintf(raw, this.Level, this.Path, this.FileSize, this.FileKeepSize, this.Format)
}

// 日志配置补充信息
func (this *LogConfig) SupDefault() {
	if len(this.Level) == 0 {
		this.Level = LOG_LEVEL
	}
	if len(this.Format) == 0 {
		this.Format = LOG_FORMAT
	}
	if this.FileSize < 1 {
		this.FileSize = LOF_FILE_SIZE
	}
	if this.FileKeepSize < 1 {
		this.FileKeepSize = LOG_FILE_KEEP_SIZE
	}
}
