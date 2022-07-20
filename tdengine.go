package mgtdengine

import (
	"errors"
	"github.com/knadh/koanf"
	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/rawbytes"
	"github.com/levigross/grequests"
	"github.com/maczh/mgconfig"
	"github.com/maczh/tdengine"
	"github.com/nacos-group/nacos-sdk-go/common/logger"
	"strings"
	"time"
)

var taos *tdengine.TDengine

func getConfigUrl(prefix string) string {
	serverType := mgconfig.GetConfigString("go.config.server_type")
	configUrl := mgconfig.GetConfigString("go.config.server")
	switch serverType {
	case "nacos":
		configUrl = configUrl + "nacos/v1/cs/configs?group=DEFAULT_GROUP&dataId=" + prefix + mgconfig.GetConfigString("go.config.mid") + mgconfig.GetConfigString("go.config.env") + mgconfig.GetConfigString("go.config.type")
	case "consul":
		configUrl = configUrl + "v1/kv/" + prefix + mgconfig.GetConfigString("go.config.mid") + mgconfig.GetConfigString("go.config.env") + mgconfig.GetConfigString("go.config.type") + "?dc=dc1&raw=true"
	case "springconfig":
		configUrl = configUrl + prefix + mgconfig.GetConfigString("go.config.mid") + mgconfig.GetConfigString("go.config.env") + mgconfig.GetConfigString("go.config.type")
	default:
		configUrl = configUrl + prefix + mgconfig.GetConfigString("go.config.mid") + mgconfig.GetConfigString("go.config.env") + mgconfig.GetConfigString("go.config.type")
	}
	return configUrl
}

func TDengineInit() {
	if !strings.Contains(mgconfig.GetConfigString("go.config.used"),"tdengine") {
		logger.Error("未配置使用tdengine数据库")
		return
	}
	if taos == nil {
		tdengineConfigUrl := getConfigUrl(mgconfig.GetConfigString("go.config.prefix.mysql"))
		logger.Debug("正在获取TDengine配置: " + tdengineConfigUrl)
		resp, err := grequests.Get(tdengineConfigUrl, nil)
		if err != nil {
			logger.Error("TDengine配置下载失败! " + err.Error())
			return
		}
		cfg := koanf.New(".")
		cfg.Load(rawbytes.Provider([]byte(resp.String())), yaml.Parser())
		taos, _ = tdengine.New(cfg.String("go.data.tdengine.dsn"))
		poolCfg := tdengine.Config{
			MaxIdelConns:    cfg.Int("go.data.tdengine.pool.max"),
			MaxOpenConns:    cfg.Int("go.data.tdengine.pool.min"),
			MaxIdelTimeout:  cfg.Int("go.data.tdengine.pool.idle"),
			MaxConnLifetime: cfg.Int("go.data.tdengine.pool.timeout"),
		}
		if poolCfg.MaxOpenConns == 0 {
			poolCfg.MaxIdelConns = 10
		}
		if poolCfg.MaxIdelConns < poolCfg.MaxOpenConns {
			poolCfg.MaxIdelConns = 5 * poolCfg.MaxOpenConns
		}
		if poolCfg.MaxIdelTimeout == 0 {
			poolCfg.MaxIdelTimeout = 60
		}
		if poolCfg.MaxConnLifetime < poolCfg.MaxIdelTimeout {
			poolCfg.MaxConnLifetime = 5 * poolCfg.MaxIdelTimeout
		}
		taos.ConnPool(poolCfg)
		if cfg.Bool("go.data.tdengine.debug") {
			taos = taos.SetDebug()
		}
	}
	//设置定时任务自动检查
	ticker := time.NewTicker(time.Minute * 5)
	go func() {
		var err error
		for _ = range ticker.C {
			logger.Debug("正在检测TDengine连接")
			taos,err = TDengineCheck()
			if err != nil {
				logger.Error("发现TDengine连接丢失，正在重新连接")
			}
		}
	}()
}

func TDengineClose() {
	err := taos.Close()
	if err != nil {
		logger.Error("TDengine close error: " + err.Error())
	}
	taos = nil
}

func TDengineCheck() (*tdengine.TDengine, error) {
	if taos == nil {
		TDengineInit()
		if taos == nil {
			return taos, errors.New("taos connection error")
		}
	}
	err := taos.Ping()
	if err != nil {
		TDengineClose()
		TDengineInit()
		if taos == nil {
			return taos, errors.New("taos connection error")
		}
	}
	return taos, nil
}


func GetTDengineConnection() (*tdengine.TDengine, error) {
	return TDengineCheck()
}
