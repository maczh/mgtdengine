package mgtdengine

import (
	"errors"
	"fmt"
	"github.com/knadh/koanf"
	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/rawbytes"
	"github.com/levigross/grequests"
	"github.com/maczh/mgconfig"
	"github.com/maczh/tdengine"
	"github.com/sadlil/gologger"
	"strings"
	"time"
)

var taos *tdengine.TDengine
var tdClients = make(map[string]*tdengine.TDengine)
var tdDsns = make(map[string]string)
var multitd bool
var poolCfg tdengine.Config
var logger = gologger.GetLogger()

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
	if !strings.Contains(mgconfig.GetConfigString("go.config.used"), "tdengine") {
		logger.Error("未配置使用tdengine数据库")
		return
	}
	if taos == nil && len(tdClients) == 0 {
		tdengineConfigUrl := getConfigUrl(mgconfig.GetConfigString("go.config.prefix.tdengine"))
		logger.Debug("正在获取TDengine配置: " + tdengineConfigUrl)
		resp, err := grequests.Get(tdengineConfigUrl, nil)
		if err != nil {
			logger.Error("TDengine配置下载失败! " + err.Error())
			return
		}
		cfg := koanf.New(".")
		cfg.Load(rawbytes.Provider([]byte(resp.String())), yaml.Parser())
		multitd = cfg.Bool("go.data.tdengine.multidb")
		poolCfg = tdengine.Config{
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
		if multitd {
			dbNames := strings.Split(cfg.String("go.data.tdengine.dbNames"), ",")
			for _, dbName := range dbNames {
				if dbName == "" || cfg.String(fmt.Sprintf("go.data.tdengine.%s.dsn", dbName)) == "" {
					logger.Error(dbName + " TDengine dsn 未配置")
					continue
				}
				dsn := cfg.String(fmt.Sprintf("go.data.tdengine.%s.dsn", dbName))
				td, err := tdengine.New(dsn)
				if err != nil {
					logger.Error(dbName + " TDengine connection error: " + err.Error())
					continue
				}
				td.ConnPool(poolCfg)
				if cfg.Bool("go.data.tdengine.debug") {
					td = td.SetDebug()
				}
				databaseName := dsn[strings.LastIndex(dsn, "/")+1:]
				td = td.Database(databaseName)
				tdClients[dbName] = td
				tdDsns[dbName] = dsn
			}
		} else {
			dsn := cfg.String("go.data.tdengine.dsn")
			taos, err = tdengine.New(dsn)
			if err != nil {
				logger.Error("TDengine connection error: " + err.Error())
				return
			}
			taos.ConnPool(poolCfg)
			if cfg.Bool("go.data.tdengine.debug") {
				taos = taos.SetDebug()
			}
			databaseName := dsn[strings.LastIndex(dsn, "/")+1:]
			if databaseName == "" {
				databaseName = cfg.String("go.data.tdengine.db")
			}
			taos = taos.Database(databaseName)
		}
	}
	//设置定时任务自动检查
	ticker := time.NewTicker(time.Minute * 5)
	go func() {
		var err error
		for _ = range ticker.C {
			logger.Debug("正在检测TDengine连接")
			err = TDengineCheck()
			if err != nil {
				logger.Error("发现TDengine连接丢失，正在重新连接")
			}
		}
	}()
}

func TDengineClose() {
	if multitd {
		for dbName, _ := range tdClients {
			tdClients[dbName].Close()
			delete(tdClients, dbName)
		}
	} else {
		err := taos.Close()
		if err != nil {
			logger.Error("TDengine close error: " + err.Error())
		}
		taos = nil
	}
}

func tdengineCheck(dbName string) error {
	if tdClients[dbName] == nil || tdClients[dbName].Ping() != nil {
		td, err := tdengine.New(tdDsns[dbName])
		if err != nil {
			logger.Error(dbName + " TDengine connection error: " + err.Error())
			return err
		}
		td.ConnPool(poolCfg)
		databaseName := tdDsns[dbName][strings.LastIndex(tdDsns[dbName], "/")+1:]
		td = td.Database(databaseName)
		tdClients[dbName] = td
	}
	return nil
}

func TDengineCheck() error {
	if multitd {
		for dbName, _ := range tdClients {
			_ = tdengineCheck(dbName)
		}
	} else {
		if taos == nil {
			logger.Error("TDengine connection closed")
			TDengineInit()
			if taos == nil {
				return errors.New("taos connection error")
			}
		}
		err := taos.Ping()
		if err != nil {
			TDengineClose()
			TDengineInit()
			if taos == nil {
				return errors.New("taos connection error")
			}
		}
	}
	logger.Debug("TDengine connection check successful")
	return nil
}

func GetTDengineConnection(dbName ...string) (*tdengine.TDengine, error) {
	if multitd {
		if len(dbName) == 0 || len(dbName) > 1 {
			return nil, errors.New("Multidb get tdengine connection must be one databaseName")
		}
		err := tdengineCheck(dbName[0])
		return tdClients[dbName[0]], err
	} else {
		err := TDengineCheck()
		return taos, err
	}
}
