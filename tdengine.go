package mgtdengine

import (
	"errors"
	"fmt"
	"github.com/knadh/koanf"
	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/rawbytes"
	"github.com/levigross/grequests"
	"github.com/maczh/tdengine"
	"github.com/sadlil/gologger"
	"strings"
)

type mgtdengine struct {
	taos      *tdengine.TDengine
	tdClients map[string]*tdengine.TDengine
	tdDsns    map[string]string
	multitd   bool
	poolCfg   tdengine.Config
	conf      *koanf.Koanf
	confUrl   string
}

var TDengine = &mgtdengine{}
var logger = gologger.GetLogger()

func (t *mgtdengine) Init(tdengineConfigUrl string) {
	if tdengineConfigUrl != "" {
		t.confUrl = tdengineConfigUrl
	}
	if t.confUrl == "" {
		logger.Error("TDengine配置Url为空")
		return
	}
	var err error
	if t.taos == nil && len(t.tdClients) == 0 {
		if t.conf == nil {
			logger.Debug("正在获取TDengine配置: " + t.confUrl)
			resp, err := grequests.Get(t.confUrl, nil)
			if err != nil {
				logger.Error("TDengine配置下载失败! " + err.Error())
				return
			}
			t.conf = koanf.New(".")
			err = t.conf.Load(rawbytes.Provider([]byte(resp.String())), yaml.Parser())
			if err != nil {
				logger.Error("TDengine配置文件解析错误:" + err.Error())
				t.conf = nil
				return
			}
		}
		t.multitd = t.conf.Bool("go.data.tdengine.multidb")
		t.poolCfg = tdengine.Config{
			MaxIdelConns:    t.conf.Int("go.data.tdengine.pool.max"),
			MaxOpenConns:    t.conf.Int("go.data.tdengine.pool.min"),
			MaxIdelTimeout:  t.conf.Int("go.data.tdengine.pool.idle"),
			MaxConnLifetime: t.conf.Int("go.data.tdengine.pool.timeout"),
		}
		if t.poolCfg.MaxOpenConns == 0 {
			t.poolCfg.MaxIdelConns = 10
		}
		if t.poolCfg.MaxIdelConns < t.poolCfg.MaxOpenConns {
			t.poolCfg.MaxIdelConns = 5 * t.poolCfg.MaxOpenConns
		}
		if t.poolCfg.MaxIdelTimeout == 0 {
			t.poolCfg.MaxIdelTimeout = 60
		}
		if t.poolCfg.MaxConnLifetime < t.poolCfg.MaxIdelTimeout {
			t.poolCfg.MaxConnLifetime = 5 * t.poolCfg.MaxIdelTimeout
		}
		if t.multitd {
			t.tdClients = make(map[string]*tdengine.TDengine)
			t.tdDsns = make(map[string]string)
			dbNames := strings.Split(t.conf.String("go.data.tdengine.dbNames"), ",")
			for _, dbName := range dbNames {
				if dbName == "" || t.conf.String(fmt.Sprintf("go.data.tdengine.%s.dsn", dbName)) == "" {
					logger.Error(dbName + " TDengine dsn 未配置")
					continue
				}
				dsn := t.conf.String(fmt.Sprintf("go.data.tdengine.%s.dsn", dbName))
				td, err := tdengine.New(dsn)
				if err != nil {
					logger.Error(dbName + " TDengine connection error: " + err.Error())
					continue
				}
				td.ConnPool(t.poolCfg)
				if t.conf.Bool("go.data.tdengine.debug") {
					td = td.SetDebug()
				}
				databaseName := dsn[strings.LastIndex(dsn, "/")+1:]
				td = td.Database(databaseName)
				t.tdClients[dbName] = td
				t.tdDsns[dbName] = dsn
			}
		} else {
			dsn := t.conf.String("go.data.tdengine.dsn")
			t.taos, err = tdengine.New(dsn)
			if err != nil {
				logger.Error("TDengine connection error: " + err.Error())
				return
			}
			t.taos.ConnPool(t.poolCfg)
			if t.conf.Bool("go.data.tdengine.debug") {
				t.taos = t.taos.SetDebug()
			}
			databaseName := dsn[strings.LastIndex(dsn, "/")+1:]
			if databaseName == "" {
				databaseName = t.conf.String("go.data.tdengine.db")
			}
			t.taos = t.taos.Database(databaseName)
		}
	}
}

func (t *mgtdengine) Close() {
	if t.multitd {
		for dbName, _ := range t.tdClients {
			t.tdClients[dbName].Close()
			delete(t.tdClients, dbName)
		}
	} else {
		err := t.taos.Close()
		if err != nil {
			logger.Error("TDengine close error: " + err.Error())
		}
		t.taos = nil
	}
}

func (t *mgtdengine) check(dbName string) error {
	if t.tdClients[dbName] == nil || t.tdClients[dbName].Ping() != nil {
		td, err := tdengine.New(t.tdDsns[dbName])
		if err != nil {
			logger.Error(dbName + " TDengine connection error: " + err.Error())
			return err
		}
		td.ConnPool(t.poolCfg)
		databaseName := t.tdDsns[dbName][strings.LastIndex(t.tdDsns[dbName], "/")+1:]
		td = td.Database(databaseName)
		t.tdClients[dbName] = td
	} else {
		_, err := t.tdClients[dbName].DB.Exec("SHOW DATABASES;")
		if err != nil {
			td, err := tdengine.New(t.tdDsns[dbName])
			if err != nil {
				logger.Error(dbName + " TDengine connection error: " + err.Error())
				return err
			}
			td.ConnPool(t.poolCfg)
			databaseName := t.tdDsns[dbName][strings.LastIndex(t.tdDsns[dbName], "/")+1:]
			td = td.Database(databaseName)
			t.tdClients[dbName] = td
		}
	}
	return nil
}

func (t *mgtdengine) Check() {
	if t.multitd {
		for dbName, _ := range t.tdClients {
			_ = t.check(dbName)
		}
	} else {
		if t.taos == nil {
			logger.Error("TDengine connection closed")
			t.Init("")
			if t.taos == nil {
				logger.Error("TDengine reconnect failed")
				return
			}
		}
		err := t.taos.Ping()
		if err != nil {
			t.Close()
			t.Init("")
			if t.taos == nil {
				logger.Error("TDengine reconnect failed")
				return
			}
		} else {
			_, err := t.taos.DB.Exec("SHOW DATABASES;")
			if err != nil {
				t.Close()
				t.Init("")
			}
		}
	}
	logger.Debug("TDengine connection check successful")
	return
}

func (t *mgtdengine) GetConnection(dbName ...string) (*tdengine.TDengine, error) {
	if t.multitd {
		if len(dbName) == 0 || len(dbName) > 1 {
			return nil, errors.New("Multidb get tdengine connection must be one databaseName")
		}
		err := t.check(dbName[0])
		return t.tdClients[dbName[0]], err
	} else {
		t.Check()
		return t.taos, nil
	}
}
