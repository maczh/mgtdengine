package mgtdengine

import (
	"fmt"
	"github.com/knadh/koanf"
	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/rawbytes"
	"github.com/levigross/grequests"
	"github.com/sadlil/gologger"
	"strings"
)

type mgtdengine struct {
	tdDsns   map[string]string
	tdDbName map[string]string
	multitd  bool
	debug    bool
	poolCfg  connectionPoolConfig
	conf     *koanf.Koanf
	confUrl  string
	tdPools  map[string]*mgTdConnectionPool
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
	t.debug = t.conf.Bool("go.data.tdengine.debug")
	t.poolCfg = connectionPoolConfig{
		MaxConns:        t.conf.Int("go.data.tdengine.pool.max"),
		MinConns:        t.conf.Int("go.data.tdengine.pool.min"),
		MaxIdelTimeout:  t.conf.Int("go.data.tdengine.pool.idle"),
		MaxConnLifetime: t.conf.Int("go.data.tdengine.pool.timeout"),
	}
	if t.poolCfg.MinConns == 0 {
		t.poolCfg.MinConns = 10
	}
	if t.poolCfg.MaxConns < t.poolCfg.MinConns {
		t.poolCfg.MaxConns = 5 * t.poolCfg.MinConns
	}
	if t.poolCfg.MaxIdelTimeout == 0 {
		t.poolCfg.MaxIdelTimeout = 60
	}
	if t.poolCfg.MaxConnLifetime < t.poolCfg.MaxIdelTimeout {
		t.poolCfg.MaxConnLifetime = 5 * t.poolCfg.MaxIdelTimeout
	}
	t.tdDsns = make(map[string]string)
	t.tdDbName = make(map[string]string)
	t.tdPools = make(map[string]*mgTdConnectionPool)
	if t.multitd {
		dbNames := strings.Split(t.conf.String("go.data.tdengine.dbNames"), ",")
		for _, dbName := range dbNames {
			if dbName == "" || t.conf.String(fmt.Sprintf("go.data.tdengine.%s.dsn", dbName)) == "" {
				logger.Error(dbName + " TDengine dsn 未配置")
				continue
			}
			dsn := t.conf.String(fmt.Sprintf("go.data.tdengine.%s.dsn", dbName))
			t.tdDsns[dbName] = dsn
			t.tdDbName[dbName] = dsn[strings.LastIndex(dsn, "/")+1:]
			t.tdPools[dbName] = poolInit(dsn, &t.poolCfg)
		}
	} else {
		dsn := t.conf.String("go.data.tdengine.dsn")
		t.tdDsns["0"] = dsn
		t.tdDbName["0"] = t.conf.String("go.data.tdengine.db")
		if t.tdDbName["0"] == "" {
			t.tdDbName["0"] = dsn[strings.LastIndex(dsn, "/")+1:]
		}
		t.tdPools["0"] = poolInit(dsn, &t.poolCfg)
	}
}

func (t *mgtdengine) connect(dbName ...string) (*mgTdConnection, error) {
	dbn := "0"
	if len(dbName) > 0 {
		dbn = dbName[0]
	}
	return t.tdPools[dbn].GetConnection()
}

func (t *mgtdengine) GetConnection(dbName ...string) (*mgTdConnection, error) {
	return t.connect(dbName...)
}

func (t *mgtdengine) Check() error {
	for _, pool := range t.tdPools {
		needIdle := pool.poolConfig.MinConns - len(pool.usingConns)
		if needIdle < 0 {
			needIdle = 0
		}
		for i, _ := range pool.freeConns {
			if needIdle > 0 {
				pool.release(i)
			} else {
				pool.connections[i] = pool.connections[i].reconnect()
			}
			needIdle--
		}
	}
	return nil
}

func (t *mgtdengine) Close() {
	for _, pool := range t.tdPools {
		for i := 0; i < len(pool.connections); i++ {
			pool.release(i)
		}
	}
}
