package mgtdengine

import (
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
	tdDsns   map[string]string
	tdDbName map[string]string
	multitd  bool
	debug    bool
	poolCfg  tdengine.Config
	conf     *koanf.Koanf
	confUrl  string
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
	t.tdDsns = make(map[string]string)
	t.tdDbName = make(map[string]string)
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
		}
	} else {
		dsn := t.conf.String("go.data.tdengine.dsn")
		t.tdDsns["0"] = dsn
		t.tdDbName["0"] = t.conf.String("go.data.tdengine.db")
		if t.tdDbName["0"] == "" {
			t.tdDbName["0"] = dsn[strings.LastIndex(dsn, "/")+1:]
		}
	}
}

func (t *mgtdengine) connect(dbName ...string) (*tdengine.TDengine, error) {
	dsn := ""
	db := ""
	if len(dbName) == 0 {
		if t.multitd {
			return nil, fmt.Errorf("TDengine多库连接必须指定库名")
		}
		dsn = t.tdDsns["0"]
		db = t.tdDbName["0"]
	} else {
		if !t.multitd {
			return nil, fmt.Errorf("TDengine单库连接不允许指定库名")
		}
		dsn = t.tdDsns[dbName[0]]
		if dsn == "" {
			return nil, fmt.Errorf("TDengine名为%s的连接串不存在", dbName[0])
		}
		db = t.tdDbName[dbName[0]]
	}
	td, err := tdengine.New(dsn)
	if err != nil {
		logger.Error("TDengine connection error: " + err.Error())
		return nil, err
	}
	td.ConnPool(t.poolCfg)
	if t.debug {
		td = td.SetDebug()
	}
	return td.Database(db), nil
}

func (t *mgtdengine) GetConnection(dbName ...string) (*tdengine.TDengine, error) {
	return t.connect(dbName...)
}
