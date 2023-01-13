package mgtdengine

import (
	"fmt"
	"github.com/knadh/koanf"
	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/rawbytes"
	"github.com/levigross/grequests"
	"github.com/maczh/tdengine"
	"github.com/sadlil/gologger"
	"math/rand"
	"strings"
	"time"
)

type mgtdengine struct {
	tdDsns   map[string]string
	tdDbName map[string]string
	multitd  bool
	debug    bool
	poolCfg  tdengine.Config
	conf     *koanf.Koanf
	confUrl  string
	tdPools  map[string]*mgTdConnectionPool
}

type mgTdConnection struct {
	dsn        string              //连接串
	connection *tdengine.TDengine  //连接
	connTime   time.Time           //建立连接时间
	returnTime time.Time           //归还时间
	connected  bool                //是否连接的状态
	index      int                 //连接池数组下标
	pool       *mgTdConnectionPool //所属连接池
}

func newConnect(dsn string) (*mgTdConnection, error) {
	td, err := tdengine.New(dsn)
	if err != nil {
		logger.Error("TDengine connection error: " + err.Error())
		return nil, err
	}
	return &mgTdConnection{
		connection: td,
		dsn:        dsn,
		connTime:   time.Now(),
		returnTime: time.Now(),
		connected:  true,
	}, nil
}

func (c *mgTdConnection) reconnect() *mgTdConnection {
	c.connection.Close()
	td, err := tdengine.New(c.dsn)
	if err != nil {
		logger.Error("TDengine connection error: " + err.Error())
		c.connected = false
		c.connection = nil
		return nil
	}
	c.connection = td
	c.connected = true
	c.connTime = time.Now()
	c.returnTime = time.Now()
	return c
}

func (c *mgTdConnection) TDengine() *tdengine.TDengine {
	return c.connection
}

func (c *mgTdConnection) release() {
	c.connection.Close()
}

func (c *mgTdConnection) Close() {
	c.returnTime = time.Now()
	if c.pool != nil {
		c.pool.freeConns[c.index] = 1
		delete(c.pool.usingConns, c.index)
	}
}

type mgTdConnectionPool struct {
	connections []*mgTdConnection
	usingConns  map[int]int
	freeConns   map[int]int
	poolConfig  *tdengine.Config
	dsn         string
}

func PoolInit(dsn string, poolConfig *tdengine.Config) *mgTdConnectionPool {
	pool := mgTdConnectionPool{
		poolConfig: poolConfig,
	}

	pool.connections = make([]*mgTdConnection, poolConfig.MaxIdelConns) //最大连接数
	pool.freeConns = make(map[int]int)
	pool.usingConns = make(map[int]int)
	var err error
	for i := 0; i < poolConfig.MaxOpenConns; i++ { //初始化按最小连接数初始
		pool.connections[i], err = newConnect(dsn)
		if err != nil {
			logger.Error(fmt.Sprintf("初始化连接%d失败:%s", i, err.Error()))
			continue
		}
		pool.freeConns[i] = 1
		pool.connections[i].index = i
		pool.connections[i].pool = &pool
	}
	return &pool
}

func (p *mgTdConnectionPool) GetConnection() (*mgTdConnection, error) {
	if len(p.freeConns) > 0 {
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		in := r.Intn(len(p.freeConns))
		idx := 0
		for i, _ := range p.freeConns {
			if in == 0 {
				idx = i
				break
			}
			in--
		}
		p.usingConns[idx] = 1
		delete(p.freeConns, idx)
		//如果已经超时
		if p.connections[idx].returnTime.Before(time.Now().Add(-1*time.Duration(p.poolConfig.MaxIdelTimeout)*time.Second)) || p.connections[idx].connTime.Before(time.Now().Add(-1*time.Duration(p.poolConfig.MaxConnLifetime)*time.Second)) {
			p.connections[idx] = p.connections[idx].reconnect()
		}
		return p.connections[idx], nil
	} else {
		if len(p.usingConns) < p.poolConfig.MaxIdelConns {
			conn, err := newConnect(p.dsn)
			if err != nil {
				logger.Error("获取tdengine连接失败:" + err.Error())
				return nil, err
			}
			conn.pool = p
			for i, _ := range p.connections {
				if p.connections[i] == nil {
					p.connections[i] = conn
					p.usingConns[i] = 1
					p.connections[i].index = i
					return conn, nil
				}
			}
			return conn, nil
		} else {
			logger.Error("连接池已满，请加大连接池配置")
			return nil, fmt.Errorf("TDengine connection pool is full.")
		}

	}
}

func (p *mgTdConnectionPool) release(idx int) {
	if _, ok := p.usingConns[idx]; ok {
		return
	}
	if idx > p.poolConfig.MaxIdelConns-1 {
		return
	}
	if p.connections[idx] == nil {
		return
	}
	p.connections[idx].release()
	delete(p.freeConns, idx)
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
			t.tdPools[dbName] = PoolInit(dsn, &t.poolCfg)
		}
	} else {
		dsn := t.conf.String("go.data.tdengine.dsn")
		t.tdDsns["0"] = dsn
		t.tdDbName["0"] = t.conf.String("go.data.tdengine.db")
		if t.tdDbName["0"] == "" {
			t.tdDbName["0"] = dsn[strings.LastIndex(dsn, "/")+1:]
		}
		t.tdPools["0"] = PoolInit(dsn, &t.poolCfg)
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
		needIdle := pool.poolConfig.MaxOpenConns - len(pool.usingConns)
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
