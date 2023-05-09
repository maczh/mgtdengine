package mgtdengine

import (
	"github.com/maczh/tdengine/v3"
	"time"
)

// tdengine连接对象
type mgTdConnection struct {
	connection *tdengine.TDengine  //连接
	connTime   time.Time           //建立连接时间
	returnTime time.Time           //归还时间
	connected  bool                //是否连接的状态
	pool       *mgTdConnectionPool //所属连接池
}

func newConnect() *mgTdConnection {
	conn := new(mgTdConnection)
	conn.connTime = time.Now()
	conn.returnTime = time.Now()
	conn.connected = false
	return conn
}

func (c *mgTdConnection) New(dsn string, pool *mgTdConnectionPool) *mgTdConnection {
	td, err := tdengine.New(dsn)
	if err != nil {
		logger.Error("TDengine connection error: " + err.Error())
		return c
	}
	c.connection = td
	c.connected = true
	c.pool = pool
	return c
}

func (c *mgTdConnection) TDengine() *tdengine.TDengine {
	return c.connection
}

func (c *mgTdConnection) Close() {
	logger.Debug("归还TDengine连接")
	c.returnTime = time.Now()
	c.pool.pool.Put(c)
}
