package mgtdengine

import (
	"github.com/maczh/tdengine"
	"time"
)

// tdengine连接对象
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
