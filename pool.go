package mgtdengine

import (
	"fmt"
	"sync"
	"time"
)

// 连接池对象
type mgTdConnectionPool struct {
	pool       *sync.Pool //连接池，连接数组
	poolConfig *connectionPoolConfig
	dsn        string
}

func poolInit(dsn string, poolConfig *connectionPoolConfig) *mgTdConnectionPool {
	tdPool := mgTdConnectionPool{
		poolConfig: poolConfig,
		dsn:        dsn,
		pool: &sync.Pool{
			New: func() any {
				return newConnect()
			},
		},
	}
	return &tdPool
}

func (p *mgTdConnectionPool) GetConnection() (*mgTdConnection, error) {
	for {
		conn := p.pool.Get().(*mgTdConnection)
		if !conn.connected {
			logger.Debug("建立新连接")
			conn = conn.New(p.dsn, p)
			if !conn.connected || conn.connection == nil {
				return nil, fmt.Errorf("TDengine get connect fail")
			} else {
				return conn, nil
			}
		} else {
			if conn.returnTime.Add(time.Duration(p.poolConfig.MaxIdelTimeout)*time.Second).Before(time.Now()) || conn.connTime.Add(time.Duration(p.poolConfig.MaxConnLifetime)*time.Second).Before(time.Now()) {
				conn.connection.Close()
				logger.Debug("抛弃超时连接")
				continue
			} else {
				logger.Debug("复用连接池中的连接")
				return conn, nil
			}
		}
	}
}

func (p *mgTdConnectionPool) close() {
	for {
		conn := p.pool.Get().(*mgTdConnection)
		if conn.connected {
			conn.connection.Close()
		} else {
			return
		}
	}
}
