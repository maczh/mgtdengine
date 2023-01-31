package mgtdengine

import (
	"fmt"
	"sync"
	"time"
)

// 连接池对象
type mgTdConnectionPool struct {
	pool       sync.Pool //连接池，连接数组
	poolConfig *connectionPoolConfig
	used       int //正在使用的连接数
	dsn        string
}

func poolInit(dsn string, poolConfig *connectionPoolConfig) *mgTdConnectionPool {
	tdPool := mgTdConnectionPool{
		poolConfig: poolConfig,
		dsn:        dsn,
		pool: sync.Pool{
			New: func() any {
				return newConnect()
			},
		},
	}
	return &tdPool
}

func (p *mgTdConnectionPool) GetConnection() (*mgTdConnection, error) {
	for {
		logger.Debug(fmt.Sprintf("当前连接数: %d", p.used))
		if p.used >= p.poolConfig.MaxConns {
			return nil, fmt.Errorf("TDengine connection pool is full")
		}
		conn := p.pool.Get().(*mgTdConnection)
		if !conn.connected {
			logger.Debug("获取到的是新连接，建立连接")
			conn = conn.New(p.dsn, p)
			if !conn.connected || conn.connection == nil {
				return nil, fmt.Errorf("TDengine get connect fail")
			} else {
				p.used++
				return conn, nil
			}
		} else {
			if conn.returnTime.Add(time.Duration(p.poolConfig.MaxIdelTimeout)*time.Second).Before(time.Now()) || conn.connTime.Add(time.Duration(p.poolConfig.MaxConnLifetime)*time.Second).Before(time.Now()) {
				conn.connection.Close()
				logger.Debug("发现超时连接，扔掉重取其他连接")
				p.used--
				continue
			} else {
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
		}
	}
}
