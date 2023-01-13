package mgtdengine

import (
	"fmt"
	"math/rand"
	"time"
)

// 连接池对象
type mgTdConnectionPool struct {
	connections []*mgTdConnection //连接池，连接数组
	usingConns  map[int]int       //使用中连接下标集合
	freeConns   map[int]int       //空闲的连接下标集合
	poolConfig  *connectionPoolConfig
	dsn         string
}

func poolInit(dsn string, poolConfig *connectionPoolConfig) *mgTdConnectionPool {
	pool := mgTdConnectionPool{
		poolConfig: poolConfig,
	}

	pool.connections = make([]*mgTdConnection, poolConfig.MaxConns) //最大连接数
	pool.freeConns = make(map[int]int)
	pool.usingConns = make(map[int]int)
	var err error
	for i := 0; i < poolConfig.MinConns; i++ { //初始化按最小连接数初始
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
		//如果已经超时
		if p.connections[idx].returnTime.Before(time.Now().Add(-1*time.Duration(p.poolConfig.MaxIdelTimeout)*time.Second)) || p.connections[idx].connTime.Before(time.Now().Add(-1*time.Duration(p.poolConfig.MaxConnLifetime)*time.Second)) {
			p.connections[idx] = p.connections[idx].reconnect()
		}
		if p.connections[idx] != nil {
			p.usingConns[idx] = 1
			delete(p.freeConns, idx)
			return p.connections[idx], nil
		} else {
			return nil, fmt.Errorf("TDengine get connection failed")
		}
	} else {
		if len(p.usingConns) < p.poolConfig.MaxConns {
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
	if idx > p.poolConfig.MaxConns-1 {
		return
	}
	if p.connections[idx] == nil {
		return
	}
	p.connections[idx].release()
	delete(p.freeConns, idx)
}
