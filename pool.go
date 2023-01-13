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
		dsn:        dsn,
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
	//定时检查
	pool.autoCheck()
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
		if p.connections[idx].returnTime.Add(time.Duration(p.poolConfig.MaxIdelTimeout)*time.Second).Before(time.Now()) || p.connections[idx].connTime.Add(time.Duration(p.poolConfig.MaxConnLifetime)*time.Second).Before(time.Now()) {
			logger.Debug(fmt.Sprintf("第%d个空闲连接超时，重新连接", idx))
			p.connections[idx] = p.connections[idx].reconnect()
		}
		if p.connections[idx] != nil {
			p.usingConns[idx] = 1
			delete(p.freeConns, idx)
			logger.Debug(fmt.Sprintf("获取到第%d个空闲连接", idx))
			return p.connections[idx], nil
		} else {
			logger.Debug(fmt.Sprintf("第%d个空闲连接为空", idx))
			return nil, fmt.Errorf("TDengine get connection failed")
		}
	} else {
		if len(p.usingConns) < p.poolConfig.MaxConns {
			logger.Debug(fmt.Sprintf("空闲连接不够，现有%d个占用连接，准备新建连接", len(p.usingConns)))
			conn, err := newConnect(p.dsn)
			if err != nil {
				logger.Error("获取tdengine连接失败:" + err.Error())
				return nil, err
			}
			conn.pool = p
			conn.usingTime = time.Now()
			for i, _ := range p.connections {
				if p.connections[i] == nil {
					logger.Debug(fmt.Sprintf("找到第%d个未使用的数组下标，新连接放到该下标", i))
					p.connections[i] = conn
					p.usingConns[i] = 1
					p.connections[i].index = i
					break
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
	p.connections[idx] = nil
	delete(p.freeConns, idx)
}

func (p *mgTdConnectionPool) check() {
	logger.Debug("启动连接池自检处理")
	for i, conn := range p.connections {
		if conn == nil {
			continue
		} else {
			if _, ok := p.freeConns[i]; ok {
				if conn.returnTime.Add(time.Duration(p.poolConfig.MaxIdelTimeout) * time.Second).Before(time.Now()) {
					logger.Debug(fmt.Sprintf("第%d个空闲连接归还时间超时，释放", i))
					p.release(i)
				}
				if conn.connTime.Add(time.Duration(p.poolConfig.MaxConnLifetime) * time.Second).Before(time.Now()) {
					logger.Debug(fmt.Sprintf("第%d个空闲连接生命周期超时，释放", i))
					p.release(i)
				}
			} else if _, ok := p.usingConns[i]; ok {
				if conn.usingTime.Add(time.Duration(p.poolConfig.QueryTimeout) * time.Second).Before(time.Now()) {
					logger.Debug(fmt.Sprintf("第%d个使用中连接查询时间超时，强制释放", i))
					p.release(i)
					delete(p.usingConns, i)
				}
			} else {
				logger.Debug(fmt.Sprintf("第%d个连接是野生连接，释放", i))
				p.release(i)
			}
		}
	}
}

func (p *mgTdConnectionPool) autoCheck() {
	//设置定时任务自动检查
	ticker := time.NewTicker(time.Minute * 1)
	go func(p *mgTdConnectionPool) {
		for range ticker.C {
			p.check()
		}
	}(p)
}

func (p *mgTdConnectionPool) close() {
	for i, conn := range p.connections {
		if conn != nil {
			p.release(i)
			delete(p.usingConns, i)
		}
	}
}
