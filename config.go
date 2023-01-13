package mgtdengine

type connectionPoolConfig struct {
	MaxConns        int //最大连接数
	MinConns        int //最小连接数
	MaxIdelTimeout  int //连接空闲时间，超过自动释放
	MaxConnLifetime int //连接生命周期，超过自动释放
	QueryTimeout    int //连接使用超时时间
}
