# MGin框架之tdengine插件

## 引入
```go
import "github.com/maczh/mgtdengine"
```

## 配置文件，存放于配置中心
```yaml
go:
  data:
    tdengine:
        dsn: user:password@tcp(tdengine-server:6030)/databasename
        debug: true
        pool:
            min: 2
            max: 20
            idle: 60
            timeout: 300
```

## 多库连接的配置文件范例
```yaml
go:
  data:
    tdengine:
      multidb: true
      dbNames: test1,test2
      test1:
        dsn: user1:password1@tcp(tdengine-server1:6030)/db1
      test2:
        dsn: user2:password2@tcp(tdengine-server2:6030)/db2
      debug: true
      pool:
          min: 2
          max: 20
          idle: 60
          timeout: 300
```


## 在应用主配置文件中的配置
```yaml
go:
  application:
    name: myapp         #应用名称,用于自动注册微服务时的服务名
    port: 8080          #端口号
    ip: xxx.xxx.xxx.xxx  #微服务注册时登记的本地IP，不配可自动获取，如需指定外网IP或Docker之外的IP时配置
  discovery: nacos                      #微服务的服务发现与注册中心类型 nacos,consul,默认是 nacos
  config:                               #统一配置服务器相关
    server: http://192.168.1.5:8848/    #配置服务器地址
    server_type: nacos                  #配置服务器类型 nacos,consul,springconfig
    env: test                           #配置环境 一般常用test/prod/dev等，跟相应配置文件匹配
    type: .yml                          #文件格式，目前仅支持yaml
    mid: "-"                            #配置文件中间名
    used: tdengine     #当前应用启用的配置,MySQL代表使用GORM v2版本的MySQL，小写mysql代表GORM v1版本
    prefix:                             #配置文件名前缀定义
      mysql: mysql                      #mysql对应的配置文件名前缀，如当前配置中对应的配置文件名为 mysql-go-test.yml
      mongodb: mongodb
      redis: redis
      rabbitmq: rabbitmq
      nacos: nacos
      pgsql: pgsql
      mssql: mssql
      tdengine: tdengine-oss-traffic
      consul: consul
      elasticsearch: elasticsearch
      hbase: hbase
      hive: hive
      couchdb: couchdb
      influxdb: influxdb
```

## 初始化
在main.go中，在执行完	mgconfig.InitConfig(configFile) 之后导入
```go
func main(){
	...
	mgin.Init(configFile)
    defer mgin.MGin.SaveExit()
	// 初始化TDengine连接
    mgin.MGin.Use("tdengine", mgtdengine.TDengine.Init, nil, nil)
}
```

## 获取tdengine连接
```go
    td,err := mgtdengine.TDengine.GetConnection()
    if err != nil {
    	logs.Error("TDengine connection error: {}", err.Error())
    }
    defer td.Close()
```

## 多库支持时获取指定库tdengine连接
```go
    td,err := mgtdengine.TDengine.GetConnection("test1")
    if err != nil {
    	logs.Error("TDengine connection error: {}", err.Error())
    }
    defer td.Close()
```

## 更新日志
- v1.1.0 更新为MGin框架插件，不再依赖mgconfig包
- v1.1.6 增加对Restful模式连接配置支持
- v1.1.7 变更成短连接模式，避免taos驱动本身的长连接Idle故障