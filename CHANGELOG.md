# v2.1.0

+ 更新模式,现在代理已经被抽出来放在项目[github.com/Golang-Tools/kafkahelper](https://github.com/Golang-Tools/kafkahelper)中,本项目只用快速构建单体应用

# v2.0.0

+ 更新支持版本,v2版本开始只支持go 1.18+
+ 更新依赖`github.com/confluentinc/confluent-kafka-go`至v1.8.2

# v0.0.1

项目创建

## 核心组件模板

| 组件名               | 说明                  |
| -------------------- | --------------------- |
| `kafkasender_proxy`  | kafka的生产者代理模板 |
| `kafkawatcher_proxy` | kafka的消费者代理模板 |
