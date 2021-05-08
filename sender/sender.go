package sender

import (
	"errors"

	log "github.com/Golang-Tools/loggerhelper"
	// "gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

//ErrProxyAllreadySettedClient 代理已经设置过redis客户端对象
var ErrProxyAllreadySettedClient = errors.New("代理不能重复设置客户端对象")

//ErrProxyNotYetSettedClient 代理还未设置客户端对象
var ErrProxyNotYetSettedClient = errors.New("代理还未设置客户端对象")

//Callback redis操作的回调函数
type Callback func(cli *kafka.Producer) error

//ProducerProxy redis客户端的代理
type ProducerProxy struct {
	*kafka.Producer
	parallelcallback  bool
	callBacks         []Callback
	delivered_records int64
}

// New 创建一个新的数据库客户端代理
func New() *ProducerProxy {
	proxy := new(ProducerProxy)
	proxy.delivered_records = 0
	return proxy
}

// IsOk 检查代理是否已经可用
func (proxy *ProducerProxy) IsOk() bool {
	if proxy.Producer == nil {
		return false
	}
	return true
}

// DeliveredRecords 查看已经发送了几条信息
func (proxy *ProducerProxy) DeliveredRecords() int64 {
	return proxy.delivered_records
}

//SetConnect 设置连接的客户端
//@params cli UniversalClient 满足redis.UniversalClient接口的对象的指针
func (proxy *ProducerProxy) SetConnect(cli *kafka.Producer) error {
	if proxy.IsOk() {
		return ErrProxyAllreadySettedClient
	}

	proxy.Producer = cli
	if proxy.parallelcallback {
		for _, cb := range proxy.callBacks {
			go func(cb Callback) {
				err := cb(proxy.Producer)
				if err != nil {
					log.Error("regist callback get error", log.Dict{"err": err})
				} else {
					log.Debug("regist callback done")
				}
			}(cb)
		}
	} else {
		for _, cb := range proxy.callBacks {
			err := cb(proxy.Producer)
			if err != nil {
				log.Error("regist callback get error", log.Dict{"err": err})
			} else {
				log.Debug("regist callback done")
			}
		}
	}
	return nil
}

//InitFromOptions 从配置条件初始化代理对象
func (proxy *ProducerProxy) InitFromOptions(options *kafka.ConfigMap) error {
	cli, err := kafka.NewProducer(options)
	if err != nil {
		return err
	}
	return proxy.SetConnect(cli)
}

//InitFromOptionsParallelCallback 从配置条件初始化代理对象,并行执行回调函数
func (proxy *ProducerProxy) InitFromOptionsParallelCallback(options *kafka.ConfigMap) error {
	cli, err := kafka.NewProducer(options)
	if err != nil {
		return err
	}
	proxy.parallelcallback = true
	return proxy.SetConnect(cli)
}

// Regist 注册回调函数,在init执行后执行回调函数
//如果对象已经设置了被代理客户端则无法再注册回调函数
func (proxy *ProducerProxy) Regist(cb Callback) error {
	if proxy.IsOk() {
		return ErrProxyAllreadySettedClient
	}
	proxy.callBacks = append(proxy.callBacks, cb)
	return nil
}

func (proxy *ProducerProxy) Start() error {
	if !proxy.IsOk() {
		return ErrProxyNotYetSettedClient
	}
	func() {
		for e := range proxy.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				{
					if ev.TopicPartition.Error != nil {
						log.Error("Delivery failed", log.Dict{"TopicPartition": ev.TopicPartition})
					} else {
						proxy.delivered_records += 1
						log.Info("Delivered message", log.Dict{"TopicPartition": ev.TopicPartition})
					}
				}
			default:
				{
					log.Error("kafka producer Ignored event", log.Dict{"ev": ev})
				}
			}
		}
	}()
	return nil
}

func (proxy *ProducerProxy) sendAsync(msg *kafka.Message) {
	proxy.ProduceChannel() <- msg
}

func (proxy *ProducerProxy) Send(msg *kafka.Message) {
	go proxy.sendAsync(msg)
}

func (proxy *ProducerProxy) SendAndWait(msg *kafka.Message) error {
	err := proxy.Produce(msg, nil) //, ins.deliveryChan)
	if err != nil {
		log.Error("publish to kafka error", log.Dict{"err": err, "msg": msg})
	}
	return err
}

//Proxy 默认的redis代理对象
var Sender = New()
