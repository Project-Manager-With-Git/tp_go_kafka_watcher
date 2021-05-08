package sender

import (
	"fmt"
	"os"
	"strings"
	"time"

	log "github.com/Golang-Tools/loggerhelper"
	s "github.com/Golang-Tools/schema-entry-go"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	// "gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

type Application struct {
	App_Version                       string   `json:"app_version" jsonschema:"required,title=v,description=应用版本"`
	App_Name                          string   `json:"app_name" jsonschema:"required,title=n,description=应用名"`
	Log_Level                         string   `json:"log_level" jsonschema:"required,title=l,description=log等级,enum=TRACE,enum=DEBUG,enum=INFO,enum=WARN,enum=ERROR"`
	Send_Kafka_URLS                   []string `json:"send_kafka_urls" jsonschema:"required,title=u,description=监听的kafka集群地址"`
	Send_Kafka_Go_Delivery_Reports    bool     `json:"send_kafka_go_delivery_reports" jsonschema:"required,title=r,description=是否报告发送情况"`
	Send_Kafka_Go_Batch_Producer      bool     `json:"send_kafka_go_batch_producer" jsonschema:"required,title=b,description=是否按批发送"`
	Send_Kafka_Queue_Buffering_Max_Ms int      `json:"send_kafka_queue_buffering_max_ms" jsonschema:"required,title=m,description=queue最大缓存多少毫秒后发送"`
	Send_Kafka_Acks                   int      `json:"send_kafka_acks" jsonschema:"required,title=a,description=校验逻辑"`
}

func (app *Application) Main() {
	log.Init(app.Log_Level, map[string]interface{}{"app_name": app.App_Name})
	err := Sender.InitFromOptions(&kafka.ConfigMap{
		"bootstrap.servers":      strings.Join(app.Send_Kafka_URLS, ","),
		"go.delivery.reports":    app.Send_Kafka_Go_Delivery_Reports,
		"go.batch.producer":      app.Send_Kafka_Go_Batch_Producer,
		"queue.buffering.max.ms": app.Send_Kafka_Queue_Buffering_Max_Ms,
		"acks":                   app.Send_Kafka_Acks,
	})
	if err != nil {
		log.Error("Failed to init watcher", log.Dict{"error": err})
		os.Exit(1)
	}
	defer Sender.Close()
	for i := 0; i < 10; i++ {
		topic := "topic1-1-1"
		value := fmt.Sprintf("msg%d", i)
		err := Sender.SendAndWait(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          []byte(value),
			Key:            []byte("key1"),
			Headers: []kafka.Header{
				{Key: "h1", Value: []byte("h1")},
				{Key: "h2", Value: []byte("h2")},
			},
		})
		if err != nil {
			log.Error("sender get error", log.Dict{"error": err})
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// root, _ := s.New(&s.EntryPointMeta{Name: "tp_go_kafka_watcher", Usage: "tp_go_kafka_watcher"})
// nodeb, _ := s.New(&s.EntryPointMeta{Name: "sender", Usage: "tp_go_kafka_watcher sender"})
var NodeSender, _ = s.New(&s.EntryPointMeta{Name: "sender", Usage: "tp_go_kafka_watcher sender"}, &Application{
	App_Version:                       "0.0.0",
	App_Name:                          "tp_go_kafka_watcher_watcher",
	Log_Level:                         "DEBUG",
	Send_Kafka_Go_Delivery_Reports:    false,
	Send_Kafka_Go_Batch_Producer:      true,
	Send_Kafka_Queue_Buffering_Max_Ms: 10,
	Send_Kafka_Acks:                   0,
})

// s.RegistSubNode(root, nodeb)
// nodeb.RegistSubNode(nodec)
// os.Setenv("FOO_BAR_PAR_A", "123")
// root.Parse([]string{"foo", "bar", "par", "--Field=4", "--Field=5", "--Field=6"})
