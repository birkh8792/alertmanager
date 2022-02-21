package kafka

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/Shopify/sarama"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/alertmanager/config"
	"github.com/prometheus/alertmanager/notify"
	"github.com/prometheus/alertmanager/template"
	"github.com/prometheus/alertmanager/types"
	"sync"
	"sync/atomic"
)

// Kafka implements a Notifier for email notifications.
type Kafka struct {
	conf    *config.KafkaConfig
	tmpl    *template.Template
	logger  log.Logger
	client  *Client
	counter uint64
	sync.RWMutex
}

func New(conf *config.KafkaConfig, t *template.Template, l log.Logger) (*Kafka, error) {
	// 创建一个 Kafka 的 客户端
	client, err := newKafkaClient(conf)
	if err != nil {
		return nil, err
	}
	client.logger = l
	level.Info(l).Log("kafka client", "kafka client created success")
	go client.AsyncSendMsg(context.Background())
	return &Kafka{
		conf:   conf,
		tmpl:   t,
		logger: l,
		client: client,
	}, nil
}

type Message struct {
	*template.Data
	Version  string `json:"version"`
	GroupKey string `json:"groupKey"`
}

func (n *Kafka) Notify(ctx context.Context, alerts ...*types.Alert) (bool, error) {
	atomic.AddUint64(&n.counter, 1)
	data := notify.GetTemplateData(ctx, n.tmpl, alerts, n.logger)
	level.Info(n.logger).Log("counter", atomic.LoadUint64(&n.counter))
	groupKey, err := notify.ExtractGroupKey(ctx)
	if err != nil {
		level.Debug(n.logger).Log("groupKey", groupKey)
		return false, errors.Wrap(err, "groupKey error")
	}
	msg := &Message{
		Version:  "4",
		Data:     data,
		GroupKey: groupKey.String(),
	}
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(msg); err != nil {
		level.Error(n.logger).Log("json", err)
		return false, errors.Wrap(err, "json encode error")
	}
	go input(buf.String())
	return false, nil
}

var messageChan = make(chan string)

func input(msg string) {
	messageChan <- msg
}

func (c *Client) AsyncSendMsg(ctx context.Context) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				level.Debug(c.logger).Log("status", "failed", "recovery result", r)
			}
		}()
		for err := range c.clientAsync.Errors() {
			level.Error(c.logger).Log("status", "failed", "error", err)
		}
	}()
	go func() {
		defer func() {
			if r := recover(); r != nil {
				level.Error(c.logger).Log("recovery", r)

			}
		}()
		for successes := range c.clientAsync.Successes() {
			level.Info(c.logger).Log("status", "success", "topic", successes.Topic, "offset", successes.Offset, "partition", successes.Partition)
		}
	}()
ProducerLoop:
	for {
		select {
		case msg := <-messageChan:
			c.clientAsync.Input() <- &sarama.ProducerMessage{Topic: c.topic, Value: sarama.StringEncoder(msg)}
		case <-ctx.Done():
			c.clientAsync.AsyncClose()
			break ProducerLoop
		}
	}
}

type Client struct {
	clientAsync sarama.AsyncProducer
	clientSync  sarama.SyncProducer
	logger      log.Logger
	topic       string
}

func newKafkaClient(kafkaConfig *config.KafkaConfig) (*Client, error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = kafkaConfig.KafkaProducerConfig.RequiredAcks.GetAck()
	config.Producer.Retry.Max = kafkaConfig.KafkaProducerConfig.RetryMax

	if kafkaConfig.KafkaProducerConfig.MaxMessageBytes == 0 {
		kafkaConfig.KafkaProducerConfig.MaxMessageBytes = 1000000
	}
	config.Producer.MaxMessageBytes = kafkaConfig.KafkaProducerConfig.MaxMessageBytes
	config.Producer.Return.Successes = true
	config.Net.DialTimeout = kafkaConfig.KafkaNetConfig.DialTimeout
	config.Metadata.Timeout = kafkaConfig.MetadataTimeout
	config.Version = kafkaConfig.Version.GetVersion()
	config.Net.SASL.Enable = kafkaConfig.KafkaNetConfig.SASL.Enabled
	config.Net.SASL.User = kafkaConfig.KafkaNetConfig.SASL.Username
	config.Net.SASL.Password = kafkaConfig.KafkaNetConfig.SASL.Password
	//if kafkaConfig.KafkaNetConfig.SASL.SCRAMClientGeneratorFunc == "SHA256" || kafkaConfig.KafkaNetConfig.SASL.SCRAMClientGeneratorFunc == "" {
	//	config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
	//		return &XDGSCRAMClient{HashGeneratorFcn: SHA256}
	//	}
	//	config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
	//} else if kafkaConfig.KafkaNetConfig.SASL.SCRAMClientGeneratorFunc == "SHA512" {
	//	config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
	//		return &XDGSCRAMClient{HashGeneratorFcn: SHA512}
	//	}
	//	config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
	//}
	asyncProducer, err := sarama.NewAsyncProducer(kafkaConfig.Brokers, config)
	if err != nil {
		return nil, errors.Wrapf(err, "new kafka async producer error:{%s}", err.Error())
	}
	syncProducer, err := sarama.NewSyncProducer(kafkaConfig.Brokers, config)
	if err != nil {
		return nil, errors.Wrapf(err, "new kafka sync producer error:{%s}", err.Error())
	}
	return &Client{
		clientAsync: asyncProducer,
		clientSync:  syncProducer,
		topic:       kafkaConfig.Topic,
	}, nil
}
