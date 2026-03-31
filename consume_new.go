package rabbitmq

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Bifang-Bird/go-rabbitmq/internal/channelmanager"
	amqp "github.com/rabbitmq/amqp091-go"
)

// Action 定义
type Action int

const (
	Ack Action = iota
	NackDiscard
	NackRequeue
	Manual
)

type Handler func(d Delivery) (action Action)

type Delivery struct {
	amqp.Delivery
}

type Consumer struct {
	chanManager                *channelmanager.ChannelManager
	reconnectErrCh             <-chan error
	closeConnectionToManagerCh chan<- struct{}
	options                    ConsumerOptions
	isClosedMux                *sync.RWMutex
	isClosed                   bool

	// 新增：用于协调批量 Ack 的通道
	batchAckChan chan uint64
	wg           sync.WaitGroup
}

func NewConsumer(
	conn *Conn,
	handler Handler,
	queue string,
	optionFuncs ...func(*ConsumerOptions),
) (*Consumer, error) {
	defaultOptions := getDefaultConsumerOptions(queue)
	options := &defaultOptions
	for _, optionFunc := range optionFuncs {
		optionFunc(options)
	}

	if conn.connectionManager == nil {
		return nil, errors.New("connection manager can't be nil")
	}

	chanManager, err := channelmanager.NewChannelManager(conn.connectionManager, options.Logger, conn.connectionManager.ReconnectInterval)
	if err != nil {
		return nil, err
	}
	reconnectErrCh, closeCh := chanManager.NotifyReconnect()

	consumer := &Consumer{
		chanManager:                chanManager,
		reconnectErrCh:             reconnectErrCh,
		closeConnectionToManagerCh: closeCh,
		options:                    *options,
		isClosedMux:                &sync.RWMutex{},
		isClosed:                   false,
		batchAckChan:               make(chan uint64, options.BatchSize*2), // 缓冲大小
	}

	// 启动逻辑
	if err := consumer.start(handler); err != nil {
		return nil, err
	}

	// 重连监听
	go func() {
		for err := range consumer.reconnectErrCh {
			consumer.options.Logger.Infof("successful consumer recovery from: %v", err)
			if err := consumer.start(handler); err != nil {
				consumer.options.Logger.Errorf("critical: consumer recovery failed: %v", err)
			}
		}
	}()

	return consumer, nil
}

func (consumer *Consumer) start(handler Handler) error {
	// 1. 基础声明
	if err := consumer.setupTopology(); err != nil {
		return err
	}

	// 2. 获取消费通道
	msgs, err := consumer.chanManager.ConsumeSafe(
		consumer.options.QueueOptions.Name,
		consumer.options.RabbitConsumerOptions.Name,
		consumer.options.RabbitConsumerOptions.AutoAck,
		consumer.options.RabbitConsumerOptions.Exclusive,
		false,
		consumer.options.RabbitConsumerOptions.NoWait,
		tableToAMQPTable(consumer.options.RabbitConsumerOptions.Args),
	)
	if err != nil {
		return err
	}

	// 3. 启动批量确认协调协程 (关键优化)
	if consumer.options.EnableBatchAck && !consumer.options.RabbitConsumerOptions.AutoAck {
		consumer.wg.Add(1)
		go consumer.batchAckCoordinator()
	}

	// 4. 启动并发工作协程
	for i := 0; i < consumer.options.Concurrency; i++ {
		consumer.wg.Add(1)
		go consumer.handlerWorker(msgs, handler)
	}

	consumer.options.Logger.Infof("Consumer started with %d workers", consumer.options.Concurrency)
	return nil
}

// setupTopology 提取拓扑配置逻辑
func (consumer *Consumer) setupTopology() error {
	ops := consumer.options
	if err := consumer.chanManager.QosSafe(ops.QOSPrefetch, 0, ops.QOSGlobal); err != nil {
		return fmt.Errorf("qos failed: %w", err)
	}
	if err := declareExchange(consumer.chanManager, ops.ExchangeOptions); err != nil {
		return err
	}
	if err := declareQueue(consumer.chanManager, ops.QueueOptions); err != nil {
		return err
	}
	return declareBindings(consumer.chanManager, ops)
}

// batchAckCoordinator 唯一的批量提交者，保证了 Tag 的顺序安全性
func (consumer *Consumer) batchAckCoordinator() {
	defer consumer.wg.Done()

	var lastTag uint64
	var count int
	ticker := time.NewTicker(consumer.options.BatchTimeout)
	defer ticker.Stop()

	flush := func() {
		if count > 0 {
			// multiple: true 表示确认所有小于等于 lastTag 的消息
			err := consumer.chanManager.GetChannel().Ack(lastTag, true)
			if err != nil {
				consumer.options.Logger.Errorf("batch ack failed: %v", err)
			}
			count = 0
		}
	}

	for {
		select {
		case tag, ok := <-consumer.batchAckChan:
			if !ok {
				flush()
				return
			}
			// 只有在当前 Tag 比之前的大的时候才更新（确保顺序性）
			if tag > lastTag {
				lastTag = tag
			}
			count++
			if count >= consumer.options.BatchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

func (consumer *Consumer) handlerWorker(msgs <-chan amqp.Delivery, handler Handler) {
	defer consumer.wg.Done()

	for msg := range msgs {
		if consumer.getIsClosed() {
			return
		}

		// 处理业务逻辑
		action := handler(Delivery{msg})

		// 处理确认逻辑
		if consumer.options.RabbitConsumerOptions.AutoAck {
			continue
		}

		switch action {
		case Ack:
			if consumer.options.EnableBatchAck {
				consumer.batchAckChan <- msg.DeliveryTag
			} else {
				_ = msg.Ack(false)
			}
		case NackDiscard:
			_ = msg.Nack(false, false)
		case NackRequeue:
			_ = msg.Nack(false, true)
		case Manual:
			// 忽略
		}
	}
}

func (consumer *Consumer) Close() {
	consumer.isClosedMux.Lock()
	if consumer.isClosed {
		consumer.isClosedMux.Unlock()
		return
	}
	consumer.isClosed = true
	consumer.isClosedMux.Unlock()

	// 1. 先关闭 Channel，停止接收新消息
	_ = consumer.chanManager.Close()

	// 2. 关闭批量确认通道
	close(consumer.batchAckChan)

	// 3. 等待所有工作协程完成
	consumer.wg.Wait()

	// 4. 通知管理器关闭
	consumer.closeConnectionToManagerCh <- struct{}{}
}

func (consumer *Consumer) getIsClosed() bool {
	consumer.isClosedMux.RLock()
	defer consumer.isClosedMux.RUnlock()
	return consumer.isClosed
}
