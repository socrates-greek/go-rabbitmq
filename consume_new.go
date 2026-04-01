package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Bifang-Bird/go-rabbitmq/internal/channelmanager"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Action int

const (
	Ack         Action = iota
	NackDiscard        // 丢弃消息
	NackRequeue        // 重新入队
	Manual             // 手动ACK，业务自行处理（框架不做任何操作）
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
	wg                         sync.WaitGroup
	consumerCtx                context.Context
	consumerCancel             context.CancelFunc
	startMux                   sync.Mutex
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
	}

	if err := consumer.start(handler); err != nil {
		return nil, err
	}

	// 重连逻辑
	go func() {
		retryInterval := time.Second
		maxInterval := 10 * time.Second
		for err := range consumer.reconnectErrCh {
			if consumer.getIsClosed() {
				return
			}
			consumer.options.Logger.Infof("consumer reconnecting after error: %v", err)
			time.Sleep(retryInterval)

			if retryInterval < maxInterval {
				retryInterval *= 2
			}

			if err := consumer.start(handler); err != nil {
				consumer.options.Logger.Errorf("consumer restart failed: %v", err)
			} else {
				retryInterval = time.Second
			}
		}
	}()

	return consumer, nil
}

func (consumer *Consumer) start(handler Handler) error {
	consumer.startMux.Lock()
	defer consumer.startMux.Unlock()

	// 优雅关闭旧实例
	if consumer.consumerCancel != nil {
		consumer.consumerCancel()
		done := make(chan struct{})
		go func() {
			consumer.wg.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			consumer.options.Logger.Warnf("worker stop timeout")
		}
	}

	consumer.consumerCtx, consumer.consumerCancel = context.WithCancel(context.Background())

	if err := consumer.setupTopology(); err != nil {
		return err
	}

	ch := consumer.chanManager.GetChannel()
	if ch == nil || ch.IsClosed() {
		return errors.New("channel is closed")
	}

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

	// 启动并发 Worker
	for i := 0; i < consumer.options.Concurrency; i++ {
		consumer.wg.Add(1)
		go consumer.handlerWorker(consumer.consumerCtx, msgs, handler)
	}

	consumer.options.Logger.Infof("Consumer started, workers=%d, prefetch=%d",
		consumer.options.Concurrency, consumer.options.QOSPrefetch)
	return nil
}

func (consumer *Consumer) setupTopology() error {
	ops := consumer.options
	// QOSPrefetch 决定了管道里能积压多少条消息未确认
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

func (consumer *Consumer) handlerWorker(
	ctx context.Context,
	msgs <-chan amqp.Delivery,
	handler Handler,
) {
	defer consumer.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-msgs:
			if !ok || consumer.getIsClosed() {
				return
			}

			// 捕获 panic 确保 worker 不挂掉，并将 panic 视为需要重试的情况
			var action Action
			func() {
				defer func() {
					if r := recover(); r != nil {
						consumer.options.Logger.Errorf("worker panic recovered: %v, tag=%d", r, msg.DeliveryTag)
						action = NackRequeue
					}
				}()
				// 执行业务逻辑
				action = handler(Delivery{msg})
			}()

			// 如果是自动确认模式，不需要进行 Ack/Nack 操作
			if consumer.options.RabbitConsumerOptions.AutoAck {
				continue
			}

			// 处理返回动作
			switch action {
			case Ack:
				// 单条 Ack，不再使用 batch 逻辑，规避序列断裂风险
				if err := msg.Ack(false); err != nil {
					consumer.options.Logger.Errorf("ack failed: %v, tag=%d", err, msg.DeliveryTag)
				}
			case NackDiscard:
				// 拒绝并丢弃
				if err := msg.Nack(false, false); err != nil {
					consumer.options.Logger.Errorf("nack discard failed: %v, tag=%d", err, msg.DeliveryTag)
				}
			case NackRequeue:
				// 拒绝并重新入队
				if err := msg.Nack(false, true); err != nil {
					consumer.options.Logger.Errorf("nack requeue failed: %v, tag=%d", err, msg.DeliveryTag)
				}
			case Manual:
				// 业务层自己调用了 msg.Ack/Nack，框架这里什么都不做
				consumer.options.Logger.Debugf("manual mode, tag=%d", msg.DeliveryTag)
			default:
				// 兜底：默认 ACK 还是 NACK 视业务而定，通常建议 NACK 重新入队以防丢失
				consumer.options.Logger.Warnf("unknown action, fallback to requeue: %d", msg.DeliveryTag)
				_ = msg.Nack(false, true)
			}
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

	consumer.options.Logger.Infof("Closing consumer...")
	if consumer.consumerCancel != nil {
		consumer.consumerCancel()
	}

	done := make(chan struct{})
	go func() {
		consumer.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		consumer.options.Logger.Warnf("close timeout")
	}

	if err := consumer.chanManager.Close(); err != nil {
		consumer.options.Logger.Errorf("close channel manager failed: %v", err)
	}
	select {
	case consumer.closeConnectionToManagerCh <- struct{}{}:
	case <-time.After(2 * time.Second):
	}
	consumer.options.Logger.Infof("Consumer closed")
}

func (consumer *Consumer) getIsClosed() bool {
	consumer.isClosedMux.RLock()
	defer consumer.isClosedMux.RUnlock()
	return consumer.isClosed
}

// 工具函数保留
func isNetworkError(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "broken pipe") ||
		strings.Contains(s, "connection reset") ||
		strings.Contains(s, "EOF") ||
		strings.Contains(s, "closed")
}
