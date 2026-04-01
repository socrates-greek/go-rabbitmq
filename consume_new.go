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
	Ack Action = iota
	NackDiscard
	NackRequeue
	Manual // 手动ACK，业务自行处理
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

type ackSession struct {
	batchAckChan chan uint64
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

	// 重连指数退避
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

	session := &ackSession{
		batchAckChan: make(chan uint64, consumer.calculateBatchAckChanCapacity()),
	}

	if consumer.options.EnableBatchAck && !consumer.options.RabbitConsumerOptions.AutoAck {
		consumer.wg.Add(1)
		go consumer.batchAckCoordinator(consumer.consumerCtx, session, ch)
	}

	// 启动worker
	for i := 0; i < consumer.options.Concurrency; i++ {
		consumer.wg.Add(1)
		go consumer.handlerWorker(consumer.consumerCtx, session, msgs, handler)
	}

	consumer.options.Logger.Infof("Consumer started, workers=%d, batch=%v, prefetch=%d",
		consumer.options.Concurrency, consumer.options.EnableBatchAck, consumer.options.QOSPrefetch)
	return nil
}

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

// 批量ACK协程：增加map清理，防止内存泄漏
func (consumer *Consumer) batchAckCoordinator(ctx context.Context, session *ackSession, ch *amqp.Channel) {
	defer consumer.wg.Done()

	batchAckChan := session.batchAckChan
	expectedTag := uint64(1)
	var lastAckedTag uint64 = 0
	completedTags := make(map[uint64]bool, consumer.options.BatchSize*2)

	ticker := time.NewTicker(consumer.options.BatchTimeout)
	defer ticker.Stop()

	flush := func() {
		highestSafeTag := expectedTag - 1
		if highestSafeTag > lastAckedTag {
			if ch.IsClosed() {
				return
			}
			if err := ch.Ack(highestSafeTag, true); err != nil {
				consumer.options.Logger.Errorf("batch ack failed: %v", err)
			} else {
				lastAckedTag = highestSafeTag
				// 优化：清理已ACK的tag，防止内存泄漏
				for tag := range completedTags {
					if tag <= highestSafeTag {
						delete(completedTags, tag)
					}
				}
			}
		}
	}

	for {
		select {
		case <-ctx.Done():
			flush()
			return
		case tag, ok := <-batchAckChan:
			if !ok {
				flush()
				return
			}
			completedTags[tag] = true
			for completedTags[expectedTag] {
				delete(completedTags, expectedTag)
				expectedTag++
			}
			if (expectedTag-1)-lastAckedTag >= uint64(consumer.options.BatchSize) {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

// 【终极修复】全链路panic捕获 + 处理Manual动作 + 修复CPU空转
func (consumer *Consumer) handlerWorker(
	ctx context.Context,
	session *ackSession,
	msgs <-chan amqp.Delivery,
	handler Handler,
) {
	defer consumer.wg.Done()
	batchAckChan := session.batchAckChan

	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-msgs:
			// 修复：通道关闭直接退出，由重连逻辑重建worker，不CPU空转
			if !ok || consumer.getIsClosed() {
				return
			}

			// 修复：全流程捕获panic，覆盖 handler + ack/nack 所有阶段
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

			if consumer.options.RabbitConsumerOptions.AutoAck {
				continue
			}

			// 修复：增加 Manual 动作处理（不做任何操作，业务手动ACK）
			switch action {
			case Ack:
				if consumer.options.EnableBatchAck {
					if !consumer.sendToBatchAck(batchAckChan, msg.DeliveryTag) {
						if err := msg.Ack(false); err != nil {
							consumer.options.Logger.Errorf("fallback ack failed: %v", err)
						}
					}
				} else {
					if err := msg.Ack(false); err != nil {
						consumer.options.Logger.Errorf("single ack failed: %v", err)
					}
				}
			case NackDiscard:
				if err := msg.Nack(false, false); err != nil {
					consumer.options.Logger.Errorf("nack discard failed: %v", err)
				}
			case NackRequeue:
				if err := msg.Nack(false, true); err != nil {
					consumer.options.Logger.Errorf("nack requeue failed: %v", err)
				}
			case Manual:
				// 手动ACK模式：不做任何操作，业务自行处理
				consumer.options.Logger.Debugf("manual ack mode, skip processing tag: %d", msg.DeliveryTag)
			}
		}
	}
}

// 优化：复用timer对象，减少GC
var sendTimerPool = sync.Pool{
	New: func() interface{} {
		return time.NewTimer(2 * time.Millisecond)
	},
}

func (consumer *Consumer) sendToBatchAck(ch chan uint64, tag uint64) bool {
	select {
	case ch <- tag:
		return true
	default:
	}

	timer := sendTimerPool.Get().(*time.Timer)
	defer sendTimerPool.Put(timer)
	defer timer.Stop()

	select {
	case ch <- tag:
		return true
	case <-timer.C:
		return false
	}
}

func (consumer *Consumer) calculateBatchAckChanCapacity() int {
	cap := consumer.options.Concurrency * consumer.options.BatchSize * 2
	if cap < 1000 {
		return 1000
	}
	if cap > 100000 {
		return 100000
	}
	return cap
}

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
