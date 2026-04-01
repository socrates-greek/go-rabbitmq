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
	wg                         sync.WaitGroup
	consumerCtx                context.Context
	consumerCancel             context.CancelFunc
	startMux                   sync.Mutex
}

type ackSession struct {
	batchAckChan chan uint64
	ctx          context.Context
	cancel       context.CancelFunc
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
	consumer.startMux.Lock()
	defer consumer.startMux.Unlock()

	if consumer.consumerCancel != nil {
		// 优雅停止旧协程，等待消息处理完成
		consumer.options.Logger.Infof("Stopping old workers for reconnect...")
		consumer.consumerCancel()

		// 等待旧 worker 完成，最多等待 30 秒
		done := make(chan struct{})
		go func() {
			consumer.wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			consumer.options.Logger.Debugf("all old workers stopped")
		case <-time.After(30 * time.Second):
			consumer.options.Logger.Warnf("timeout waiting for old workers, forcing stop")
		}
	}

	consumer.consumerCtx, consumer.consumerCancel = context.WithCancel(context.Background())

	if err := consumer.setupTopology(); err != nil {
		return err
	}

	// ------------- 修复 2：每次重连都获取新 channel，杜绝野指针 -------------
	// 核心修复：获取有效Channel，重连后一定是新Channel
	ch := consumer.chanManager.GetChannel()
	if ch == nil || ch.IsClosed() {
		return errors.New("get channel failed: channel closed")
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

	// 优化 1：增加 batchAckChan 容量，支持高并发
	// 容量 = Concurrency * BatchSize * 2，确保不会轻易满
	session := &ackSession{
		batchAckChan: make(chan uint64,
			consumer.calculateBatchAckChanCapacity()),
	}

	if consumer.options.EnableBatchAck && !consumer.options.RabbitConsumerOptions.AutoAck {
		// 优化 2：为 batchAckCoordinator 创建独立的 context
		session.ctx, session.cancel = context.WithCancel(consumer.consumerCtx)
		consumer.wg.Add(1)
		go consumer.batchAckCoordinator(session, ch)
	}

	// 启动worker
	for i := 0; i < consumer.options.Concurrency; i++ {
		consumer.wg.Add(1)
		go consumer.handlerWorker(session, msgs, handler)
	}

	consumer.options.Logger.Infof("Consumer started with %d workers, batch_ack=%v, prefetch=%d",
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

// 优化 6：批量 ACK 协调器 - 无锁设计，高性能
func (consumer *Consumer) batchAckCoordinator(session *ackSession, ch *amqp.Channel) {
	defer consumer.wg.Done()
	defer session.cancel()
	defer close(session.batchAckChan)

	batchAckChan := session.batchAckChan

	// DeliveryTag 仅当前 Channel 有效，重置计数器
	expectedTag := uint64(1)
	var lastAckedTag uint64 = 0
	completedTags := make(map[uint64]bool, consumer.options.BatchSize*2)

	ticker := time.NewTicker(consumer.options.BatchTimeout)
	defer ticker.Stop()

	flush := func() {
		highestSafeTag := expectedTag - 1
		if highestSafeTag > lastAckedTag {
			// 优化 7：去掉不必要的锁，ch.Ack 本身就是线程安全的
			if ch.IsClosed() {
				consumer.options.Logger.Errorf("batch ack failed: channel closed")
				return
			}

			err := ch.Ack(highestSafeTag, true)
			if err != nil {
				consumer.options.Logger.Errorf("batch ack tag %d failed: %v", highestSafeTag, err)
				// 如果是网络错误，可能需要重连
				if isNetworkError(err) {
					consumer.options.Logger.Warnf("network error during ack, may need reconnect")
				}
			} else {
				lastAckedTag = highestSafeTag
				consumer.options.Logger.Debugf("batch ack success, last tag: %d, count: %d",
					lastAckedTag, len(completedTags))
			}
		}
	}

	for {
		select {
		case <-session.ctx.Done():
			consumer.options.Logger.Debugf("batch ack coordinator stopping, flushing pending acks")
			flush()
			return
		case tag, ok := <-batchAckChan:
			if !ok {
				consumer.options.Logger.Debugf("batch ack chan closed, flushing")
				flush()
				return
			}
			completedTags[tag] = true
			// 推进 expectedTag，确认连续的消息
			for completedTags[expectedTag] {
				delete(completedTags, expectedTag)
				expectedTag++
			}
			// 达到批量大小，立即 flush
			if (expectedTag-1)-lastAckedTag >= uint64(consumer.options.BatchSize) {
				flush()
			}
		case <-ticker.C:
			// 超时 flush，保证延迟上限
			flush()
		}
	}
}

// 优化 8：Worker 协程 - 无锁设计，高性能
func (consumer *Consumer) handlerWorker(session *ackSession, msgs <-chan amqp.Delivery, handler Handler) {
	defer consumer.wg.Done()

	batchAckChan := session.batchAckChan

	for {
		select {
		case <-consumer.consumerCtx.Done():
			return
		case msg, ok := <-msgs:
			if !ok {
				consumer.options.Logger.Debugf("message channel closed, worker exiting")
				return
			}

			// 检查 consumer 是否已关闭
			if consumer.getIsClosed() {
				consumer.options.Logger.Debugf("consumer closed, discarding message")
				return
			}

			action := handler(Delivery{msg})

			if consumer.options.RabbitConsumerOptions.AutoAck {
				continue
			}

			// 优化 9：完全去掉锁，msg 对象本身是值传递，可以安全使用
			switch action {
			case Ack:
				if consumer.options.EnableBatchAck {
					// 优化 10：带重试的降级策略
					if !consumer.sendToBatchAck(batchAckChan, msg.DeliveryTag) {
						// 降级为单条 ACK
						if err := msg.Ack(false); err != nil {
							consumer.options.Logger.Errorf("fallback single ack failed: %v", err)
						} else {
							consumer.options.Logger.Debugf("fallback single ack success for tag: %d", msg.DeliveryTag)
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
			}
		}
	}
}

// 优化 11：辅助方法 - 发送 DeliveryTag 到批量 ACK 通道（带重试）
func (consumer *Consumer) sendToBatchAck(batchAckChan chan uint64, tag uint64) bool {
	// 第一次尝试（非阻塞）
	select {
	case batchAckChan <- tag:
		return true
	default:
	}

	// 重试：使用可控制的 Timer
	timer := time.NewTimer(2 * time.Millisecond)
	defer timer.Stop() // ✅ 确保资源释放

	select {
	case batchAckChan <- tag:
		return true
	case <-timer.C:
		return false
	}
}

// 优化 12：动态计算 batchAckChan 容量
func (consumer *Consumer) calculateBatchAckChanCapacity() int {
	capacity := consumer.options.Concurrency * consumer.options.BatchSize * 2
	// 最小容量不低于 1000，最大不超过 100000
	if capacity < 1000 {
		capacity = 1000
	}
	if capacity > 100000 {
		capacity = 100000
	}
	return capacity
}

// 优化 13：判断是否为网络错误
func isNetworkError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return containsAny(errStr, []string{
		"broken pipe",
		"connection reset",
		"i/o timeout",
		"EOF",
		"closed",
	})
}

func containsAny(s string, substrs []string) bool {
	for _, sub := range substrs {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
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

	// 优化 14：先取消 context，让 worker 停止接收新消息
	if consumer.consumerCancel != nil {
		consumer.consumerCancel()
	}

	// 优化 15：等待 worker 完成，但设置超时
	done := make(chan struct{})
	go func() {
		consumer.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		consumer.options.Logger.Debugf("all workers stopped")
	case <-time.After(30 * time.Second):
		consumer.options.Logger.Warnf("timeout waiting for workers during close")
	}

	// 优化 16：最后关闭 channel manager
	_ = consumer.chanManager.Close()

	// 通知连接管理器
	select {
	case consumer.closeConnectionToManagerCh <- struct{}{}:
	case <-time.After(time.Second * 2):
		consumer.options.Logger.Warnf("timeout notifying connection manager")
	}

	consumer.options.Logger.Infof("consumer closed")
}

func (consumer *Consumer) getIsClosed() bool {
	consumer.isClosedMux.RLock()
	defer consumer.isClosedMux.RUnlock()
	return consumer.isClosed
}
