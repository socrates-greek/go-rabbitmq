package rabbitmq

import (
	"context" // Add context import
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

	// Add context for managing goroutines lifecycle
	consumerCtx    context.Context
	consumerCancel context.CancelFunc
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

	// Initialize context here
	consumerCtx, consumerCancel := context.WithCancel(context.Background())

	consumer := &Consumer{
		chanManager:                chanManager,
		reconnectErrCh:             reconnectErrCh,
		closeConnectionToManagerCh: closeCh,
		options:                    *options,
		isClosedMux:                &sync.RWMutex{},
		isClosed:                   false,
		batchAckChan:               make(chan uint64, options.BatchSize*2), // 缓冲大小
		consumerCtx:                consumerCtx,
		consumerCancel:             consumerCancel,
	}

	// 启动逻辑
	if err := consumer.start(handler); err != nil {
		// If initial start fails, ensure context is cancelled
		consumer.consumerCancel()
		return nil, err
	}

	// 重连监听
	go func() {
		for err := range consumer.reconnectErrCh {
			consumer.options.Logger.Infof("successful consumer recovery from: %v", err)
			if err := consumer.start(handler); err != nil {
				consumer.options.Logger.Errorf("critical: consumer recovery failed: %v", err)
				// If recovery fails, consider stopping the consumer entirely
				consumer.Close() // This will set isClosed and cancel the context
				return
			}
		}
		consumer.options.Logger.Infof("reconnectErrCh closed, consumer reconnect listener exiting.")
	}()

	return consumer, nil
}

func (consumer *Consumer) start(handler Handler) error {
	// If this is a reconnect, stop previous goroutines before starting new ones
	// This handles the case where `start` is called multiple times (e.g., on reconnect)
	if consumer.consumerCancel != nil {
		consumer.consumerCancel() // Cancel the old context
		consumer.wg.Wait()        // Wait for old goroutines to finish
	}

	// Create a new context for the new set of goroutines
	consumer.consumerCtx, consumer.consumerCancel = context.WithCancel(context.Background())

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
		go consumer.batchAckCoordinator(consumer.consumerCtx) // Pass the new context
	}

	// 4. 启动并发工作协程
	for i := 0; i < consumer.options.Concurrency; i++ {
		consumer.wg.Add(1)
		go consumer.handlerWorker(consumer.consumerCtx, msgs, handler) // Pass the new context
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
func (consumer *Consumer) batchAckCoordinator(ctx context.Context) { // Accept context
	defer consumer.wg.Done()

	var lastTag uint64
	var count int
	ticker := time.NewTicker(consumer.options.BatchTimeout)
	defer ticker.Stop()

	flush := func() {
		if count > 0 {
			// Always get the latest channel from the manager
			ch := consumer.chanManager.GetChannel()
			if ch == nil {
				consumer.options.Logger.Errorf("batch ack failed: channel is nil during flush, tags will be lost: %d messages, max tag: %d", count, lastTag)
				// Consider what to do with unacked messages here. Requeueing might be an option
				// but requires more complex state management. For now, log and discard.
				count = 0 // Reset count even if channel is nil to prevent repeated errors
				return
			}
			err := ch.Ack(lastTag, true) // Use the latest channel
			if err != nil {
				consumer.options.Logger.Errorf("batch ack failed: %v", err)
			} else {
				consumer.options.Logger.Debugf("batch acked %d messages, max tag: %d", count, lastTag)
			}
			count = 0
		}
	}

	for {
		select {
		case <-ctx.Done(): // Listen for context cancellation
			consumer.options.Logger.Infof("batchAckCoordinator context cancelled, flushing remaining acks.")
			flush()
			return
		case tag, ok := <-consumer.batchAckChan:
			if !ok {
				// This case should ideally be handled by ctx.Done() if batchAckChan is closed
				// due to consumer.Close(). If it's closed for other reasons, it means no more tags.
				consumer.options.Logger.Infof("batchAckChan closed, flushing remaining acks.")
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

func (consumer *Consumer) handlerWorker(ctx context.Context, msgs <-chan amqp.Delivery, handler Handler) { // Accept context
	defer consumer.wg.Done()

	for {
		select {
		case <-ctx.Done(): // Listen for context cancellation
			consumer.options.Logger.Infof("handlerWorker context cancelled, exiting.")
			return
		case msg, ok := <-msgs:
			if !ok { // msgs channel closed (e.g., due to chanManager.Close())
				consumer.options.Logger.Infof("handlerWorker msgs channel closed, exiting.")
				return
			}

			// This check might be redundant with context.Done() but keep for safety
			if consumer.getIsClosed() {
				consumer.options.Logger.Infof("handlerWorker consumer is closed, exiting.")
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
					select {
					case consumer.batchAckChan <- msg.DeliveryTag:
						// Successfully sent tag
					case <-ctx.Done():
						// Context cancelled while trying to send tag, nack the message
						consumer.options.Logger.Warnf("Context cancelled while sending tag %d to batchAckChan, nacking message and requeueing.", msg.DeliveryTag)
						_ = msg.Nack(false, true) // Requeue the message
					}
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

	// 1. Cancel the context to signal all goroutines (batchAckCoordinator, handlerWorker) to stop
	if consumer.consumerCancel != nil {
		consumer.consumerCancel()
	}

	// 2. Close the underlying AMQP channel. This will cause the `msgs` channel in handlerWorker to close.
	// This should happen after signaling goroutines to stop, so they can gracefully exit.
	// The `chanManager.Close()` will also cause the `reconnectErrCh` to close eventually,
	// stopping the reconnect listener goroutine.
	err := consumer.chanManager.Close()
	if err != nil {
		consumer.options.Logger.Warnf("error while closing the channel manager: %v", err)
	}

	// 3. 等待所有工作协程完成
	consumer.wg.Wait()
	consumer.options.Logger.Infof("All consumer goroutines stopped.")

	// 4. 通知管理器关闭
	// This should be the last step after all consumer-specific goroutines are done.
	select {
	case consumer.closeConnectionToManagerCh <- struct{}{}:
		consumer.options.Logger.Infof("Signaled connection manager to close.")
	case <-time.After(5 * time.Second): // Add a timeout to prevent blocking indefinitely
		consumer.options.Logger.Warnf("Timeout waiting to signal connection manager to close.")
	}
}

func (consumer *Consumer) getIsClosed() bool {
	consumer.isClosedMux.RLock()
	defer consumer.isClosedMux.RUnlock()
	return consumer.isClosed
}
