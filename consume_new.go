package rabbitmq

import (
	"context"
	"errors"
	"fmt"
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

	wg             sync.WaitGroup
	consumerCtx    context.Context
	consumerCancel context.CancelFunc

	// ------------- 修复 1：增加 channel 锁，解决并发非安全 -------------
	chMux sync.Mutex
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
	if consumer.consumerCancel != nil {
		consumer.consumerCancel()
		consumer.wg.Wait()
	}

	consumer.consumerCtx, consumer.consumerCancel = context.WithCancel(context.Background())

	if err := consumer.setupTopology(); err != nil {
		return err
	}

	// ------------- 修复 2：每次重连都获取新 channel，杜绝野指针 -------------
	ch := consumer.chanManager.GetChannel()

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

	sessionBatchAckChan := make(chan uint64, consumer.options.BatchSize*2)

	if consumer.options.EnableBatchAck && !consumer.options.RabbitConsumerOptions.AutoAck {
		consumer.wg.Add(1)
		go consumer.batchAckCoordinator(consumer.consumerCtx, ch, sessionBatchAckChan)
	}

	for i := 0; i < consumer.options.Concurrency; i++ {
		consumer.wg.Add(1)
		go consumer.handlerWorker(consumer.consumerCtx, sessionBatchAckChan, msgs, handler)
	}

	consumer.options.Logger.Infof("Consumer started with %d workers", consumer.options.Concurrency)
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

// ------------- 修复 3：批量ACK 加锁，保证 channel 安全 -------------
func (consumer *Consumer) batchAckCoordinator(ctx context.Context, ch *amqp.Channel, batchAckChan <-chan uint64) {
	defer consumer.wg.Done()

	expectedTag := uint64(1)
	var lastAckedTag uint64 = 0
	completedTags := make(map[uint64]bool)

	ticker := time.NewTicker(consumer.options.BatchTimeout)
	defer ticker.Stop()

	flush := func() {
		highestSafeTag := expectedTag - 1
		if highestSafeTag > lastAckedTag {
			// -------- 加锁！防止并发调用 Ack --------
			consumer.chMux.Lock()
			defer consumer.chMux.Unlock()

			err := ch.Ack(highestSafeTag, true)
			if err != nil {
				consumer.options.Logger.Debugf("batch ack safe_tag %d failed: %v", highestSafeTag, err)
			} else {
				lastAckedTag = highestSafeTag
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

func (consumer *Consumer) handlerWorker(ctx context.Context, batchAckChan chan<- uint64, msgs <-chan amqp.Delivery, handler Handler) {
	defer consumer.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return

		case msg, ok := <-msgs:
			if !ok || consumer.getIsClosed() {
				return
			}

			action := handler(Delivery{msg})

			if consumer.options.RabbitConsumerOptions.AutoAck {
				continue
			}

			switch action {
			case Ack:
				if consumer.options.EnableBatchAck {
					// ------------- 修复 4：非阻塞发送，防止整个消费者挂起 -------------
					select {
					case batchAckChan <- msg.DeliveryTag:
					case <-ctx.Done():
						_ = msg.Nack(false, true)
					}
				} else {
					_ = msg.Ack(false)
				}

			case NackDiscard:
				_ = msg.Nack(false, false)

			case NackRequeue:
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

	_ = consumer.chanManager.Close()
	consumer.wg.Wait()

	select {
	case consumer.closeConnectionToManagerCh <- struct{}{}:
	case <-time.After(time.Second * 2):
	}
}

func (consumer *Consumer) getIsClosed() bool {
	consumer.isClosedMux.RLock()
	defer consumer.isClosedMux.RUnlock()
	return consumer.isClosed
}
