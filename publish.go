package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Bifang-Bird/go-rabbitmq/internal/channelmanager"
	"github.com/Bifang-Bird/go-rabbitmq/internal/connectionmanager"
	amqp "github.com/rabbitmq/amqp091-go"
)

// DeliveryMode. Transient means higher throughput but messages will not be
// restored on broker restart.
const (
	Transient  uint8 = amqp.Transient
	Persistent uint8 = amqp.Persistent
)

// Return captures a flattened struct of fields returned by the server
type Return struct {
	amqp.Return
}

// Confirmation notifies the acknowledgment or negative acknowledgement of a publishing
type Confirmation struct {
	amqp.Confirmation
	ReconnectionCount int
}

// Publisher allows you to publish messages safely across an open connection
type Publisher struct {
	chanManager                *channelmanager.ChannelManager
	connManager                *connectionmanager.ConnectionManager
	reconnectErrCh             <-chan error
	closeConnectionToManagerCh chan<- struct{}

	disablePublishDueToFlow    int32 // atomic bool
	disablePublishDueToBlocked int32 // atomic bool

	handlerMux           sync.RWMutex
	notifyReturnHandler  func(r Return)
	notifyPublishHandler func(p Confirmation)

	options PublisherOptions

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	isClosed int32
}

type PublisherConfirmation []*amqp.DeferredConfirmation

// NewPublisher returns a new publisher with an open channel to the cluster.
func NewPublisher(conn *Conn, optionFuncs ...func(*PublisherOptions)) (*Publisher, error) {
	defaultOptions := getDefaultPublisherOptions()
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
	ctx, cancel := context.WithCancel(context.Background())

	publisher := &Publisher{
		chanManager:                chanManager,
		connManager:                conn.connectionManager,
		reconnectErrCh:             reconnectErrCh,
		closeConnectionToManagerCh: closeCh,
		options:                    *options,
		ctx:                        ctx,
		cancel:                     cancel,
	}

	err = publisher.startup()
	if err != nil {
		cancel()
		return nil, err
	}

	if options.ConfirmMode {
		publisher.NotifyPublish(func(_ Confirmation) {
			// set a blank handler to set the channel in confirm mode
		})
	}

	go func() {
		for {
			select {
			case <-publisher.ctx.Done():
				return
			case err, ok := <-publisher.reconnectErrCh:
				if !ok {
					return
				}
				publisher.options.Logger.Infof("successful publisher recovery from: %v", err)
				if err := publisher.startup(); err != nil {
					publisher.options.Logger.Errorf("error on startup for publisher after recovery: %v", err)
					continue
				}
				publisher.startReturnHandler()
				publisher.startPublishHandler()
			}
		}
	}()

	return publisher, nil
}

func (publisher *Publisher) startup() error {
	err := declareExchange(publisher.chanManager, publisher.options.ExchangeOptions)
	if err != nil {
		return fmt.Errorf("declare exchange failed: %w", err)
	}

	publisher.startNotifyFlowHandler()
	publisher.startNotifyBlockedHandler()

	return nil
}

// Publish publishes the provided data to the given routing keys over the connection.
func (publisher *Publisher) Publish(
	data []byte,
	routingKeys []string,
	optionFuncs ...func(*PublishOptions),
) error {
	return publisher.PublishWithContext(context.Background(), data, routingKeys, optionFuncs...)
}

// PublishWithContext publishes the provided data to the given routing keys over the connection.
func (publisher *Publisher) PublishWithContext(
	ctx context.Context,
	data []byte,
	routingKeys []string,
	optionFuncs ...func(*PublishOptions),
) error {
	if atomic.LoadInt32(&publisher.disablePublishDueToFlow) == 1 {
		return fmt.Errorf("publishing blocked due to high flow on the server")
	}
	if atomic.LoadInt32(&publisher.disablePublishDueToBlocked) == 1 {
		return fmt.Errorf("publishing blocked due to TCP block on the server")
	}

	options := &PublishOptions{}
	for _, optionFunc := range optionFuncs {
		optionFunc(options)
	}
	if options.DeliveryMode == 0 {
		options.DeliveryMode = Transient
	}

	for _, routingKey := range routingKeys {
		message := amqp.Publishing{
			ContentType:     options.ContentType,
			DeliveryMode:    options.DeliveryMode,
			Body:            data,
			Headers:         tableToAMQPTable(options.Headers),
			Expiration:      options.Expiration,
			ContentEncoding: options.ContentEncoding,
			Priority:        options.Priority,
			CorrelationId:   options.CorrelationID,
			ReplyTo:         options.ReplyTo,
			MessageId:       options.MessageID,
			Timestamp:       options.Timestamp,
			Type:            options.Type,
			UserId:          options.UserID,
			AppId:           options.AppID,
		}

		err := publisher.chanManager.PublishWithContextSafe(
			ctx,
			options.Exchange,
			routingKey,
			options.Mandatory,
			options.Immediate,
			message,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

// PublishWithDeferredConfirmWithContext ...
func (publisher *Publisher) PublishWithDeferredConfirmWithContext(
	ctx context.Context,
	data []byte,
	routingKeys []string,
	optionFuncs ...func(*PublishOptions),
) (PublisherConfirmation, error) {
	if atomic.LoadInt32(&publisher.disablePublishDueToFlow) == 1 {
		return nil, fmt.Errorf("publishing blocked due to high flow on the server")
	}
	if atomic.LoadInt32(&publisher.disablePublishDueToBlocked) == 1 {
		return nil, fmt.Errorf("publishing blocked due to TCP block on the server")
	}

	options := &PublishOptions{}
	for _, optionFunc := range optionFuncs {
		optionFunc(options)
	}
	if options.DeliveryMode == 0 {
		options.DeliveryMode = Transient
	}

	var deferredConfirmations []*amqp.DeferredConfirmation

	for _, routingKey := range routingKeys {
		message := amqp.Publishing{
			ContentType:     options.ContentType,
			DeliveryMode:    options.DeliveryMode,
			Body:            data,
			Headers:         tableToAMQPTable(options.Headers),
			Expiration:      options.Expiration,
			ContentEncoding: options.ContentEncoding,
			Priority:        options.Priority,
			CorrelationId:   options.CorrelationID,
			ReplyTo:         options.ReplyTo,
			MessageId:       options.MessageID,
			Timestamp:       options.Timestamp,
			Type:            options.Type,
			UserId:          options.UserID,
			AppId:           options.AppID,
		}

		conf, err := publisher.chanManager.PublishWithDeferredConfirmWithContextSafe(
			ctx,
			options.Exchange,
			routingKey,
			options.Mandatory,
			options.Immediate,
			message,
		)
		if err != nil {
			return nil, err
		}
		deferredConfirmations = append(deferredConfirmations, conf)
	}
	return deferredConfirmations, nil
}

// Close closes the publisher and releases resources
func (publisher *Publisher) Close() {
	if !atomic.CompareAndSwapInt32(&publisher.isClosed, 0, 1) {
		return
	}

	publisher.options.Logger.Infof("closing publisher...")
	publisher.cancel()

	err := publisher.chanManager.Close()
	if err != nil {
		publisher.options.Logger.Warnf("error while closing the channel manager: %v", err)
	}

	publisher.wg.Wait()

	select {
	case publisher.closeConnectionToManagerCh <- struct{}{}:
	case <-time.After(time.Second * 2):
	}
}

// NotifyReturn registers a listener for basic.return methods.
func (publisher *Publisher) NotifyReturn(handler func(r Return)) {
	publisher.handlerMux.Lock()
	start := publisher.notifyReturnHandler == nil
	publisher.notifyReturnHandler = handler
	publisher.handlerMux.Unlock()

	if start {
		publisher.startReturnHandler()
	}
}

// NotifyPublish registers a listener for publish confirmations
func (publisher *Publisher) NotifyPublish(handler func(p Confirmation)) {
	publisher.handlerMux.Lock()
	shouldStart := publisher.notifyPublishHandler == nil
	publisher.notifyPublishHandler = handler
	publisher.handlerMux.Unlock()

	if shouldStart {
		publisher.startPublishHandler()
	}
}

func (publisher *Publisher) startReturnHandler() {
	publisher.handlerMux.RLock()
	if publisher.notifyReturnHandler == nil {
		publisher.handlerMux.RUnlock()
		return
	}
	publisher.handlerMux.RUnlock()

	publisher.wg.Add(1)
	go func() {
		defer publisher.wg.Done()
		returns := publisher.chanManager.NotifyReturnSafe(make(chan amqp.Return, 1))
		for {
			select {
			case <-publisher.ctx.Done():
				return
			case ret, ok := <-returns:
				if !ok {
					return
				}
				publisher.handlerMux.RLock()
				handler := publisher.notifyReturnHandler
				publisher.handlerMux.RUnlock()
				if handler != nil {
					go handler(Return{ret})
				}
			}
		}
	}()
}

func (publisher *Publisher) startPublishHandler() {
	publisher.handlerMux.RLock()
	if publisher.notifyPublishHandler == nil {
		publisher.handlerMux.RUnlock()
		return
	}
	publisher.handlerMux.RUnlock()

	publisher.chanManager.ConfirmSafe(false)

	publisher.wg.Add(1)
	go func() {
		defer publisher.wg.Done()
		confirmationCh := publisher.chanManager.NotifyPublishSafe(make(chan amqp.Confirmation, 1))
		for {
			select {
			case <-publisher.ctx.Done():
				return
			case conf, ok := <-confirmationCh:
				if !ok {
					return
				}
				publisher.handlerMux.RLock()
				handler := publisher.notifyPublishHandler
				publisher.handlerMux.RUnlock()
				if handler != nil {
					go handler(Confirmation{
						Confirmation:      conf,
						ReconnectionCount: int(publisher.chanManager.GetReconnectionCount()),
					})
				}
			}
		}
	}()
}
