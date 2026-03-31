package channelmanager

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Bifang-Bird/go-rabbitmq/internal/connectionmanager"
	"github.com/Bifang-Bird/go-rabbitmq/internal/dispatcher"
	"github.com/Bifang-Bird/go-rabbitmq/internal/logger"
	amqp "github.com/rabbitmq/amqp091-go"
)

// ChannelManager manages the lifecycle of an AMQP channel
type ChannelManager struct {
	logger            logger.Logger
	channel           atomic.Value // holds *amqp.Channel
	connManager       *connectionmanager.ConnectionManager
	reconnectMux      sync.Mutex
	reconnectInterval time.Duration
	reconnectionCount uint64
	dispatcher        *dispatcher.Dispatcher
	isClosed          int32
}

// NewChannelManager creates a new channel manager
func NewChannelManager(connManager *connectionmanager.ConnectionManager, log logger.Logger, reconnectInterval time.Duration) (*ChannelManager, error) {
	ch, err := getNewChannel(connManager)
	if err != nil {
		return nil, err
	}

	chanManager := &ChannelManager{
		logger:            log,
		connManager:       connManager,
		reconnectInterval: reconnectInterval,
		dispatcher:        dispatcher.NewDispatcher(),
	}
	chanManager.channel.Store(ch)
	go chanManager.startNotifyCancelOrClosed()
	return chanManager, nil
}

func getNewChannel(connManager *connectionmanager.ConnectionManager) (*amqp.Channel, error) {
	conn := connManager.CheckoutConnection()
	defer connManager.CheckinConnection()

	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}
	return ch, nil
}

func (chanManager *ChannelManager) startNotifyCancelOrClosed() {
	ch := chanManager.channel.Load().(*amqp.Channel)
	notifyCloseChan := ch.NotifyClose(make(chan *amqp.Error, 1))
	notifyCancelChan := ch.NotifyCancel(make(chan string, 1))

	select {
	case err, ok := <-notifyCloseChan:
		if !ok || atomic.LoadInt32(&chanManager.isClosed) == 1 {
			return
		}
		if err != nil {
			chanManager.logger.Errorf("attempting to reconnect to amqp channel after close with error: %v", err)
			chanManager.reconnectLoop()
			chanManager.logger.Warnf("successfully reconnected to amqp channel")
			chanManager.dispatcher.Dispatch(err)
		}
	case reason, ok := <-notifyCancelChan:
		if !ok || atomic.LoadInt32(&chanManager.isClosed) == 1 {
			return
		}
		chanManager.logger.Errorf("attempting to reconnect to amqp channel after cancel with error: %s", reason)
		chanManager.reconnectLoop()
		chanManager.logger.Warnf("successfully reconnected to amqp channel after cancel")
		chanManager.dispatcher.Dispatch(errors.New(reason))
	}
}

// GetChannel returns the current active channel
func (chanManager *ChannelManager) GetChannel() *amqp.Channel {
	return chanManager.channel.Load().(*amqp.Channel)
}

// GetReconnectionCount returns the number of channel reconnections
func (chanManager *ChannelManager) GetReconnectionCount() uint64 {
	return atomic.LoadUint64(&chanManager.reconnectionCount)
}

func (chanManager *ChannelManager) reconnectLoop() {
	for {
		if atomic.LoadInt32(&chanManager.isClosed) == 1 {
			return
		}
		time.Sleep(chanManager.reconnectInterval)
		err := chanManager.reconnect()
		if err != nil {
			chanManager.logger.Errorf("error reconnecting to amqp channel: %v", err)
		} else {
			atomic.AddUint64(&chanManager.reconnectionCount, 1)
			go chanManager.startNotifyCancelOrClosed()
			return
		}
	}
}

func (chanManager *ChannelManager) reconnect() error {
	chanManager.reconnectMux.Lock()
	defer chanManager.reconnectMux.Unlock()

	newChannel, err := getNewChannel(chanManager.connManager)
	if err != nil {
		return err
	}

	oldChannel := chanManager.channel.Load().(*amqp.Channel)
	_ = oldChannel.Close()

	chanManager.channel.Store(newChannel)
	return nil
}

// Close safely closes the current channel
func (chanManager *ChannelManager) Close() error {
	if !atomic.CompareAndSwapInt32(&chanManager.isClosed, 0, 1) {
		return nil
	}
	ch := chanManager.channel.Load().(*amqp.Channel)
	return ch.Close()
}

// NotifyReconnect adds a new subscriber for channel reconnection events
func (chanManager *ChannelManager) NotifyReconnect() (<-chan error, chan<- struct{}) {
	return chanManager.dispatcher.AddSubscriber()
}
