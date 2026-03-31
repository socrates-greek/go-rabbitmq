package connectionmanager

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Bifang-Bird/go-rabbitmq/internal/dispatcher"
	"github.com/Bifang-Bird/go-rabbitmq/internal/logger"
	amqp "github.com/rabbitmq/amqp091-go"
)

// ConnectionManager manages the AMQP connection and its lifecycle
type ConnectionManager struct {
	logger            logger.Logger
	url               string
	connection        atomic.Value // holds *amqp.Connection
	amqpConfig        amqp.Config
	reconnectMux      sync.Mutex // prevent multiple simultaneous reconnections
	ReconnectInterval time.Duration
	reconnectionCount uint64
	dispatcher        *dispatcher.Dispatcher
	isClosed          int32 // atomic boolean to indicate if the manager is closed
}

// NewConnectionManager creates a new connection manager
func NewConnectionManager(url string, conf amqp.Config, log logger.Logger, reconnectInterval time.Duration) (*ConnectionManager, error) {
	conn, err := amqp.DialConfig(url, amqp.Config(conf))
	if err != nil {
		return nil, err
	}
	connManager := &ConnectionManager{
		logger:            log,
		url:               url,
		amqpConfig:        conf,
		ReconnectInterval: reconnectInterval,
		dispatcher:        dispatcher.NewDispatcher(),
	}
	connManager.connection.Store(conn)
	go connManager.startNotifyClose()
	return connManager, nil
}

// Close safely closes the connection
func (connManager *ConnectionManager) Close() error {
	if !atomic.CompareAndSwapInt32(&connManager.isClosed, 0, 1) {
		return nil // Already closed
	}
	connManager.logger.Infof("closing connection manager...")
	conn := connManager.connection.Load().(*amqp.Connection)
	return conn.Close()
}

// NotifyReconnect adds a new subscriber for reconnection events
func (connManager *ConnectionManager) NotifyReconnect() (<-chan error, chan<- struct{}) {
	return connManager.dispatcher.AddSubscriber()
}

// CheckoutConnection returns the current active connection
func (connManager *ConnectionManager) CheckoutConnection() *amqp.Connection {
	return connManager.connection.Load().(*amqp.Connection)
}

// CheckinConnection is kept for API compatibility but is a no-op with atomic.Value
func (connManager *ConnectionManager) CheckinConnection() {}

func (connManager *ConnectionManager) startNotifyClose() {
	conn := connManager.connection.Load().(*amqp.Connection)
	notifyCloseChan := conn.NotifyClose(make(chan *amqp.Error, 1))

	err, ok := <-notifyCloseChan
	if !ok { // Channel closed, manager is likely shutting down
		return
	}

	if atomic.LoadInt32(&connManager.isClosed) == 1 {
		connManager.logger.Infof("Connection manager is closed, not attempting to reconnect.")
		return
	}

	if err != nil {
		connManager.logger.Errorf("attempting to reconnect to amqp server after connection close with error: %v", err)
		connManager.reconnectLoop()
		connManager.logger.Warnf("successfully reconnected to amqp server")
		connManager.dispatcher.Dispatch(err) // Dispatch the error after successful reconnection
	} else {
		connManager.logger.Infof("amqp connection closed gracefully")
	}
}

// GetReconnectionCount returns the total number of reconnections
func (connManager *ConnectionManager) GetReconnectionCount() uint64 {
	return atomic.LoadUint64(&connManager.reconnectionCount)
}

func (connManager *ConnectionManager) reconnectLoop() {
	for {
		if atomic.LoadInt32(&connManager.isClosed) == 1 {
			return // Manager is closed, stop reconnecting
		}
		connManager.logger.Infof("waiting %s seconds to attempt to reconnect to amqp server", connManager.ReconnectInterval)
		time.Sleep(connManager.ReconnectInterval)
		err := connManager.reconnect()
		if err != nil {
			connManager.logger.Errorf("error reconnecting to amqp server: %v", err)
		} else {
			atomic.AddUint64(&connManager.reconnectionCount, 1)
			go connManager.startNotifyClose() // Start listening on the new connection
			return
		}
	}
}

func (connManager *ConnectionManager) reconnect() error {
	connManager.reconnectMux.Lock()
	defer connManager.reconnectMux.Unlock()

	// Double-check if already closed while waiting for lock
	if atomic.LoadInt32(&connManager.isClosed) == 1 {
		return errors.New("connection manager is closed, cannot reconnect")
	}

	newConn, err := amqp.DialConfig(connManager.url, amqp.Config(connManager.amqpConfig))
	if err != nil {
		return err
	}

	oldConn := connManager.connection.Load().(*amqp.Connection)
	// Best effort close the old connection. Errors here are logged but not returned
	// as the primary goal is to establish a new connection.
	if err := oldConn.Close(); err != nil {
		connManager.logger.Warnf("error closing old connection during reconnect: %v", err)
	}

	connManager.connection.Store(newConn)
	return nil
}
