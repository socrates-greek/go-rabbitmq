package dispatcher

import (
	"sync"
)

// Dispatcher manages subscribers who want to be notified of events (like reconnections)
type Dispatcher struct {
	subscribers    map[int]dispatchSubscriber
	subscribersMux sync.RWMutex
	nextID         int
}

type dispatchSubscriber struct {
	notifyChan chan error
	closeCh    <-chan struct{}
}

// NewDispatcher creates a new event dispatcher
func NewDispatcher() *Dispatcher {
	return &Dispatcher{
		subscribers: make(map[int]dispatchSubscriber),
	}
}

// Dispatch sends an error to all subscribers in a non-blocking way
func (d *Dispatcher) Dispatch(err error) {
	d.subscribersMux.RLock()
	defer d.subscribersMux.RUnlock()

	for _, subscriber := range d.subscribers {
		select {
		case subscriber.notifyChan <- err:
		default:
			// If the subscriber's buffer is full, we skip it to avoid blocking the whole system.
			// This is safe because reconnections are periodic events.
		}
	}
}

// AddSubscriber adds a new subscriber and returns a channel for notifications and a channel to trigger removal
func (d *Dispatcher) AddSubscriber() (<-chan error, chan<- struct{}) {
	d.subscribersMux.Lock()
	defer d.subscribersMux.Unlock()

	id := d.nextID
	d.nextID++

	// Use a buffered channel to prevent blocking the dispatcher
	notifyChan := make(chan error, 1)
	closeCh := make(chan struct{})

	d.subscribers[id] = dispatchSubscriber{
		notifyChan: notifyChan,
		closeCh:    closeCh,
	}

	go func(id int, c <-chan struct{}) {
		<-c
		d.subscribersMux.Lock()
		defer d.subscribersMux.Unlock()
		if sub, ok := d.subscribers[id]; ok {
			close(sub.notifyChan)
			delete(d.subscribers, id)
		}
	}(id, closeCh)

	return notifyChan, closeCh
}
