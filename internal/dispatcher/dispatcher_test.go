package dispatcher

import (
	"errors"
	"testing"
	"time"
)

func TestNewDispatcher(t *testing.T) {
	d := NewDispatcher()
	if d.subscribers == nil {
		t.Error("Dispatcher subscribers map is nil")
	}
}

func TestAddSubscriber(t *testing.T) {
	d := NewDispatcher()
	notifyChan, closeCh := d.AddSubscriber()
	if notifyChan == nil {
		t.Error("notifyChan is nil")
	}
	if closeCh == nil {
		t.Error("closeCh is nil")
	}
	if len(d.subscribers) != 1 {
		t.Errorf("expected 1 subscriber, got %d", len(d.subscribers))
	}
}

func TestCloseSubscriber(t *testing.T) {
	d := NewDispatcher()
	_, closeCh := d.AddSubscriber()
	close(closeCh)

	// Give the goroutine time to remove the subscriber
	for i := 0; i < 10; i++ {
		d.subscribersMux.RLock()
		l := len(d.subscribers)
		d.subscribersMux.RUnlock()
		if l == 0 {
			break
		}
		time.Sleep(time.Millisecond * 10)
	}

	d.subscribersMux.RLock()
	defer d.subscribersMux.RUnlock()
	if len(d.subscribers) != 0 {
		t.Error("Dispatcher subscribers length is not 0 after closing closeCh")
	}
}

func TestDispatch(t *testing.T) {
	d := NewDispatcher()
	notifyChan, _ := d.AddSubscriber()

	errToSend := errors.New("test error")
	d.Dispatch(errToSend)

	select {
	case err := <-notifyChan:
		if err != errToSend {
			t.Errorf("expected %v, got %v", errToSend, err)
		}
	case <-time.After(time.Millisecond * 100):
		t.Error("timed out waiting for dispatch")
	}
}

func TestDispatchNonBlocking(t *testing.T) {
	d := NewDispatcher()
	// Subscriber 1: buffer will be filled
	notifyChan1, _ := d.AddSubscriber()
	// Subscriber 2: will read normally
	notifyChan2, _ := d.AddSubscriber()

	err1 := errors.New("error 1")
	err2 := errors.New("error 2")

	// Fill the buffer of subscriber 1 (cap is 1)
	d.Dispatch(err1)
	// This second dispatch should skip subscriber 1 but succeed for subscriber 2
	d.Dispatch(err2)

	// Subscriber 1 should have err1
	select {
	case err := <-notifyChan1:
		if err != err1 {
			t.Errorf("expected %v, got %v", err1, err)
		}
	default:
		t.Error("expected error 1 in notifyChan1")
	}

	// Subscriber 2 should have at least one of the errors (likely err2 if err1 was processed fast, or err1)
	// The key is that the system didn't block.
	select {
	case <-notifyChan2:
		// Success
	case <-time.After(time.Millisecond * 100):
		t.Error("subscriber 2 was blocked or did not receive message")
	}
}
