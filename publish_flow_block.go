package rabbitmq

import (
	"sync/atomic"

	amqp "github.com/rabbitmq/amqp091-go"
)

func (publisher *Publisher) startNotifyFlowHandler() {
	notifyFlowChan := publisher.chanManager.NotifyFlowSafe(make(chan bool, 1))
	atomic.StoreInt32(&publisher.disablePublishDueToFlow, 0)

	for {
		select {
		case <-publisher.ctx.Done():
			return
		case ok, open := <-notifyFlowChan:
			if !open {
				return
			}
			if ok {
				publisher.options.Logger.Warnf("pausing publishing due to flow request from server")
				atomic.StoreInt32(&publisher.disablePublishDueToFlow, 1)
			} else {
				atomic.StoreInt32(&publisher.disablePublishDueToFlow, 0)
				publisher.options.Logger.Warnf("resuming publishing due to flow request from server")
			}
		}
	}
}

func (publisher *Publisher) startNotifyBlockedHandler() {
	blockings := publisher.connManager.NotifyBlockedSafe(make(chan amqp.Blocking, 1))
	atomic.StoreInt32(&publisher.disablePublishDueToBlocked, 0)

	for {
		select {
		case b, ok := <-blockings:
			if !ok {
				return
			}
			if b.Active {
				publisher.options.Logger.Warnf("pausing publishing due to TCP blocking from server")
				atomic.StoreInt32(&publisher.disablePublishDueToBlocked, 1)
			} else {
				atomic.StoreInt32(&publisher.disablePublishDueToBlocked, 0)
				publisher.options.Logger.Warnf("resuming publishing due to TCP blocking from server")
			}
		case <-publisher.ctx.Done():
			return
		}
	}
}
