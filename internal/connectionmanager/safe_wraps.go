package connectionmanager

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

// NotifyBlockedSafe safely wraps the (*amqp.Connection).NotifyBlocked method
func (connManager *ConnectionManager) NotifyBlockedSafe(
	receiver chan amqp.Blocking,
) chan amqp.Blocking {
	conn := connManager.CheckoutConnection()
	defer connManager.CheckinConnection()

	return conn.NotifyBlocked(
		receiver,
	)
}
