package rabbitmq

import (
	"context"
	"sync"
	"time"

	"github.com/Bifang-Bird/go-rabbitmq/internal/logger"
	"github.com/rabbitmq/amqp091-go"
)

// 2. 添加批量确认管理器
type BatchAckManager struct {
	mu          sync.Mutex
	channel     *amqp091.Channel
	pendingTags []uint64
	batchSize   int
	timeout     time.Duration
	timer       *time.Timer
	ctx         context.Context
	cancel      context.CancelFunc
	logger      logger.Logger
}

func NewBatchAckManager(ch *amqp091.Channel, batchSize int, timeout time.Duration,
	logger logger.Logger) *BatchAckManager {

	ctx, cancel := context.WithCancel(context.Background())
	mgr := &BatchAckManager{
		channel:     ch,
		pendingTags: make([]uint64, 0, batchSize),
		batchSize:   batchSize,
		timeout:     timeout,
		logger:      logger,
		ctx:         ctx,
		cancel:      cancel,
	}

	// 启动定时器
	mgr.timer = time.AfterFunc(timeout, mgr.flush)
	return mgr
}

func (b *BatchAckManager) AddTag(tag uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.pendingTags = append(b.pendingTags, tag)

	// 达到批量大小，立即确认
	if len(b.pendingTags) >= b.batchSize {
		b.flushLocked()
		return
	}

	// 重置定时器
	if b.timer != nil {
		b.timer.Reset(b.timeout)
	}
}

func (b *BatchAckManager) flush() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.flushLocked()
}

func (b *BatchAckManager) flushLocked() {
	if len(b.pendingTags) == 0 {
		return
	}

	// 确认最大的 tag，multiple=true 确认之前所有
	maxTag := b.pendingTags[len(b.pendingTags)-1]
	err := b.channel.Ack(maxTag, true)
	if err != nil {
		b.logger.Errorf("batch ack failed: %v", err)
	} else {
		b.logger.Debugf("batch acked %d messages, max tag: %d", len(b.pendingTags), maxTag)
	}

	b.pendingTags = b.pendingTags[:0]
}

func (b *BatchAckManager) Close() {
	b.cancel()
	if b.timer != nil {
		b.timer.Stop()
	}
	b.flush() // 关闭前确认剩余消息
}
