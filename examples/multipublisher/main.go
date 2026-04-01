package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	rabbitmq "github.com/Bifang-Bird/go-rabbitmq"
)

const (
	totalMessages = 2000000 // 总消息数量
	concurrency   = 50      // 并发协程数
)

func main() {
	conn, err := rabbitmq.NewConn(
		"amqp://guest:guest@localhost:5672",
		rabbitmq.WithConnectionOptionsLogging,
	)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	publisher, err := rabbitmq.NewPublisher(
		conn,
		rabbitmq.WithPublisherOptionsLogging,
		rabbitmq.WithPublisherOptionsExchangeName("events"),
		rabbitmq.WithPublisherOptionsExchangeDeclare,
	)
	if err != nil {
		log.Fatal(err)
	}
	defer publisher.Close()

	// 优雅退出信号
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	fmt.Printf("开始大批量发布测试:\n- 总消息数: %d\n- 并发协程数: %d\n", totalMessages, concurrency)

	startTime := time.Now()
	var sentCount int64 = 0
	var failCount int64 = 0

	var wg sync.WaitGroup
	sem := make(chan struct{}, concurrency)

	// 用于通知协程停止的标志
	stopFlag := int32(0)

	// 监听信号，设置停止标志
	go func() {
		<-sigs
		fmt.Println("\n收到中断信号，正在停止接收新任务...")
		atomic.StoreInt32(&stopFlag, 1)
	}()

	for i := 0; i < totalMessages; i++ {
		// 检查是否收到退出信号
		if atomic.LoadInt32(&stopFlag) == 1 {
			break
		}

		wg.Add(1)
		sem <- struct{}{}

		go func(msgId int) {
			defer func() {
				<-sem
				wg.Done()
			}()

			// 再次检查，防止在队列中积压时仍发送
			if atomic.LoadInt32(&stopFlag) == 1 {
				return
			}

			body := []byte(fmt.Sprintf("batch-test-message-%d", msgId))
			// 修改点：固定路由键为 "my_routing_key"
			routingKey := "my_routing_key"

			err := publisher.PublishWithContext(
				context.Background(),
				body,
				[]string{routingKey},
				rabbitmq.WithPublishOptionsContentType("application/json"),
				rabbitmq.WithPublishOptionsExchange("events"),
			)

			if err != nil {
				newFail := atomic.AddInt64(&failCount, 1)
				if newFail <= 5 {
					log.Printf("发送失败 (ID:%d): %v", msgId, err)
				}
			} else {
				newSent := atomic.AddInt64(&sentCount, 1)
				if newSent%10000 == 0 {
					fmt.Printf("已发送：%d / %d (失败：%d)\n", newSent, totalMessages, atomic.LoadInt64(&failCount))
				}
			}
		}(i)

	}

	// 等待所有正在运行的协程完成
	// 不需要单独的 goroutine 和 channel 来处理中断，因为 wg.Wait() 会阻塞直到完成
	// 如果需要在等待过程中响应二次中断强制退出，可以加一个 select，但通常优雅关闭只需等待
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	<-done

	duration := time.Since(startTime)
	finalSent := atomic.LoadInt64(&sentCount)
	finalFail := atomic.LoadInt64(&failCount)

	fmt.Println("\n========== 测试结果 ==========")
	fmt.Printf("总耗时：%.2f 秒\n", duration.Seconds())
	fmt.Printf("成功发送：%d\n", finalSent)
	fmt.Printf("发送失败：%d\n", finalFail)
	if duration > 0 {
		qps := float64(finalSent) / duration.Seconds()
		fmt.Printf("平均吞吐量：%.2f msgs/sec\n", qps)
	}
	fmt.Println("============================")
}
