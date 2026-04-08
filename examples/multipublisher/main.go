package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	rabbitmq "github.com/Bifang-Bird/go-rabbitmq"
)

const (
	totalMessages = 300000 // 总消息数量
	concurrency   = 50     // 并发协程数
)

// return "amqp://Simba_admin:Simba_123@amqp-5kggpo9g-nj-public-d.amqp.tencenttdmq.com:5672"
// amqp://Simba_admin:Simba_123@192.168.20.201:30672
func main() {
	conn, err := rabbitmq.NewConn(
		//"amqp://guest:guest@localhost:5672",
		"amqp://Simba_admin:Simba_123@amqp-5kggpo9g-nj-public-d.amqp.tencenttdmq.com:5672",
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
	defer signal.Stop(sigs) // 防止信号处理器泄露

	fmt.Printf("开始大批量发布测试:\n- 总消息数: %d\n- 并发协程数: %d\n", totalMessages, concurrency)

	startTime := time.Now()
	var sentCount int64 = 0
	var failCount int64 = 0

	// 记录初始状态
	initialGoroutines := runtime.NumGoroutine()
	var initialMem runtime.MemStats
	runtime.ReadMemStats(&initialMem)
	initialAlloc := initialMem.Alloc
	initialTotalAlloc := initialMem.TotalAlloc

	var wg sync.WaitGroup
	sem := make(chan struct{}, concurrency)

	// 用于通知协程停止的标志
	stopFlag := int32(0)

	// 监听信号，设置停止标志（使用 context 替代，避免 goroutine 泄露）
	stopCtx, stopCancel := context.WithCancel(context.Background())
	defer stopCancel()

	go func() {
		select {
		case sig := <-sigs:
			fmt.Printf("\n收到中断信号 (%v)，正在停止接收新任务...\n", sig)
			atomic.StoreInt32(&stopFlag, 1)
			stopCancel() // 通知主循环停止
		case <-stopCtx.Done():
			// 正常退出，goroutine 自动结束
			return
		}
	}()

	for i := 0; i < totalMessages; i++ {
		// 检查是否收到退出信号
		if atomic.LoadInt32(&stopFlag) == 1 {
			fmt.Println("检测到停止标志，停止发送新消息")
			break
		}

		time.Sleep(500 * time.Microsecond)

		// 使用 select 避免在停止时继续阻塞等待信号量
		select {
		case sem <- struct{}{}:
			// 成功获取信号量
		case <-stopCtx.Done():
			fmt.Println("上下文取消，停止发送")
			goto WAIT_COMPLETION
		}

		wg.Add(1)

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

			err := publisher.Publish(
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

WAIT_COMPLETION:
	// 等待所有正在运行的协程完成
	fmt.Println("\n等待所有发送任务完成...")

	// 启动一个 goroutine 来处理二次中断（强制退出）
	forceExit := make(chan struct{})
	go func() {
		<-sigs
		fmt.Println("\n收到二次中断信号，强制退出！")
		close(forceExit)
	}()

	// 使用 select 等待完成或强制退出
	waitDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(waitDone)
	}()

	select {
	case <-waitDone:
		fmt.Println("所有任务已完成")
	case <-forceExit:
		fmt.Println("强制退出，不等待剩余任务")
		os.Exit(1)
	}

	duration := time.Since(startTime)
	finalSent := atomic.LoadInt64(&sentCount)
	finalFail := atomic.LoadInt64(&failCount)

	// 收集最终状态
	finalGoroutines := runtime.NumGoroutine()
	var finalMem runtime.MemStats
	runtime.ReadMemStats(&finalMem)

	// 计算差值
	goroutineDiff := finalGoroutines - initialGoroutines
	allocDiff := float64(finalMem.Alloc-initialAlloc) / 1024 / 1024
	totalAllocDiff := float64(finalMem.TotalAlloc-initialTotalAlloc) / 1024 / 1024
	numGC := finalMem.NumGC - initialMem.NumGC

	fmt.Println("\n========== 测试结果 ==========")
	fmt.Printf("总耗时：%.2f 秒\n", duration.Seconds())
	fmt.Printf("成功发送：%d\n", finalSent)
	fmt.Printf("发送失败：%d\n", finalFail)
	if duration > 0 {
		qps := float64(finalSent) / duration.Seconds()
		fmt.Printf("平均吞吐量：%.2f msgs/sec\n", qps)
	}

	// 详细的性能和泄露检测信息
	fmt.Println("\n========== Goroutine 分析 ==========")
	fmt.Printf("初始 Goroutine 数量: %d\n", initialGoroutines)
	fmt.Printf("当前 Goroutine 数量: %d\n", finalGoroutines)
	fmt.Printf("Goroutine 变化: %+d\n", goroutineDiff)

	if goroutineDiff <= 2 {
		fmt.Println("✅ Goroutine 状态: 正常 (无明显泄露)")
	} else if goroutineDiff <= 5 {
		fmt.Println("⚠️  Goroutine 状态: 可疑 (轻微增长，建议观察)")
	} else {
		fmt.Printf("❌ Goroutine 状态: 异常 (可能存在泄露，增长了 %d 个)\n", goroutineDiff)
	}

	fmt.Println("\n========== 内存使用分析 ==========")
	fmt.Printf("初始内存分配: %.2f MB\n", float64(initialAlloc)/1024/1024)
	fmt.Printf("当前内存分配: %.2f MB\n", float64(finalMem.Alloc)/1024/1024)
	fmt.Printf("内存增量: %+.2f MB\n", allocDiff)
	fmt.Printf("测试期间总分配: %.2f MB\n", totalAllocDiff)
	fmt.Printf("GC 触发次数: %d\n", numGC)
	fmt.Printf("堆对象数量: %d\n", finalMem.HeapObjects)
	fmt.Printf("堆内存使用: %.2f MB\n", float64(finalMem.HeapAlloc)/1024/1024)
	fmt.Printf("堆内存系统: %.2f MB\n", float64(finalMem.HeapSys)/1024/1024)

	if allocDiff < 10 {
		fmt.Println("✅ 内存状态: 正常 (增量 < 10MB)")
	} else if allocDiff < 50 {
		fmt.Println("⚠️  内存状态: 关注 (增量 10-50MB)")
	} else {
		fmt.Printf("❌ 内存状态: 警告 (增量 > 50MB，可能存在泄露)\n")
	}

	// 等待观察 Goroutine 清理情况
	fmt.Println("\n========== 泄露观察 ==========")
	fmt.Println("等待 2 秒观察 Goroutine 清理...")
	time.Sleep(2 * time.Second)

	afterWaitGoroutines := runtime.NumGoroutine()
	var afterWaitMem runtime.MemStats
	runtime.ReadMemStats(&afterWaitMem)

	fmt.Printf("2秒后 Goroutine 数量: %d\n", afterWaitGoroutines)
	fmt.Printf("2秒后内存分配: %.2f MB\n", float64(afterWaitMem.Alloc)/1024/1024)

	if afterWaitGoroutines <= 2 {
		fmt.Println("✅ 泄露检测: 通过 (所有 Goroutine 已清理)")
	} else if afterWaitGoroutines <= initialGoroutines+2 {
		fmt.Printf("⚠️  泄露检测: 可接受 (剩余 %d 个 Goroutine)\n", afterWaitGoroutines)
	} else {
		fmt.Printf("❌ 泄露检测: 失败 (仍有 %d 个 Goroutine 未清理)\n", afterWaitGoroutines)
	}

	fmt.Println("\n============================")
}
