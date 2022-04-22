package main

import (
	"fmt"
	"time"
)

/// ================== timer demo

// output：
// time out
// demo1: 设置超时，并通过channel通知超时
func demo1(){
	timer := time.NewTimer(1 * time.Second)
	fmt.Println(time.Now().Second())
	select {
	case <-timer.C: // 1s 后管道将被投递数据通知已经超时
		fmt.Println("time out")

	}
	fmt.Println(time.Now().Second())
}

// output：
// after sleep
// cb after timeout
// demo2: 设置超时，重置超时，并设置超时后的回调
func demo2(){

	// 1. 设置超时回调
	timer := time.AfterFunc(1 * time.Second, func(){
		fmt.Println("cb after timeout")
	})
	timer.Reset(3 * time.Second)	// 2. 重置超时

	time.Sleep(2*time.Second)
	fmt.Println("after sleep")

	// 3. 注：已经设置回调了，timer.C将失去作用

	time.Sleep(2*time.Second)
	//<-timer.C

}

// output:
// stop
// demo3: 终止timer， stop timer 之后，timer.C不会有信息被投递，所以需要用额外的chan通知
func demo3(){
	timer := time.NewTimer(1 * time.Second)
	var flag chan bool
	flag = make(chan bool)

	go func(){
		time.Sleep(500*time.Millisecond)
		// 2.  stop 成功返回true，已经stop或失败返回false
		tick := timer.Stop()
		flag <- tick	// 1. 通知timer已经stop
	}()

	select {
	// Stop不会关闭chan t.C，以避免从该通道的读取不正确的成功
	case <-timer.C: // 1s 后管道将被投递数据通知已经超时
		fmt.Println("time out")
	case <-flag:
		fmt.Println("stop")
	}
}

/// ==================== ticker demo

// output:
// tick...
// tick...
// tick...
// time out
// demo4: 区别 timer 和 ticker，ticker 为循环的定时器，timer只触发一次
func demo4(){
	ticker := time.NewTicker( 1 * time.Second )
	timer := time.NewTimer( 3 * time.Second )

END:
	for true{
		select {
		case <- ticker.C:
			fmt.Println("tick...")
		case <- timer.C:
			fmt.Println("time out")
			break END
		}
	}

}

func main(){
	//demo1()
	demo4()

}