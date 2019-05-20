package main

import (
	"log"
	"math/rand"
	"runtime"

	"github.com/adjust/redismq"
)

func main() {
	runtime.GOMAXPROCS(5)
	server := redismq.NewServer("localhost", "6379", "", 9, "9999", false)
	server.Start()
	queue := redismq.CreateQueue("localhost", "6379", "", 9, "example", false)
	go write(queue)
	go read(queue, "1")
	go read(queue, "2")
	go read(queue, "3")
	select {}
}

func randomString(l int) string {
	bytes := make([]byte, l)
	for i := 0; i < l; i++ {
		bytes[i] = byte(randInt(65, 90))
	}
	return string(bytes)
}

func randInt(min int, max int) int {
	return min + rand.Intn(max-min)
}

func write(queue *redismq.Queue) {
	payload := randomString(1024 * 1) //adjust for size
	for {
		queue.Put(payload)
	}
}

func read(queue *redismq.Queue, prefix string) {
	consumer, err := queue.AddConsumer("testconsumer" + prefix)
	if err != nil {
		panic(err)
	}
	consumer.ResetWorking()
	for {
		p, err := consumer.Get()
		if err != nil {
			log.Println(err)
			continue
		}
		err = p.Ack()
	}
}
