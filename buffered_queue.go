package redismq

import (
	"fmt"
	"time"
)

// BufferedQueue provides an queue with buffered writes for increased performance.
// Only one buffered queue (per name) can be started at a time.
// Before terminating the queue should be flushed using FlushBuffer() to avoid package loss
type BufferedQueue struct {
	*Queue
	BufferSize   int
	Buffer       chan *Package
	nextWrite    int64
	flushStatus  chan (chan bool)
	flushCommand chan bool
}

// CreateBufferedQueue returns BufferedQueue.
// To start writing the buffer to redis use Start().
// Optimal BufferSize seems to be around 200.
// Works like SelectBufferedQueue for existing queues
func CreateBufferedQueue(redisHost, redisPort, redisPassword string, redisDB int, name string, bufferSize int, tls bool) *BufferedQueue {
	q := CreateQueue(redisHost, redisPort, redisPassword, redisDB, name, tls)
	return &BufferedQueue{
		Queue:        q,
		BufferSize:   bufferSize,
		Buffer:       make(chan *Package, bufferSize*2),
		flushStatus:  make(chan chan bool, 1),
		flushCommand: make(chan bool, bufferSize*2),
	}
}

// SelectBufferedQueue returns a BufferedQueue if a queue with the name exists
func SelectBufferedQueue(redisHost, redisPort, redisPassword string, redisDB int, name string, bufferSize int) (queue *BufferedQueue, err error) {
	q, err := SelectQueue(redisHost, redisPort, redisPassword, redisDB, name)
	if err != nil {
		return nil, err
	}

	return &BufferedQueue{
		Queue:        q,
		BufferSize:   bufferSize,
		Buffer:       make(chan *Package, bufferSize*2),
		flushStatus:  make(chan chan bool, 1),
		flushCommand: make(chan bool, bufferSize*2),
	}, nil
}

// Start dispatches the background writer that flushes the buffer.
// If there is already a BufferedQueue running it will return an error.
func (queue *BufferedQueue) Start() error {
	queue.redisClient.SAdd(masterQueueKey(), queue.Name)
	val := queue.redisClient.Get(queueHeartbeatKey(queue.Name)).Val()
	if val == "ping" {
		return fmt.Errorf("buffered queue with this name is already started")
	}
	queue.startHeartbeat()
	queue.startWritingBufferToRedis()
	queue.startPacemaker()
	return nil
}

// Put writes the payload to the buffer
func (queue *BufferedQueue) Put(payload string) error {
	p := &Package{CreatedAt: time.Now(), Payload: payload, Queue: queue}
	queue.Buffer <- p
	queue.flushCommand <- true
	return nil
}

// ConsumeMulti return channel to read on
func (c *Consumer) ConsumeMulti(size int, fn func([]byte)) chan []byte {

	fmt.Println("setting up buffered consumer")
	chn := make(chan []byte)

	go func(chn chan []byte, size int, fn func([]byte)) {
		c.ResetWorking()
		for {
			packages, err := c.MultiGet(size)
			if err != nil {
				fmt.Println(err)
				if err.Error() == "unacked Packages found" {
					for i := 0; i < int(c.GetUnackedLength()); i++ {
						p, _ := c.GetUnacked()
						p.Ack()
						go fn([]byte(p.Payload))
					}
				}
				continue
			}
			packages[len(packages)-1].MultiAck()

			for _, p := range packages {
				fmt.Println("got multi packages", p.Payload)
				if fn != nil {
					fmt.Println("Got Package", p.Payload)
					go fn([]byte(p.Payload))
					continue
				}
				chn <- []byte(p.Payload)
			}
		}
	}(chn, size, fn)

	return chn
}

// FlushBuffer tells the background writer to flush the buffer to redis
func (queue *BufferedQueue) FlushBuffer() {
	flushing := make(chan bool, 1)
	queue.flushStatus <- flushing
	queue.flushCommand <- true
	<-flushing
	return
}

func (queue *BufferedQueue) startWritingBufferToRedis() {
	go func() {
		queue.nextWrite = time.Now().Unix()
		for {
			if len(queue.Buffer) >= queue.BufferSize || time.Now().Unix() >= queue.nextWrite {
				size := len(queue.Buffer)
				a := []string{}
				for i := 0; i < size; i++ {
					p := <-queue.Buffer
					a = append(a, p.getString())
				}
				queue.redisClient.LPush(queueInputKey(queue.Name), a)
				queue.incrRate(queueInputRateKey(queue.Name), int64(size))
				for i := 0; i < len(queue.flushStatus); i++ {
					c := <-queue.flushStatus
					c <- true
				}
				queue.nextWrite = time.Now().Unix() + 1
			}
			<-queue.flushCommand
		}
	}()
}

func (queue *BufferedQueue) startPacemaker() {
	go func() {
		for {
			queue.flushCommand <- true
			time.Sleep(10 * time.Millisecond)
		}
	}()
}

func (queue *BufferedQueue) startHeartbeat() {
	firstWrite := make(chan bool, 1)
	go func() {
		firstRun := true
		for {
			queue.redisClient.Set(queueHeartbeatKey(queue.Name), "ping", time.Second)
			if firstRun {
				firstWrite <- true
				firstRun = false
			}
			time.Sleep(500 * time.Millisecond)
		}
	}()
	<-firstWrite
	return
}
