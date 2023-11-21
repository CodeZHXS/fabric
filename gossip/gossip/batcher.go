/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package gossip

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
)

type emitBatchCallback func([]interface{})

// batchingEmitter is used for the gossip push/forwarding phase.
// Messages are added into the batchingEmitter, and they are forwarded periodically T times in batches and then discarded.
// If the batchingEmitter's stored message count reaches a certain capacity, that also triggers a message dispatch
type batchingEmitter interface {
	// 将一条消息添加到批处理队列
	// Add adds a message to be batched
	Add(interface{})

	// Stop stops the component
	Stop()

	// Size returns the amount of pending messages to be emitted
	Size() int
}

// 这个构造函数接受这些参数：
// iterations: 每个消息转发的次数
// burstSize: 如果大于这个数马上进行转发
// latency: 转发周期
// cb: 回调函数，按顺序调用以触发转发。
// newBatchingEmitter accepts the following parameters:
// iterations: number of times each message is forwarded
// burstSize: a threshold that triggers a forwarding because of message count
// latency: the maximum delay that each message can be stored without being forwarded
// cb: a callback that is called in order for the forwarding to take place
func newBatchingEmitter(iterations, burstSize int, latency time.Duration, cb emitBatchCallback) batchingEmitter {
	if iterations < 0 {
		panic(errors.Errorf("Got a negative iterations number"))
	}

	p := &batchingEmitterImpl{
		cb:         cb,
		delay:      latency,
		iterations: iterations, // 转发的轮次
		burstSize:  burstSize,
		lock:       &sync.Mutex{},
		buff:       make([]*batchedMessage, 0),
		stopFlag:   int32(0),
	}

	// 一旦新建一个Emitter 就启动一个线程不断的发送消息
	if iterations != 0 {
		go p.periodicEmit()
	}

	return p
}

// 周期性发送
func (p *batchingEmitterImpl) periodicEmit() {
	for !p.toDie() {
		time.Sleep(p.delay)
		p.lock.Lock()
		p.emit()
		p.lock.Unlock()
	}
}

// 将buff所的消息发送出去
func (p *batchingEmitterImpl) emit() {
	if p.toDie() {
		return
	}
	if len(p.buff) == 0 {
		return
	}
	msgs2beEmitted := make([]interface{}, len(p.buff))
	for i, v := range p.buff {
		msgs2beEmitted[i] = v.data
	}
	// 函数接口 要进去看具体的函数实现
	p.cb(msgs2beEmitted)
	p.decrementCounters()
}

func (p *batchingEmitterImpl) decrementCounters() {
	n := len(p.buff)
	for i := 0; i < n; i++ {
		msg := p.buff[i]
		msg.iterationsLeft--
		if msg.iterationsLeft == 0 {
			p.buff = append(p.buff[:i], p.buff[i+1:]...)
			n--
			i--
		}
	}
}

func (p *batchingEmitterImpl) toDie() bool {
	return atomic.LoadInt32(&(p.stopFlag)) == int32(1)
}

type batchingEmitterImpl struct {
	iterations int
	burstSize  int
	delay      time.Duration
	cb         emitBatchCallback
	lock       *sync.Mutex
	buff       []*batchedMessage
	stopFlag   int32
}

type batchedMessage struct {
	data           interface{}
	iterationsLeft int
}

func (p *batchingEmitterImpl) Stop() {
	atomic.StoreInt32(&(p.stopFlag), int32(1))
}

func (p *batchingEmitterImpl) Size() int {
	p.lock.Lock()
	defer p.lock.Unlock()
	return len(p.buff)
}

func (p *batchingEmitterImpl) Add(message interface{}) {
	if p.iterations == 0 {
		return
	}
	p.lock.Lock()
	defer p.lock.Unlock()

	p.buff = append(p.buff, &batchedMessage{data: message, iterationsLeft: p.iterations})

	// 如果buff满了 马上发送
	if len(p.buff) >= p.burstSize {
		p.emit()
	}
}
