/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package algo

import (
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hyperledger/fabric/gossip/util"
	"github.com/stretchr/testify/require"
)

func init() {
	util.SetupTestLogging()
}

type messageHook func(interface{})

type pullTestInstance struct {
	msgHooks          []messageHook // 收到消息之后可以预处理的函数
	peers             map[string]*pullTestInstance
	name              string
	nextPeerSelection []string
	msgQueue          chan interface{}
	lock              sync.Mutex
	stopChan          chan struct{}
	*PullEngine
}

type helloMsg struct {
	nonce  uint64
	source string
}

type digestMsg struct {
	nonce  uint64
	digest []string
	source string
}

type reqMsg struct {
	items  []string
	nonce  uint64
	source string
}

type resMsg struct {
	items []string
	nonce uint64
}

func newPushPullTestInstance(name string, peers map[string]*pullTestInstance) *pullTestInstance {
	inst := &pullTestInstance{
		msgHooks:          make([]messageHook, 0),
		peers:             peers,
		msgQueue:          make(chan interface{}, 100),
		nextPeerSelection: make([]string, 0),
		stopChan:          make(chan struct{}, 1),
		name:              name,
	}

	config := PullEngineConfig{
		DigestWaitTime:   time.Duration(100) * time.Millisecond,
		RequestWaitTime:  time.Duration(200) * time.Millisecond,
		ResponseWaitTime: time.Duration(200) * time.Millisecond,
	}

	inst.PullEngine = NewPullEngine(inst, time.Duration(500)*time.Millisecond, config)

	peers[name] = inst
	// 持续的接收消息
	go func() {
		for {
			select {
			case <-inst.stopChan:
				return
			case m := <-inst.msgQueue:
				inst.handleMessage(m)
			}
		}
	}()

	return inst
}

// 用于测试节点收到的消息 f是函数接口
// Used to test the messages one peer sends to another.
// Assert statements should be passed via the messageHook f
func (p *pullTestInstance) hook(f messageHook) {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.msgHooks = append(p.msgHooks, f)
}

func (p *pullTestInstance) handleMessage(m interface{}) {
	p.lock.Lock()
	for _, f := range p.msgHooks {
		f(m)
	}
	p.lock.Unlock()

	if helloMsg, isHello := m.(*helloMsg); isHello {
		p.OnHello(helloMsg.nonce, helloMsg.source)
		return
	}

	if digestMsg, isDigest := m.(*digestMsg); isDigest {
		p.OnDigest(digestMsg.digest, digestMsg.nonce, digestMsg.source)
		return
	}

	if reqMsg, isReq := m.(*reqMsg); isReq {
		p.OnReq(reqMsg.items, reqMsg.nonce, reqMsg.source)
		return
	}

	if resMsg, isRes := m.(*resMsg); isRes {
		p.OnRes(resMsg.items, resMsg.nonce)
	}
}

func (p *pullTestInstance) stop() {
	p.stopChan <- struct{}{}
	p.Stop()
}

func (p *pullTestInstance) setNextPeerSelection(selection []string) {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.nextPeerSelection = selection
}

func (p *pullTestInstance) SelectPeers() []string {
	p.lock.Lock()
	defer p.lock.Unlock()
	return p.nextPeerSelection
}

func (p *pullTestInstance) Hello(dest string, nonce uint64) {
	p.peers[dest].msgQueue <- &helloMsg{nonce: nonce, source: p.name}
}

func (p *pullTestInstance) SendDigest(digest []string, nonce uint64, context interface{}) {
	p.peers[context.(string)].msgQueue <- &digestMsg{source: p.name, nonce: nonce, digest: digest}
}

func (p *pullTestInstance) SendReq(dest string, items []string, nonce uint64) {
	p.peers[dest].msgQueue <- &reqMsg{nonce: nonce, source: p.name, items: items}
}

func (p *pullTestInstance) SendRes(items []string, context interface{}, nonce uint64) {
	p.peers[context.(string)].msgQueue <- &resMsg{items: items, nonce: nonce}
}

func TestPullEngine_Add(t *testing.T) {
	peers := make(map[string]*pullTestInstance)
	inst1 := newPushPullTestInstance("p1", peers)
	defer inst1.Stop()
	inst1.Add("0")
	inst1.Add("0")
	require.True(t, inst1.PullEngine.state.Exists("0"))
}

func TestPullEngine_Remove(t *testing.T) {
	peers := make(map[string]*pullTestInstance)
	inst1 := newPushPullTestInstance("p1", peers)
	defer inst1.Stop()
	inst1.Add("0")
	require.True(t, inst1.PullEngine.state.Exists("0"))
	inst1.Remove("0")
	require.False(t, inst1.PullEngine.state.Exists("0"))
	inst1.Remove("0") // remove twice
	require.False(t, inst1.PullEngine.state.Exists("0"))
}

func TestPullEngine_Stop(t *testing.T) {
	peers := make(map[string]*pullTestInstance)
	inst1 := newPushPullTestInstance("p1", peers)
	inst2 := newPushPullTestInstance("p2", peers)
	defer inst2.stop()
	inst2.setNextPeerSelection([]string{"p1"}) // p2向
	go func() {
		for i := 0; i < 100; i++ {
			inst1.Add(strconv.Itoa(i))
			time.Sleep(time.Duration(10) * time.Millisecond)
		}
	}()

	time.Sleep(time.Duration(800) * time.Millisecond)
	len1 := len(inst2.state.ToArray())
	inst1.stop()
	time.Sleep(time.Duration(800) * time.Millisecond)
	len2 := len(inst2.state.ToArray())
	require.Equal(t, len1, len2, "PullEngine was still active after Stop() was invoked!")
}

// 生成 10 个节点，每个节点初始携带 1 个数据
// 最终每个节点应该都携带 10 个数据
func TestPullEngineAll2AllWithIncrementalSpawning(t *testing.T) {
	// Scenario: spawn 10 nodes, each 50 ms after the other
	// and have them transfer data between themselves.
	// Expected outcome: obviously, everything should succeed.
	// Isn't that's why we're here?
	instanceCount := 10
	peers := make(map[string]*pullTestInstance)

	for i := 0; i < instanceCount; i++ {
		inst := newPushPullTestInstance(fmt.Sprintf("p%d", i+1), peers)
		inst.Add(strconv.Itoa(i + 1))
		time.Sleep(time.Duration(50) * time.Millisecond)
	}
	for i := 0; i < instanceCount; i++ {
		pID := fmt.Sprintf("p%d", i+1)
		peers[pID].setNextPeerSelection(keySet(pID, peers))
	}
	time.Sleep(time.Duration(4000) * time.Millisecond)

	for i := 0; i < instanceCount; i++ {
		pID := fmt.Sprintf("p%d", i+1)
		require.Equal(t, instanceCount, len(peers[pID].state.ToArray()))
	}
}

// 场景：inst1 拥有 {1, 3}，inst2 拥有 {0, 1, 2, 3}。
// inst1 向 inst2 发起请求。
// 期望结果：inst1 请求 0、2，inst2 只发送 0、2。
func TestPullEngineSelectiveUpdates(t *testing.T) {
	// Scenario: inst1 has {1, 3} and inst2 has {0,1,2,3}.
	// inst1 initiates to inst2
	// Expected outcome: inst1 asks for 0,2 and inst2 sends 0,2 only
	peers := make(map[string]*pullTestInstance)
	inst1 := newPushPullTestInstance("p1", peers)
	inst2 := newPushPullTestInstance("p2", peers)
	defer inst1.stop()
	defer inst2.stop()

	inst1.Add("1", "3")
	inst2.Add("0", "1", "2", "3")

	// 保证inst2发出的digest消息是所有
	// Ensure inst2 sent a proper digest to inst1
	inst1.hook(func(m interface{}) {
		if dig, isDig := m.(*digestMsg); isDig {
			require.True(t, util.IndexInSlice(dig.digest, "0", Strcmp) != -1)
			require.True(t, util.IndexInSlice(dig.digest, "1", Strcmp) != -1)
			require.True(t, util.IndexInSlice(dig.digest, "2", Strcmp) != -1)
			require.True(t, util.IndexInSlice(dig.digest, "3", Strcmp) != -1)
		}
	})
	// 保证inst1只请求缺失的0和2
	// Ensure inst1 requested only needed updates from inst2
	inst2.hook(func(m interface{}) {
		if req, isReq := m.(*reqMsg); isReq {
			require.True(t, util.IndexInSlice(req.items, "1", Strcmp) == -1)
			require.True(t, util.IndexInSlice(req.items, "3", Strcmp) == -1)

			require.True(t, util.IndexInSlice(req.items, "0", Strcmp) != -1)
			require.True(t, util.IndexInSlice(req.items, "2", Strcmp) != -1)
		}
	})

	// Ensure inst1 received only needed updates from inst2
	inst1.hook(func(m interface{}) {
		if res, isRes := m.(*resMsg); isRes {
			require.True(t, util.IndexInSlice(res.items, "1", Strcmp) == -1)
			require.True(t, util.IndexInSlice(res.items, "3", Strcmp) == -1)

			require.True(t, util.IndexInSlice(res.items, "0", Strcmp) != -1)
			require.True(t, util.IndexInSlice(res.items, "2", Strcmp) != -1)
		}
	})

	inst1.setNextPeerSelection([]string{"p2"})

	time.Sleep(time.Duration(2000) * time.Millisecond)
	require.Equal(t, len(inst2.state.ToArray()), len(inst1.state.ToArray()))
}

// 场景：inst1 向 inst2 发送 Hello，但 inst3 是拜占庭节点，因此它尝试向 inst1 发送摘要和响应。
// 期望的结果是 inst1 不会处理来自 inst3 的更新。
func TestByzantineResponder(t *testing.T) {
	// Scenario: inst1 sends hello to inst2 but inst3 is byzantine so it attempts to send a digest and a response to inst1.
	// expected outcome is for inst1 not to process updates from inst3.
	peers := make(map[string]*pullTestInstance)
	inst1 := newPushPullTestInstance("p1", peers)
	inst2 := newPushPullTestInstance("p2", peers)
	inst3 := newPushPullTestInstance("p3", peers)
	defer inst1.stop()
	defer inst2.stop()
	defer inst3.stop()

	receivedDigestFromInst3 := int32(0)

	inst2.Add("1", "2", "3")
	inst3.Add("1", "6", "7")

	// 一旦inst2收到hello，inst3就发送他的摘要
	inst2.hook(func(m interface{}) {
		if _, isHello := m.(*helloMsg); isHello {
			inst3.SendDigest([]string{"5", "6", "7"}, 0, "p1")
		}
	})

	inst1.hook(func(m interface{}) {
		if dig, isDig := m.(*digestMsg); isDig {
			if dig.source == "p3" {
				atomic.StoreInt32(&receivedDigestFromInst3, int32(1))
				// inst3强行向inst1发送Res消息
				time.AfterFunc(time.Duration(150)*time.Millisecond, func() {
					inst3.SendRes([]string{"5", "6", "7"}, "p1", 0)
				})
			}
		}

		if res, isRes := m.(*resMsg); isRes {
			// the response is from p3
			if util.IndexInSlice(res.items, "6", Strcmp) != -1 {
				// inst1 is currently accepting responses
				require.Equal(t, int32(1), atomic.LoadInt32(&(inst1.acceptingResponses)), "inst1 is not accepting digests")
			}
		}
	})

	inst1.setNextPeerSelection([]string{"p2"})

	time.Sleep(time.Duration(1000) * time.Millisecond)

	require.Equal(t, int32(1), atomic.LoadInt32(&receivedDigestFromInst3), "inst1 hasn't received a digest from inst3")

	require.True(t, util.IndexInSlice(inst1.state.ToArray(), "1", Strcmp) != -1)
	require.True(t, util.IndexInSlice(inst1.state.ToArray(), "2", Strcmp) != -1)
	require.True(t, util.IndexInSlice(inst1.state.ToArray(), "3", Strcmp) != -1)

	require.True(t, util.IndexInSlice(inst1.state.ToArray(), "5", Strcmp) == -1)
	require.True(t, util.IndexInSlice(inst1.state.ToArray(), "6", Strcmp) == -1)
	require.True(t, util.IndexInSlice(inst1.state.ToArray(), "7", Strcmp) == -1)
}

// 场景：inst1、inst2 和 inst3 同时与 inst4 启动协议。
// 期望结果：inst4 成功向它们所有人传输状态。
func TestMultipleInitiators(t *testing.T) {
	// Scenario: inst1, inst2 and inst3 both start protocol with inst4 at the same time.
	// Expected outcome: inst4 successfully transfers state to all of them
	peers := make(map[string]*pullTestInstance)
	inst1 := newPushPullTestInstance("p1", peers)
	inst2 := newPushPullTestInstance("p2", peers)
	inst3 := newPushPullTestInstance("p3", peers)
	inst4 := newPushPullTestInstance("p4", peers)
	defer inst1.stop()
	defer inst2.stop()
	defer inst3.stop()
	defer inst4.stop()

	inst4.Add("1", "2", "3", "4")
	inst1.setNextPeerSelection([]string{"p4"})
	inst2.setNextPeerSelection([]string{"p4"})
	inst3.setNextPeerSelection([]string{"p4"})

	time.Sleep(time.Duration(2000) * time.Millisecond)

	for _, inst := range []*pullTestInstance{inst1, inst2, inst3} {
		require.True(t, util.IndexInSlice(inst.state.ToArray(), "1", Strcmp) != -1)
		require.True(t, util.IndexInSlice(inst.state.ToArray(), "2", Strcmp) != -1)
		require.True(t, util.IndexInSlice(inst.state.ToArray(), "3", Strcmp) != -1)
		require.True(t, util.IndexInSlice(inst.state.ToArray(), "4", Strcmp) != -1)
	}
}

// 场景：inst1 向 inst2（项目：{1,2,3,4}）和 inst3（项目：{5,6,7,8}）发起请求，
// 但 inst2 响应太慢，所有项目都应该从 inst3 收到。
func TestLatePeers(t *testing.T) {
	// Scenario: inst1 initiates to inst2 (items: {1,2,3,4}) and inst3 (items: {5,6,7,8}),
	// but inst2 is too slow to respond, and all items
	// should be received from inst3.
	peers := make(map[string]*pullTestInstance)
	inst1 := newPushPullTestInstance("p1", peers)
	inst2 := newPushPullTestInstance("p2", peers)
	inst3 := newPushPullTestInstance("p3", peers)
	defer inst1.stop()
	defer inst2.stop()
	defer inst3.stop()
	inst2.Add("1", "2", "3", "4")
	inst3.Add("5", "6", "7", "8")
	inst2.hook(func(m interface{}) {
		// 不管p2收到什么请求 都停止600ms
		time.Sleep(time.Duration(600) * time.Millisecond)
	})
	inst1.setNextPeerSelection([]string{"p2", "p3"})

	time.Sleep(time.Duration(2000) * time.Millisecond)

	require.True(t, util.IndexInSlice(inst1.state.ToArray(), "1", Strcmp) == -1)
	require.True(t, util.IndexInSlice(inst1.state.ToArray(), "2", Strcmp) == -1)
	require.True(t, util.IndexInSlice(inst1.state.ToArray(), "3", Strcmp) == -1)
	require.True(t, util.IndexInSlice(inst1.state.ToArray(), "4", Strcmp) == -1)

	require.True(t, util.IndexInSlice(inst1.state.ToArray(), "5", Strcmp) != -1)
	require.True(t, util.IndexInSlice(inst1.state.ToArray(), "6", Strcmp) != -1)
	require.True(t, util.IndexInSlice(inst1.state.ToArray(), "7", Strcmp) != -1)
	require.True(t, util.IndexInSlice(inst1.state.ToArray(), "8", Strcmp) != -1)
}

// 场景：inst1 拥有 {1, 3}，inst2 拥有 {0,2}，两者同时向对方发起请求。
// 期望结果：最终两者都拥有 {0,1,2,3}。
func TestBiDiUpdates(t *testing.T) {
	// Scenario: inst1 has {1, 3} and inst2 has {0,2} and both initiate to the other at the same time.
	// Expected outcome: both have {0,1,2,3} in the end
	peers := make(map[string]*pullTestInstance)
	inst1 := newPushPullTestInstance("p1", peers)
	inst2 := newPushPullTestInstance("p2", peers)
	defer inst1.stop()
	defer inst2.stop()

	inst1.Add("1", "3")
	inst2.Add("0", "2")

	inst1.setNextPeerSelection([]string{"p2"})
	inst2.setNextPeerSelection([]string{"p1"})

	time.Sleep(time.Duration(2000) * time.Millisecond)

	require.True(t, util.IndexInSlice(inst1.state.ToArray(), "0", Strcmp) != -1)
	require.True(t, util.IndexInSlice(inst1.state.ToArray(), "1", Strcmp) != -1)
	require.True(t, util.IndexInSlice(inst1.state.ToArray(), "2", Strcmp) != -1)
	require.True(t, util.IndexInSlice(inst1.state.ToArray(), "3", Strcmp) != -1)

	require.True(t, util.IndexInSlice(inst2.state.ToArray(), "0", Strcmp) != -1)
	require.True(t, util.IndexInSlice(inst2.state.ToArray(), "1", Strcmp) != -1)
	require.True(t, util.IndexInSlice(inst2.state.ToArray(), "2", Strcmp) != -1)
	require.True(t, util.IndexInSlice(inst2.state.ToArray(), "3", Strcmp) != -1)
}

// 场景：p2-p5都有0-100，p1向p2-p4 pull数据‘
// 期望结果：p2-p4 至少被选择一次（不选择它们的概率很小），p5 不被选择。
func TestSpread(t *testing.T) {
	// Scenario: inst1 initiates to inst2, inst3 inst4 and each have items 0-100. inst5 also has the same items but isn't selected
	// Expected outcome: each responder (inst2, inst3 and inst4) is chosen at least once (the probability for not choosing each of them is slim)
	// inst5 isn't selected at all
	peers := make(map[string]*pullTestInstance)
	inst1 := newPushPullTestInstance("p1", peers)
	inst2 := newPushPullTestInstance("p2", peers)
	inst3 := newPushPullTestInstance("p3", peers)
	inst4 := newPushPullTestInstance("p4", peers)
	inst5 := newPushPullTestInstance("p5", peers)
	defer inst1.stop()
	defer inst2.stop()
	defer inst3.stop()
	defer inst4.stop()
	defer inst5.stop()

	chooseCounters := make(map[string]int)
	chooseCounters["p2"] = 0
	chooseCounters["p3"] = 0
	chooseCounters["p4"] = 0
	chooseCounters["p5"] = 0

	lock := &sync.Mutex{}

	addToCounters := func(dest string) func(m interface{}) {
		return func(m interface{}) {
			if _, isReq := m.(*reqMsg); isReq {
				lock.Lock()
				chooseCounters[dest]++
				lock.Unlock()
			}
		}
	}

	inst2.hook(addToCounters("p2"))
	inst3.hook(addToCounters("p3"))
	inst4.hook(addToCounters("p4"))
	inst5.hook(addToCounters("p5"))

	for i := 0; i < 100; i++ {
		item := fmt.Sprintf("%d", i)
		inst2.Add(item)
		inst3.Add(item)
		inst4.Add(item)
	}

	inst1.setNextPeerSelection([]string{"p2", "p3", "p4"})

	time.Sleep(time.Duration(2000) * time.Millisecond)

	lock.Lock()
	for pI, counter := range chooseCounters {
		if pI == "p5" {
			require.Equal(t, 0, counter)
		} else {
			require.True(t, counter > 0, "%s was not selected!", pI)
		}
	}
	lock.Unlock()
}

// p1 有 0-5，p1 只给 p2 偶数项，只给 p3 奇数项，且 p2 和 p3 不互通
// 期望结果：p2 只有偶数项，p3 只有奇数项。
func TestFilter(t *testing.T) {
	// Scenario: 3 instances, items [0-5] are found only in the first instance, the other 2 have none.
	//           and also the first instance only gives the 2nd instance even items, and odd items to the 3rd.
	//           also, instances 2 and 3 don't know each other.
	// Expected outcome: inst2 has only even items, and inst3 has only odd items
	peers := make(map[string]*pullTestInstance)
	inst1 := newPushPullTestInstance("p1", peers)
	inst2 := newPushPullTestInstance("p2", peers)
	inst3 := newPushPullTestInstance("p3", peers)
	defer inst1.stop()
	defer inst2.stop()
	defer inst3.stop()

	// 给p1发送奇数项，给p2发送偶数项
	inst1.PullEngine.digFilter = func(context interface{}) func(digestItem string) bool {
		return func(digestItem string) bool {
			n, _ := strconv.ParseInt(digestItem, 10, 64)
			if context == "p2" {
				return n%2 == 0
			}
			return n%2 == 1
		}
	}

	inst1.Add("0", "1", "2", "3", "4", "5")
	inst2.setNextPeerSelection([]string{"p1"})
	inst3.setNextPeerSelection([]string{"p1"})

	time.Sleep(time.Second * 2)

	require.True(t, util.IndexInSlice(inst2.state.ToArray(), "0", Strcmp) != -1)
	require.True(t, util.IndexInSlice(inst2.state.ToArray(), "1", Strcmp) == -1)
	require.True(t, util.IndexInSlice(inst2.state.ToArray(), "2", Strcmp) != -1)
	require.True(t, util.IndexInSlice(inst2.state.ToArray(), "3", Strcmp) == -1)
	require.True(t, util.IndexInSlice(inst2.state.ToArray(), "4", Strcmp) != -1)
	require.True(t, util.IndexInSlice(inst2.state.ToArray(), "5", Strcmp) == -1)

	require.True(t, util.IndexInSlice(inst3.state.ToArray(), "0", Strcmp) == -1)
	require.True(t, util.IndexInSlice(inst3.state.ToArray(), "1", Strcmp) != -1)
	require.True(t, util.IndexInSlice(inst3.state.ToArray(), "2", Strcmp) == -1)
	require.True(t, util.IndexInSlice(inst3.state.ToArray(), "3", Strcmp) != -1)
	require.True(t, util.IndexInSlice(inst3.state.ToArray(), "4", Strcmp) == -1)
	require.True(t, util.IndexInSlice(inst3.state.ToArray(), "5", Strcmp) != -1)
}

func Strcmp(a interface{}, b interface{}) bool {
	return a.(string) == b.(string)
}

// m是所有Peer节点的map，这个函数返回一个不包含selfPeer的Peer数组
func keySet(selfPeer string, m map[string]*pullTestInstance) []string {
	peers := make([]string, len(m)-1)
	i := 0
	for pID := range m {
		if pID == selfPeer {
			continue
		}
		peers[i] = pID
		i++
	}
	return peers
}
