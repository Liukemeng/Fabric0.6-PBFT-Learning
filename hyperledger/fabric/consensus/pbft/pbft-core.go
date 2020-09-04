/*
Copyright IBM Corp. 2016 All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

		 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package pbft

import (
	"encoding/base64"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/hyperledger/fabric/consensus"
	"github.com/hyperledger/fabric/consensus/util/events"
	_ "github.com/hyperledger/fabric/core" // Needed for logging format init
	"github.com/op/go-logging"

	"github.com/golang/protobuf/proto"
	"github.com/spf13/viper"
)

// =============================================================================
// init
// =============================================================================

var logger *logging.Logger // package-level logger

func init() {
	logger = logging.MustGetLogger("consensus/pbft")
}

const (
	// UnreasonableTimeout is an ugly thing, we need to create timers, then stop them before they expire, so use a large timeout
	UnreasonableTimeout = 100 * time.Hour
)

// =============================================================================
// custom interfaces and structure definitions
// =============================================================================

// Event Types

// workEvent is a temporary type, to inject work
//临时类型，用于注入work
type workEvent func()

// viewChangeTimerEvent is sent when the view change timer expires
//发生视图切换时，发送viewChangeTimerEvent这个事件。
type viewChangeTimerEvent struct{}

// execDoneEvent is sent when an execution completes
//一个execution执行完了，发送execDoneEvent事件。
type execDoneEvent struct{}

// pbftMessageEvent is sent when a consensus messages is received to be sent to pbft
//
type pbftMessageEvent pbftMessage

// viewChangedEvent is sent when the view change timer expires
type viewChangedEvent struct{}

// viewChangeResendTimerEvent is sent when the view change resend timer expires
type viewChangeResendTimerEvent struct{}

// returnRequestBatchEvent is sent by pbft when we are forwarded a request
//当我们收到一个request请求的时候，发送这个事件
type returnRequestBatchEvent *RequestBatch

// nullRequestEvent provides "keep-alive" null requests
//心跳事件，没有request请求的时候，发送nullRequestEvent保持alive
type nullRequestEvent struct{}

// Unless otherwise noted, all methods consume the PBFT thread, and should therefore
// not rely on PBFT accomplishing any work while that thread is being held
// 除非另有说明，所有的方法消耗PBFT线程，因此不应该依赖PBFT完成任何工作，而该线程被持有
type innerStack interface {
	broadcast(msgPayload []byte)
	unicast(msgPayload []byte, receiverID uint64) (err error)
	execute(seqNo uint64, reqBatch *RequestBatch) // This is invoked on a separate thread
	getState() []byte
	getLastSeqNo() (uint64, error)
	skipTo(seqNo uint64, snapshotID []byte, peers []uint64)

	sign(msg []byte) ([]byte, error)
	verify(senderID uint64, signature []byte, message []byte) error

	invalidateState()
	validateState()

	consensus.StatePersistor
}

// This structure is used for incoming PBFT bound messages
//所有共识过程中消息都被封装为pbftMessage;pbftMessage消息包括以下几种类型的消息，
//	*Message_RequestBatch
//	*Message_PrePrepare
//	*Message_Prepare
//	*Message_Commit
//	*Message_Checkpoint
//	*Message_ViewChange
//	*Message_NewView
//	*Message_FetchRequestBatch
//	*Message_ReturnRequestBatch
type pbftMessage struct {
	sender uint64
	msg    *Message
}

//检查点消息
type checkpointMessage struct {
	seqNo uint64
	id    []byte
}

//收到的N-f个checkpointMsg?
type stateUpdateTarget struct {
	checkpointMessage
	replicas []uint64
}

//共识过程中的instance，包含了共识期间用到的所有字段，
type pbftCore struct {
	// internal data   内部数据
	internalLock sync.Mutex
	executing    bool // signals that application is executing

	idleChan   chan struct{} // 用于检测宕机？ Used to detect idleness for testing
	injectChan chan func()   // 用于向PBFT中注入work来模拟拜占庭？ Used as a hack to inject work onto the PBFT thread, to be removed eventually
	consumer innerStack    //innerStack 接口，实际上是obcBatch

	// PBFT data
	activeView    bool              // 在有效的视图内，正常共识的时候是true，当发生视图转换的时候置为false；
	byzantine     bool              // 用来进行测试用的
	f             int               // max. number of faults we can tolerate
	N             int               // max.number of validators in the network
	h             uint64            // 低水线
	id            uint64            // replica ID; PBFT `i`
	K             uint64            // 生成checkpoint间隔；来自config.yaml 中的 general.K 字段。
	logMultiplier uint64            // 一个大于2的倍数,用来计算L的
	L             uint64            // 水线间距，L=K*logMultiplier，高水线是H=h+L;
	lastExec      uint64            // 已经执行完成的"最近的"requestBatch编号,检查点通过这个是不是K的整数倍来决定是否进行checkpoint；
	replicaCount  int               //  number of replicas; PBFT `|R|`
	seqNo         uint64            //当前的序号
	view          uint64            // 当前视图编号

	chkpts        map[uint64]string // 保存自己生成的Checkpoint; key是lastExec（刚执行完的seqNo），value是当前区块链的状态；
	                                // 在pbftCore.Checkpoint()方法中设置的；在pbftCore.moveWatermarks()方法中删除；
	hChkpts           map[uint64]uint64  //其他节点高于我最高检查点H的；checkpoint；key是各个节点的编号，value是checkpoint的SequenceNumber；
                                          //在pbftCore.recvCheckpoint(Checkpoint)中调用pbftCore.weakCheckpointSetOutOfRange(Checkpoint)更新并清空;
	checkpointStore map[Checkpoint]bool // 存收到的其他节点发送过来的Checkpoint检查点消息，key是Checkpoint，value是 bool类型；
	                                    // pbftCore.recvCheckpoint()中把这个checkpoint对应的value置为true；在pbftcore.moveWatermarks()中清除小于h的checkpoint；

	pset          map[uint64]*ViewChange_PQ   //pset里面存当前节点最新checkpoint之后，所有已经到达prepared状态的batch消息；在pbftcore.moveWatermarks()中清除小于h的消息；
	qset          map[qidx]*ViewChange_PQ    //qset里面存当前节点最新checkpoint之后，到达Pre-Prepared的requestBatch的batch消息。
	                                         // 主节点在Pre-prepare阶段从节点在Prepare阶段将Pre-prepare存到qset里面；bftcore.moveWatermarks()中清除小于h的消息；

	skipInProgress    bool               // 在节点落后，需要执行状态同步的时候会置为true；
	stateTransferring bool               // 正在进行状态数据的传输，正在传输的话置为true；
	highStateTarget   *stateUpdateTarget // 需要同步到什么sequenceNum。

	currentExec           *uint64    // 当前正在被执行的requestBatch的编号；在pbftCore.executeOne()中被置为当前正在执行的batch的n;在pbftCore.execDoneSync()中被置为nil.
	timerActive           bool                     // newViewTimer定时器是否正在运行；softStartTimer()、startTimer()将它置为true，stopTimer将它置为false；
	newViewTimer          events.Timer             // 本轮共识没在规定时间内完成，触发视图转换
	requestTimeout        time.Duration            // 一个request从被reception到exception的超时时间阈值，就是newViewTimer的时间阈值；对应config里面timeout.request，到时间就vc
	newViewTimeout        time.Duration            // 一个视图转换过程需要的时间上限；对应config里面timeout.viewchange
	newViewTimerReason    string                   // what triggered the timer

	vcResendTimer         events.Timer             // 视图转换超时，再次发送视图转换的定时器
	vcResendTimeout       time.Duration            // 接收quorum vc的时间上限，到了就重发vc
	lastNewViewTimeout    time.Duration            //上一次视图转换的时间阈值，这一轮vcResendTimer的时间阈值vcResendTimeout应该是会*2
	broadcastTimeout      time.Duration            //一个消息广播的时间阈值，同步假设
	nullRequestTimer   events.Timer  // 定时器:检测主节点是否在规定事件内发送nullRequest batch(心跳信息)
	nullRequestTimeout time.Duration // nullRequestTimer的阈值，设置为0为disable，需要greater than request timeout
	viewChangePeriod   uint64        // period between automatic view changes，自动视图转换之间的时间段
	viewChangeSeqNo    uint64        // 到这个视图的时候，强制视图转换。

	outstandingReqBatches map[string]*RequestBatch //存那些那些处于共识三阶段的、但还未被执行的requestBatch；
	// recvRequestBatch()方法将requestBatch放到这里面，然后到达committed状态在执行之前，删除掉这里面的这个的requestBatch,
	//执行完相应的requestBatch之后，判断这个map是不是空，如果outstandingReqBatches中还有需要待处理的requestBatch,就reset newViewTimer,如果没有待处理的requestBatch就reset nullRequestTimer。
	reqBatchStore   map[string]*RequestBatch // 存主节点构造的RequestBatch，
	// 主节点在pbftCore.recvRequestBatch()，从节点在pbftCore.recvPrePrepare()中将收到的 在requestBatch存到这个字段里面，在pbftcore.moveWatermarks()中清除小于h的requestBatch
	certStore       map[msgID]*msgCert       //msgId由视图编号v和交易序号组成，msgCert结构体中包含digest、 PrePrepare及Prepare、Commit2种状态的一个切片，所以这个map存正在共识的requestBatch的PrePrepare、Prepare、Commit消息（自己的和其他节点发送过来的），
	// 在pbftcore.moveWatermarks()中清除小于新的稳定检查点h的相关信息；在pbftCore.sendViewChange()中删除小于新的视图v+1的certStore消息，因为在这个方法中已经根据certstore更新了PQset，相关信息会传到下个视图；

	viewChangeStore map[vcidx]*ViewChange // 存本次视图转换收到的viewChange消息，在pbftCore.sendViewChange()里面清空的小于v+1的vc；
	newViewStore    map[uint64]*NewView      // 此字段记录了自己发送的或接收的、最新的、但还没处理完的 new-view 消息，在接收到 new-view 消息时，用这个字段判断相同视图编号的 new-view 消息是否已经在处理中；在pbftCore.processNewView2()进入到新视图后，将关于前一个视图的vc删除；
	missingReqBatches map[string]bool // for all the assigned, non-checkpointed request batches we might be missing during view-change

}

type qidx struct {
	d string
	n uint64
}

type msgID struct { // our index through certStore
	v uint64
	n uint64
}

type msgCert struct {
	digest      string
	prePrepare  *PrePrepare
	sentPrepare bool
	prepare     []*Prepare
	sentCommit  bool
	commit      []*Commit
}

type vcidx struct {
	v  uint64
	id uint64
}

type sortableUint64Slice []uint64

func (a sortableUint64Slice) Len() int {
	return len(a)
}
func (a sortableUint64Slice) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}
func (a sortableUint64Slice) Less(i, j int) bool {
	return a[i] < a[j]
}

// =============================================================================
// constructors
// =============================================================================

func newPbftCore(id uint64, config *viper.Viper, consumer innerStack, etf events.TimerFactory) *pbftCore {
	var err error
	instance := &pbftCore{}
	instance.id = id
	instance.consumer = consumer

	//设置定时器
	instance.newViewTimer = etf.CreateTimer()
	instance.vcResendTimer = etf.CreateTimer()
	instance.nullRequestTimer = etf.CreateTimer()

	//设置相关变量
	instance.N = config.GetInt("general.N")
	instance.f = config.GetInt("general.f")
	if instance.f*3+1 > instance.N {
		panic(fmt.Sprintf("need at least %d enough replicas to tolerate %d byzantine faults, but only %d replicas configured", instance.f*3+1, instance.f, instance.N))
	}

	instance.K = uint64(config.GetInt("general.K"))

	instance.logMultiplier = uint64(config.GetInt("general.logmultiplier"))
	if instance.logMultiplier < 2 {
		panic("Log multiplier must be greater than or equal to 2")
	}
	instance.L = instance.logMultiplier * instance.K // log size
	instance.viewChangePeriod = uint64(config.GetInt("general.viewchangeperiod"))

	instance.byzantine = config.GetBool("general.byzantine")

	instance.requestTimeout, err = time.ParseDuration(config.GetString("general.timeout.request"))
	if err != nil {
		panic(fmt.Errorf("Cannot parse request timeout: %s", err))
	}
	instance.vcResendTimeout, err = time.ParseDuration(config.GetString("general.timeout.resendviewchange"))
	if err != nil {
		panic(fmt.Errorf("Cannot parse request timeout: %s", err))
	}
	instance.newViewTimeout, err = time.ParseDuration(config.GetString("general.timeout.viewchange"))
	if err != nil {
		panic(fmt.Errorf("Cannot parse new view timeout: %s", err))
	}
	instance.nullRequestTimeout, err = time.ParseDuration(config.GetString("general.timeout.nullrequest"))
	if err != nil {
		instance.nullRequestTimeout = 0
	}
	instance.broadcastTimeout, err = time.ParseDuration(config.GetString("general.timeout.broadcast"))
	if err != nil {
		panic(fmt.Errorf("Cannot parse new broadcast timeout: %s", err))
	}

	instance.activeView = true
	instance.replicaCount = instance.N

	logger.Infof("PBFT type = %T", instance.consumer)
	logger.Infof("PBFT Max number of validating peers (N) = %v", instance.N)
	logger.Infof("PBFT Max number of failing peers (f) = %v", instance.f)
	logger.Infof("PBFT byzantine flag = %v", instance.byzantine)
	logger.Infof("PBFT request timeout = %v", instance.requestTimeout)
	logger.Infof("PBFT view change timeout = %v", instance.newViewTimeout)
	logger.Infof("PBFT Checkpoint period (K) = %v", instance.K)
	logger.Infof("PBFT broadcast timeout = %v", instance.broadcastTimeout)
	logger.Infof("PBFT Log multiplier = %v", instance.logMultiplier)
	logger.Infof("PBFT log size (L) = %v", instance.L)
	if instance.nullRequestTimeout > 0 {
		logger.Infof("PBFT null requests timeout = %v", instance.nullRequestTimeout)
	} else {
		logger.Infof("PBFT null requests disabled")
	}
	if instance.viewChangePeriod > 0 {
		logger.Infof("PBFT view change period = %v", instance.viewChangePeriod)
	} else {
		logger.Infof("PBFT automatic view change disabled")
	}

	// init the logs
	instance.certStore = make(map[msgID]*msgCert)
	instance.reqBatchStore = make(map[string]*RequestBatch)
	instance.checkpointStore = make(map[Checkpoint]bool)
	instance.chkpts = make(map[uint64]string)
	instance.viewChangeStore = make(map[vcidx]*ViewChange)
	instance.pset = make(map[uint64]*ViewChange_PQ)
	instance.qset = make(map[qidx]*ViewChange_PQ)
	instance.newViewStore = make(map[uint64]*NewView)

	// initialize state transfer
	instance.hChkpts = make(map[uint64]uint64)

	instance.chkpts[0] = "XXX GENESIS"

	instance.lastNewViewTimeout = instance.newViewTimeout
	instance.outstandingReqBatches = make(map[string]*RequestBatch)
	instance.missingReqBatches = make(map[string]bool)


	instance.restoreState()//恢复状态，恢复PpqSet、reqBatch、chkpt；

	instance.viewChangeSeqNo = ^uint64(0) // infinity
	instance.updateViewChangeSeqNo()

	return instance
}

// close tears down resources opened by newPbftCore
func (instance *pbftCore) close() {
	instance.newViewTimer.Halt()
	instance.nullRequestTimer.Halt()
}


//lkm  共识模块进行处理的地方

// allow the view-change protocol to kick-off when the timer expires
//共识模块的事件处理
func (instance *pbftCore) ProcessEvent(e events.Event) events.Event {
	var err error
	logger.Debugf("Replica %d processing event", instance.id)
	switch et := e.(type) {
	case viewChangeTimerEvent:
		logger.Infof("Replica %d view change timer expired, sending view change: %s", instance.id, instance.newViewTimerReason)
		instance.timerActive = false
		instance.sendViewChange()
	case *pbftMessage:
		return pbftMessageEvent(*et)
	//【节点间】的共识消息事件（pbftMessageEvent）都走这个分支进行处理
	//对发送方身份进行验证，并解析出共识消息RequestBatch、PrePrepare、Prepare、Commit、Checkpoint、ViewChange、Newview、FetchRequestBatch、ReturnRequestBatch；
	case pbftMessageEvent:
		msg := et
		logger.Debugf("Replica %d received incoming message from %v", instance.id, msg.sender)
		next, err := instance.recvMsg(msg.msg, msg.sender)
		if err != nil {
			break
		}
		return next
	case *RequestBatch://只有主节点才会到这个分支
		err = instance.recvRequestBatch(et)//主节点构造并发送PrePrepare消息。
	case *PrePrepare:
		err = instance.recvPrePrepare(et)//从节点收到PrePrepare消息，验证PrePrepare消息的合法性（主节点身份、序号是否在水线范围内、需要是否小于下一个视图的编号），将PrePrepare消息存到certStore中，并将pre-prepare中的RequestBatch进行持久化存储，随后构造Prepare消息并广播出去
	case *Prepare:
		err = instance.recvPrepare(et)//各个vp节点收到Prepare，验证Prepare消息的合法性（主节点身份、序号是否在水线范围内），将prepare消息存到certStore里面，将prepare消息存储到certStore中，然后更新pset，并持久化pset,然后再maybeSendCommit。
	case *Commit:
		err = instance.recvCommit(et)//各个vp节点收到Commit，验证视图和水线，将prepare消息存到certstore中，如果到达了commited状态，停止newViewTimer计时器，删除outstandingReqBatches中的这个commit中的batch，并执行这个batch中的request。
	case *Checkpoint:
		return instance.recvCheckpoint(et)
	case *ViewChange:
		return instance.recvViewChange(et)
	case *NewView:
		return instance.recvNewView(et)
	case *FetchRequestBatch:
		err = instance.recvFetchRequestBatch(et)
	case returnRequestBatchEvent:
		return instance.recvReturnRequestBatch(et)
	//【本节点其他模块】发送的消息事件
	case stateUpdatedEvent:
		update := et.chkpt
		instance.stateTransferring = false
		// If state transfer did not complete successfully, or if it did not reach our low watermark, do it again
		if et.target == nil || update.seqNo < instance.h {
			if et.target == nil {
				logger.Warningf("Replica %d attempted state transfer target was not reachable (%v)", instance.id, et.chkpt)
			} else {
				logger.Warningf("Replica %d recovered to seqNo %d but our low watermark has moved to %d", instance.id, update.seqNo, instance.h)
			}
			if instance.highStateTarget == nil {
				logger.Debugf("Replica %d has no state targets, cannot resume state transfer yet", instance.id)
			} else if update.seqNo < instance.highStateTarget.seqNo {
				logger.Debugf("Replica %d has state target for %d, transferring", instance.id, instance.highStateTarget.seqNo)
				instance.retryStateTransfer(nil)
			} else {
				logger.Debugf("Replica %d has no state target above %d, highest is %d", instance.id, update.seqNo, instance.highStateTarget.seqNo)
			}
			return nil
		}
		logger.Infof("Replica %d application caught up via state transfer, lastExec now %d", instance.id, update.seqNo)
		instance.lastExec = update.seqNo
		instance.moveWatermarks(instance.lastExec) // The watermark movement handles moving this to a checkpoint boundary
		instance.skipInProgress = false
		instance.consumer.validateState()
		instance.Checkpoint(update.seqNo, update.id)
		instance.executeOutstanding()
	case execDoneEvent:
		instance.execDoneSync()
		if instance.skipInProgress {
			instance.retryStateTransfer(nil)
		}
		// 如果节点当前的执行块的编号currentExec小于最近的稳定检查点编号cp.SequenceNumber，并且在[currentExec,cp.SequenceNumber]之间的requestBatch都到达了committed状态，那么就先执行这些块，之后再处理新视图
		return instance.processNewView()
	case nullRequestEvent:
		instance.nullRequestHandler()
	case workEvent:
		et() // Used to allow the caller to steal use of the main thread, to be removed
	case viewChangeQuorumEvent:
		logger.Debugf("Replica %d received view change quorum, processing new view", instance.id)
		if instance.primary(instance.view) == instance.id {
			return instance.sendNewView()
		}
		return instance.processNewView()
	case viewChangedEvent:
		// No-op, processed by plugins if needed
	case viewChangeResendTimerEvent://ResendTimer定时器超时了，视图编号减1后，重新发送viewChange消息；
		if instance.activeView {
			logger.Warningf("Replica %d had its view change resend timer expire but it's in an active view, this is benign but may indicate a bug", instance.id)
			return nil
		}
		logger.Debugf("Replica %d view change resend timer expired before view change quorum was reached, resending", instance.id)
		instance.view-- // sending the view change increments this
		return instance.sendViewChange()
	default:
		logger.Warningf("Replica %d received an unknown message type %T", instance.id, et)
	}

	if err != nil {
		logger.Warning(err.Error())
	}

	return nil
}

// =============================================================================
// helper functions for PBFT
// =============================================================================

// Given a certain view n, what is the expected primary?
//根据视图序号生成这一轮的primary编号
func (instance *pbftCore) primary(n uint64) uint64 {
	return n % uint64(instance.replicaCount)
}

// Is the sequence number between watermarks?
//验证batch编号是不是在高低水线范围内
func (instance *pbftCore) inW(n uint64) bool {
	return n-instance.h > 0 && n-instance.h <= instance.L
}

// Is the view right? And is the sequence number between watermarks?
//同时验证batch编号和视图需要的合法性
func (instance *pbftCore) inWV(v uint64, n uint64) bool {
	return instance.view == v && instance.inW(n)
}

// Given a digest/view/seq, is there an entry in the certLog?
// If so, return it. If not, create it.
//根据v,n构建msgid,然后获取certstore切片
func (instance *pbftCore) getCert(v uint64, n uint64) (cert *msgCert) {
	idx := msgID{v, n}
	cert, ok := instance.certStore[idx]
	if ok {
		return
	}

	cert = &msgCert{}
	instance.certStore[idx] = cert
	return
}

// =============================================================================
//  quorum计算； prePrepared、Prepared、Committed状态判断
// =============================================================================

// intersectionQuorum returns the number of replicas that have to
// agree to guarantee that at least one correct replica is shared by
// two intersection quora
//根据N和f，计算Quorum
func (instance *pbftCore) intersectionQuorum() int {
	return (instance.N + instance.f + 2) / 2
}

// allCorrectReplicasQuorum returns the number of correct replicas (N-f)
//N-f，共识的时候需要收到N-f个消息后才能进入到下一个状态
func (instance *pbftCore) allCorrectReplicasQuorum() int {
	return (instance.N - instance.f)
}

//判断batch是不是达到prePrepared的状态：先判断是不是在qset里面，再判断是不是在certStore中prePrepare切片里面。
func (instance *pbftCore) prePrepared(digest string, v uint64, n uint64) bool {
	_, mInLog := instance.reqBatchStore[digest]

	if digest != "" && !mInLog {
		return false
	}

	if q, ok := instance.qset[qidx{digest, n}]; ok && q.View == v {
		return true
	}

	cert := instance.certStore[msgID{v, n}]
	if cert != nil {
		p := cert.prePrepare
		if p != nil && p.View == v && p.SequenceNumber == n && p.BatchDigest == digest {
			return true
		}
	}
	logger.Debugf("Replica %d does not have view=%d/seqNo=%d pre-prepared",
		instance.id, v, n)
	return false
}

//判断batch是不是到达了prepared状态：先判断是不是到达了prePrepared状态，然后统计这个batch在certStore中prepare消息的数量，判断是不是到达了Prepared状态
func (instance *pbftCore) prepared(digest string, v uint64, n uint64) bool {
	if !instance.prePrepared(digest, v, n) {
		return false
	}

	if p, ok := instance.pset[n]; ok && p.View == v && p.BatchDigest == digest {
		return true
	}

	quorum := 0
	cert := instance.certStore[msgID{v, n}]
	if cert == nil {
		return false
	}

	for _, p := range cert.prepare {
		if p.View == v && p.SequenceNumber == n && p.BatchDigest == digest {
			quorum++
		}
	}

	logger.Debugf("Replica %d prepare count for view=%d/seqNo=%d: %d",
		instance.id, v, n, quorum)

	return quorum >= instance.intersectionQuorum()-1
}
//判断batch是不是到达了committed状态：先判断是不是到达了Prepared状态，然后统计这个batch在certStore中commit消息的数量，判断是不是到达了committed状态
func (instance *pbftCore) committed(digest string, v uint64, n uint64) bool {
	if !instance.prepared(digest, v, n) {
		return false
	}

	quorum := 0
	cert := instance.certStore[msgID{v, n}]
	if cert == nil {
		return false
	}

	for _, p := range cert.commit {
		if p.View == v && p.SequenceNumber == n {
			quorum++
		}
	}

	logger.Debugf("Replica %d commit count for view=%d/seqNo=%d: %d",
		instance.id, v, n, quorum)

	return quorum >= instance.intersectionQuorum()
}

// =============================================================================
//                     receive methods：对应上面各个case的具体处理函数
// =============================================================================

func (instance *pbftCore) nullRequestHandler() {
	if !instance.activeView {
		return
	}

	if instance.primary(instance.view) != instance.id {
		// backup expected a null request, but primary never sent one
		logger.Info("Replica %d null request timer expired, sending view change", instance.id)
		instance.sendViewChange()
	} else {
		// time for the primary to send a null request
		// pre-prepare with null digest
		logger.Info("Primary %d null request timer expired, sending null request", instance.id)
		instance.sendPrePrepare(nil, "")
	}
}

func (instance *pbftCore) recvMsg(msg *Message, senderID uint64) (interface{}, error) {
	if reqBatch := msg.GetRequestBatch(); reqBatch != nil {
		return reqBatch, nil
	} else if preprep := msg.GetPrePrepare(); preprep != nil {
		if senderID != preprep.ReplicaId {
			return nil, fmt.Errorf("Sender ID included in pre-prepare message (%v) doesn't match ID corresponding to the receiving stream (%v)", preprep.ReplicaId, senderID)
		}
		return preprep, nil
	} else if prep := msg.GetPrepare(); prep != nil {
		if senderID != prep.ReplicaId {
			return nil, fmt.Errorf("Sender ID included in prepare message (%v) doesn't match ID corresponding to the receiving stream (%v)", prep.ReplicaId, senderID)
		}
		return prep, nil
	} else if commit := msg.GetCommit(); commit != nil {
		if senderID != commit.ReplicaId {
			return nil, fmt.Errorf("Sender ID included in commit message (%v) doesn't match ID corresponding to the receiving stream (%v)", commit.ReplicaId, senderID)
		}
		return commit, nil
	} else if chkpt := msg.GetCheckpoint(); chkpt != nil {
		if senderID != chkpt.ReplicaId {
			return nil, fmt.Errorf("Sender ID included in checkpoint message (%v) doesn't match ID corresponding to the receiving stream (%v)", chkpt.ReplicaId, senderID)
		}
		return chkpt, nil
	} else if vc := msg.GetViewChange(); vc != nil {
		if senderID != vc.ReplicaId {
			return nil, fmt.Errorf("Sender ID included in view-change message (%v) doesn't match ID corresponding to the receiving stream (%v)", vc.ReplicaId, senderID)
		}
		return vc, nil
	} else if nv := msg.GetNewView(); nv != nil {
		if senderID != nv.ReplicaId {
			return nil, fmt.Errorf("Sender ID included in new-view message (%v) doesn't match ID corresponding to the receiving stream (%v)", nv.ReplicaId, senderID)
		}
		return nv, nil
	} else if fr := msg.GetFetchRequestBatch(); fr != nil {
		if senderID != fr.ReplicaId {
			return nil, fmt.Errorf("Sender ID included in fetch-request-batch message (%v) doesn't match ID corresponding to the receiving stream (%v)", fr.ReplicaId, senderID)
		}
		return fr, nil
	} else if reqBatch := msg.GetReturnRequestBatch(); reqBatch != nil {
		// it's ok for sender ID and replica ID to differ; we're sending the original request message
		return returnRequestBatchEvent(reqBatch), nil
	}
	return nil, fmt.Errorf("Invalid message: %v", msg)
}

// =============================================================================
//                        request phase
// =============================================================================
//主节点在RequestBatch分支的处理方法：
func (instance *pbftCore) recvRequestBatch(reqBatch *RequestBatch) error {
	digest := hash(reqBatch)
	logger.Debugf("Replica %d received request batch %s", instance.id, digest)
	//1、将requestBatch放到reqBatchStore和outstandingReqBatches里面，并将requestBatch持久化存储；
	instance.reqBatchStore[digest] = reqBatch
	instance.outstandingReqBatches[digest] = reqBatch
	instance.persistRequestBatch(digest)
	//2、没有视图转换，就软启动newViewTimer定时器；
	if instance.activeView {
		instance.softStartTimer(instance.requestTimeout, fmt.Sprintf("new request batch %s", digest))
	}
	//3、主节点合法并且没有视图转换；关闭nullRequestTimer定时器，发送PrePrepare消息；
	if instance.primary(instance.view) == instance.id && instance.activeView {
		instance.nullRequestTimer.Stop()
		instance.sendPrePrepare(reqBatch, digest)
	} else {
		logger.Debugf("Replica %d is backup, not sending pre-prepare for request batch %s", instance.id, digest)
	}
	return nil
}


//主节点发送PrePrepare消息；
func (instance *pbftCore) sendPrePrepare(reqBatch *RequestBatch, digest string) {
	logger.Debugf("Replica %d is primary, issuing pre-prepare for request batch %s", instance.id, digest)

	n := instance.seqNo + 1  //序号+1
	//检查certStore中是否存在着和自己的batch，在视图和batch哈希相同，但是编号不同的情况；
	for _, cert := range instance.certStore { // check for other PRE-PREPARE for same digest, but different seqNo
		if p := cert.prePrepare; p != nil {
			if p.View == instance.view && p.SequenceNumber != n && p.BatchDigest == digest && digest != "" {
				logger.Infof("Other pre-prepare found with same digest but different seqNo: %d instead of %d", p.SequenceNumber, n)
				return
			}
		}
	}

	//检查视图编号，交易序号合法性。
	if !instance.inWV(instance.view, n) || n > instance.h+instance.L/2 {
		// We don't have the necessary stable certificates to advance our watermarks
		logger.Warningf("Primary %d not sending pre-prepare for batch %s - out of sequence numbers", instance.id, digest)
		return
	}

	//判断n是否大于v+1视图时的序号； viewChangeSeqNo是下一个视图的时候，应该取的编号；
	if n > instance.viewChangeSeqNo {
		logger.Info("Primary %d about to switch to next primary, not sending pre-prepare with seqno=%d", instance.id, n)
		return
	}

	logger.Debugf("Primary %d broadcasting pre-prepare for view=%d/seqNo=%d and digest %s", instance.id, instance.view, n, digest)
	//构造Pre-prepare消息
	instance.seqNo = n
	preprep := &PrePrepare{
		View:           instance.view,
		SequenceNumber: n,
		BatchDigest:    digest,
		RequestBatch:   reqBatch,//它放的直接是RequestBatch,没有任何的压缩处理，RBFT放的是交易哈希。
		ReplicaId:      instance.id,
	}
	//将Pre-prepare存到cerStore中
	cert := instance.getCert(instance.view, n)
	cert.prePrepare = preprep
	cert.digest = digest
	//将Pre-prepare存到qset中
	instance.persistQSet()
	//用proto进行编码后，进行广播；
	instance.innerBroadcast(&Message{Payload: &Message_PrePrepare{PrePrepare: preprep}})
	//判断这个requestbatch是不是到达了prepared状态，到达了就构造并发送commit消息。
	instance.maybeSendCommit(digest, instance.view, n)
}
//如果是主节点的话，会遍历outstandingReqBatches中还没进入到共识阶段的requestBatch，然后调用recvRequestBatch(reqBatch)进行处理，
//就是构造PrePrepare消息
func (instance *pbftCore) resubmitRequestBatches() {
	if instance.primary(instance.view) != instance.id {
		return
	}

	var submissionOrder []*RequestBatch

outer:
	for d, reqBatch := range instance.outstandingReqBatches {
		for _, cert := range instance.certStore {
			if cert.digest == d {
				logger.Debugf("Replica %d already has certificate for request batch %s - not going to resubmit", instance.id, d)
				continue outer
			}
		}
		logger.Debugf("Replica %d has detected request batch %s must be resubmitted", instance.id, d)
		submissionOrder = append(submissionOrder, reqBatch)
	}

	if len(submissionOrder) == 0 {
		return
	}

	for _, reqBatch := range submissionOrder {
		// This is a request batch that has not been pre-prepared yet
		// Trigger request batch processing again
		instance.recvRequestBatch(reqBatch)
	}
}

// =============================================================================
//                        Pre-Prepare phase
// =============================================================================
//从节点节点收到PrePrepare后的处理方法；验证PrePrepare消息的合法性（没有验证具体交易的合法性），并将pre-prepare中的RequestBatch进行持久化存储，随后构造Prepare消息并广播出去
func (instance *pbftCore) recvPrePrepare(preprep *PrePrepare) error {
	logger.Debugf("Replica %d received pre-prepare from replica %d for view=%d/seqNo=%d",
		instance.id, preprep.ReplicaId, preprep.View, preprep.SequenceNumber)

	//视图转换的时候，不接收pre-prepare消息
	if !instance.activeView {
		logger.Debugf("Replica %d ignoring pre-prepare as we are in a view change", instance.id)
		return nil
	}

	//验证PrePrepare的合法性：
	//1、验证主节点。不合法返回nil。
	if instance.primary(instance.view) != preprep.ReplicaId {
		logger.Warningf("Pre-prepare from other than primary: got %d, should be %d", preprep.ReplicaId, instance.primary(instance.view))
		return nil
	}
	//2、交易序号是否在水线范围内。不在返回nil。
	if !instance.inWV(preprep.View, preprep.SequenceNumber) {
		if preprep.SequenceNumber != instance.h && !instance.skipInProgress {
			logger.Warningf("Replica %d pre-prepare view different, or sequence number outside watermarks: preprep.View %d, expected.View %d, seqNo %d, low-mark %d", instance.id, preprep.View, instance.primary(instance.view), preprep.SequenceNumber, instance.h)
		} else {
			// This is perfectly normal
			logger.Debugf("Replica %d pre-prepare view different, or sequence number outside watermarks: preprep.View %d, expected.View %d, seqNo %d, low-mark %d", instance.id, preprep.View, instance.primary(instance.view), preprep.SequenceNumber, instance.h)
		}

		return nil
	}
	//3、是否需要强制视图转换。需要，发起视图转换
	if preprep.SequenceNumber > instance.viewChangeSeqNo {
		logger.Info("Replica %d received pre-prepare for %d, which should be from the next primary", instance.id, preprep.SequenceNumber)
		instance.sendViewChange()
		return nil
	}
	//4、根据域编号和序号获取相应的共识信息，
	//   判断是否存在哈requestBatch哈希一致、但序号和域编号不一致的情况。如果存在则发起视图转换。
	cert := instance.getCert(preprep.View, preprep.SequenceNumber)
	if cert.digest != "" && cert.digest != preprep.BatchDigest {
		logger.Warningf("Pre-prepare found for same view/seqNo but different digest: received %s, stored %s", preprep.BatchDigest, cert.digest)
		instance.sendViewChange()
		return nil
	}

	//5、将PrePrepare消息存到Cert里面。
	cert.prePrepare = preprep
	cert.digest = preprep.BatchDigest

	//6、将pre-prepare中的RequestBatch存到reqBatchStore中，并进行持久化存储；
	if _, ok := instance.reqBatchStore[preprep.BatchDigest]; !ok && preprep.BatchDigest != "" {
		digest := hash(preprep.GetRequestBatch())
		if digest != preprep.BatchDigest {
			logger.Warningf("Pre-prepare and request digest do not match: request %s, digest %s", digest, preprep.BatchDigest)
			return nil
		}
		instance.reqBatchStore[digest] = preprep.GetRequestBatch()
		logger.Debugf("Replica %d storing request batch %s in outstanding request batch store", instance.id, digest)
		instance.outstandingReqBatches[digest] = preprep.GetRequestBatch()
		instance.persistRequestBatch(digest)//持久化存储RequestBatch，发生崩溃时，还能够恢复。
	}

	//7、timerActive置为true，newViewTimer定时器设置为requestTimeout时间，停止 nullRequestTimer定时器；
	instance.softStartTimer(instance.requestTimeout, fmt.Sprintf("new pre-prepare for request batch %s", preprep.BatchDigest))
	instance.nullRequestTimer.Stop()

	//8、当这个节点不是primary，已经接受当前请求的prePrepared消息,并且还没发送Prepare消息，就构造Prepare消息并广播出去；
	if instance.primary(instance.view) != instance.id && instance.prePrepared(preprep.BatchDigest, preprep.View, preprep.SequenceNumber) && !cert.sentPrepare {
		logger.Debugf("Backup %d broadcasting prepare for view=%d/seqNo=%d", instance.id, preprep.View, preprep.SequenceNumber)
		prep := &Prepare{
			View:           preprep.View,
			SequenceNumber: preprep.SequenceNumber,
			BatchDigest:    preprep.BatchDigest,
			ReplicaId:      instance.id,
		}
		cert.sentPrepare = true
		//9、将pre-prepare存到Qset里面
		instance.persistQSet()
		//10、调用recvPrepare()方法，处理自己的Prepare消息。
		instance.recvPrepare(prep)
		return instance.innerBroadcast(&Message{Payload: &Message_Prepare{Prepare: prep}})
	}

	return nil
}

// =============================================================================
//                        Prepare phase
// =============================================================================
//vp节点收到PrePare消息后的处理方法：
func (instance *pbftCore) recvPrepare(prep *Prepare) error {
	logger.Debugf("Replica %d received prepare from replica %d for view=%d/seqNo=%d",
		instance.id, prep.ReplicaId, prep.View, prep.SequenceNumber)

	//1、验证主节点
	if instance.primary(prep.View) == prep.ReplicaId {
		logger.Warningf("Replica %d received prepare from primary, ignoring", instance.id)
		return nil
	}

	//2、验证prepare的视图和交易序号是否在水线范围内
	if !instance.inWV(prep.View, prep.SequenceNumber) {
		if prep.SequenceNumber != instance.h && !instance.skipInProgress {
			logger.Warningf("Replica %d ignoring prepare for view=%d/seqNo=%d: not in-wv, in view %d, low water mark %d", instance.id, prep.View, prep.SequenceNumber, instance.view, instance.h)
		} else {
			// This is perfectly normal
			logger.Debugf("Replica %d ignoring prepare for view=%d/seqNo=%d: not in-wv, in view %d, low water mark %d", instance.id, prep.View, prep.SequenceNumber, instance.view, instance.h)
		}
		return nil
	}

	//3、判断自己的certStore中是否已经有了这个节点发送过来的prepare消息，如果没有就存到certStore里面，并进行持久化
	cert := instance.getCert(prep.View, prep.SequenceNumber)
	for _, prevPrep := range cert.prepare {
		if prevPrep.ReplicaId == prep.ReplicaId {
			logger.Warningf("Ignoring duplicate prepare from %d", prep.ReplicaId)
			return nil
		}
	}
	cert.prepare = append(cert.prepare, prep)
	//4、这个函数里面调用了pbftCore.calcPSet(),会根据cerStore.prepare中prepare是否到达了2f个，判断这个消息是否到达了更prepared；
	//如果到达prepared了，那么就将这个消息放到pset里面,并对pset进行持久化保存；
	instance.persistPSet()
	//5、判断这个requestBatch是否到达了Prepared状态，到了就构造并发送commit消息
	return instance.maybeSendCommit(prep.BatchDigest, prep.View, prep.SequenceNumber)
}

//判断这个requestbatch是不是到达了prepared状态，到达了就构造并发送commit消息。
func (instance *pbftCore) maybeSendCommit(digest string, v uint64, n uint64) error {
	cert := instance.getCert(v, n)
	//到达了prepared状态，但是还没发送commit消息，就构造并发送commit消息。
	if instance.prepared(digest, v, n) && !cert.sentCommit {
		logger.Debugf("Replica %d broadcasting commit for view=%d/seqNo=%d",
			instance.id, v, n)
		commit := &Commit{
			View:           v,
			SequenceNumber: n,
			BatchDigest:    digest,
			ReplicaId:      instance.id,
		}
		cert.sentCommit = true
		instance.recvCommit(commit)//这个时候，instance直接进入到了commit阶段。
		return instance.innerBroadcast(&Message{&Message_Commit{commit}})
	}
	return nil
}

// =============================================================================
//                        Commit phase
// =============================================================================
//instance收到Commit消息后，进行的处理方法。
func (instance *pbftCore) recvCommit(commit *Commit) error {
	logger.Debugf("Replica %d received commit from replica %d for view=%d/seqNo=%d",
		instance.id, commit.ReplicaId, commit.View, commit.SequenceNumber)

	//1、检查视图和水线
	if !instance.inWV(commit.View, commit.SequenceNumber) {
		if commit.SequenceNumber != instance.h && !instance.skipInProgress {
			logger.Warningf("Replica %d ignoring commit for view=%d/seqNo=%d: not in-wv, in view %d, low water mark %d", instance.id, commit.View, commit.SequenceNumber, instance.view, instance.h)
		} else {
			// This is perfectly normal
			logger.Debugf("Replica %d ignoring commit for view=%d/seqNo=%d: not in-wv, in view %d, low water mark %d", instance.id, commit.View, commit.SequenceNumber, instance.view, instance.h)
		}
		return nil
	}

	//2、把commit消息存到certStore中，
	cert := instance.getCert(commit.View, commit.SequenceNumber)
	for _, prevCommit := range cert.commit {
		if prevCommit.ReplicaId == commit.ReplicaId {
			logger.Warningf("Ignoring duplicate commit from %d", commit.ReplicaId)
			return nil
		}
	}
	cert.commit = append(cert.commit, commit)

	//3、如果到达了commited 状态，停止newViewTimer计时器，删除outstandingReqBatches中的这个commit中的batch,
	//然后执行这个batch中的request,如果到达了视图转换的时候，强制视图转换；
	if instance.committed(commit.BatchDigest, commit.View, commit.SequenceNumber) {
		instance.stopTimer()
		instance.lastNewViewTimeout = instance.newViewTimeout
		delete(instance.outstandingReqBatches, commit.BatchDigest)

		//执行outstandingReqBatches中到达这committed的requestBatch
		instance.executeOutstanding()

		//如果这个batch的序号等于SequenceNumber，就调用视图转换方法。
		if commit.SequenceNumber == instance.viewChangeSeqNo {
			logger.Infof("Replica %d cycling view for seqNo=%d", instance.id, commit.SequenceNumber)
			instance.sendViewChange()
		}
	}

	return nil
}
//更新highStateTarget字段
func (instance *pbftCore) updateHighStateTarget(target *stateUpdateTarget) {
	if instance.highStateTarget != nil && instance.highStateTarget.seqNo >= target.seqNo {
		logger.Debugf("Replica %d not updating state target to seqNo %d, has target for seqNo %d", instance.id, target.seqNo, instance.highStateTarget.seqNo)
		return
	}

	instance.highStateTarget = target
}
//调用他来进行明确的数据同步操作，这里也不是直接就进行同步，需要在执行完正在执行的requestbatch、进行同步完之后才开始进行新的数据同步
func (instance *pbftCore) stateTransfer(optional *stateUpdateTarget) {
	if !instance.skipInProgress {
		logger.Debugf("Replica %d is out of sync, pending state transfer", instance.id)
		instance.skipInProgress = true
		//Invalidate表明账本已经过期，应该拒绝查询
		instance.consumer.invalidateState()
	}

	instance.retryStateTransfer(optional)
}
//在进行数据同步之前，再检验一下是不是有区块正在执行，是不是正在进行数据同步，都没有才会进行数据同步，
//并把stateTransferring字段设置为true，最终调用其他模块的Executor.UpdateState接口进行状态同步；
func (instance *pbftCore) retryStateTransfer(optional *stateUpdateTarget) {
	if instance.currentExec != nil {
		logger.Debugf("Replica %d is currently mid-execution, it must wait for the execution to complete before performing state transfer", instance.id)
		return
	}

	if instance.stateTransferring {
		logger.Debugf("Replica %d is currently mid state transfer, it must wait for this state transfer to complete before initiating a new one", instance.id)
		return
	}

	target := optional
	if target == nil {
		if instance.highStateTarget == nil {
			logger.Debugf("Replica %d has no targets to attempt state transfer to, delaying", instance.id)
			return
		}
		target = instance.highStateTarget
	}

	instance.stateTransferring = true

	logger.Debugf("Replica %d is initiating state transfer to seqNo %d", instance.id, target.seqNo)
	instance.consumer.skipTo(target.seqNo, target.id, target.replicas)

}

//recvCommit函数里面调用了这个执行requeBatch中的交易的方法；
func (instance *pbftCore) executeOutstanding() {
	//1、有requestBatch正在执行，就返回；
	if instance.currentExec != nil {
		logger.Debugf("Replica %d not attempting to executeOutstanding because it is currently executing %d", instance.id, *instance.currentExec)
		return
	}
	logger.Debugf("Replica %d attempting to executeOutstanding", instance.id)

	//2.遍历certStore中所有的msgCert，找到一个合适的requestBatch执行
	for idx := range instance.certStore {//从certStore里面取出来msgID,就是v、n，然后传给 executeOne。
		if instance.executeOne(idx) {
			break
		}
	}
	logger.Debugf("Replica %d certstore %+v", instance.id, instance.certStore)

	//3. 如果outstandingReqBatches中还有需要待处理的requestBatch,软启动 newViewTimer,如果没有待处理的requestBatch就 重启 nullRequestTimer。
	instance.startTimerIfOutstandingRequests()
}

//执行一个batch的函数，执行的是根据idx从reqBatchStore里面取出来的batch。
func (instance *pbftCore) executeOne(idx msgID) bool {
	//1。通过msgID取到这个requestBatch对应的msgCert。
	cert := instance.certStore[idx]

	//2.判断这个requestBatch的序号是不是"最近"被执行requestBatch的下一个。不是返回nil。
	if idx.n != instance.lastExec+1 || cert == nil || cert.prePrepare == nil {
		return false
	}

	//3. 如果此时节点落后，在同步区块，不执行。
	if instance.skipInProgress {
		logger.Debugf("Replica %d currently picking a starting point to resume, will not execute", instance.id)
		return false
	}

	//4. 开始真正执行：从reqBatchStore里面找到对应的barequestBatch进行执行，
	digest := cert.digest
	reqBatch := instance.reqBatchStore[digest]

	//5. 执行之前判断一下是不是到达了committed状态，在recvCommit()里面其实已经判断过了,所以这里没用。
	if !instance.committed(digest, idx.v, idx.n) {
		return false
	}

	//6. 更新msgCert字段；
	currentExec := idx.n
	instance.currentExec = &currentExec

	//7. null request是心跳信息，此时不需要执行而是直接调用pbftCore.execDoneSync 处理请求已执行完的情况
	if digest == "" {
		logger.Infof("Replica %d executing/committing null request for view=%d/seqNo=%d",
			instance.id, idx.v, idx.n)
		instance.execDoneSync()
	} else {
		logger.Infof("Replica %d executing/committing request batch for view=%d/seqNo=%d and digest %s",
			instance.id, idx.v, idx.n, digest)
		// 8. 如果不是空请求，则调用 pbftCore.consumer 中的 execute 方法执行请求。其中 pbftCore.consumer 实际上指向的是 obcBatch 对象，
		//    因此这里实际上调用的是 obcBatch.execute 方法。
		instance.consumer.execute(idx.n, reqBatch)
	}
	return true
}
//构造并广播Checkpoint消息
func (instance *pbftCore) Checkpoint(seqNo uint64, id []byte) {
	//1、判断当前执行完的requestBatch的序号lastExec字段，是不是k的整数倍；
	if seqNo%instance.K != 0 {
		logger.Errorf("Attempted to checkpoint a sequence number (%d) which is not a multiple of the checkpoint interval (%d)", seqNo, instance.K)
		return
	}

	//2. id是instance.consumer.getState()获得的区块链的当前状态信息；
	idAsString := base64.StdEncoding.EncodeToString(id)

	logger.Debugf("Replica %d preparing checkpoint for view=%d/seqNo=%d and b64 id of %s",
		instance.id, instance.view, seqNo, idAsString)

	//3.构造checkpoint消息，并放到pbftCore的chkpts map里面，并持久化
	chkpt := &Checkpoint{
		SequenceNumber: seqNo,
		ReplicaId:      instance.id,
		Id:             idAsString,
	}
	instance.chkpts[seqNo] = idAsString

	//
	instance.persistCheckpoint(seqNo, id)
	//4. 自己直接调用pbftCore.recvCheckpoint()方法；
	instance.recvCheckpoint(chkpt)
	//5. 将checkpoint消息广播出去；
	instance.innerBroadcast(&Message{Payload: &Message_Checkpoint{Checkpoint: chkpt}})
}
//其他模块执行完成后，会最终调用这个方法；
func (instance *pbftCore) execDoneSync() {
	if instance.currentExec != nil {
		logger.Infof("Replica %d finished execution %d, trying next", instance.id, *instance.currentExec)
		//1. 更新最近被执行完的lrequestBatch的astExec字段
		instance.lastExec = *instance.currentExec
		//2. 判断是否需要进行checkpoint
		if instance.lastExec%instance.K == 0 {
			instance.Checkpoint(instance.lastExec, instance.consumer.getState())
		}

	} else {
		// XXX This masks a bug, this should not be called when currentExec is nil
		logger.Warningf("Replica %d had execDoneSync called, flagging ourselves as out of date", instance.id)
		instance.skipInProgress = true
	}
	//3. 清空当前正在被执行的requestBatch的编号，置为nil
	instance.currentExec = nil

	//4.pnftCore.recvCommit()中就是调用这个函数开始执行交易的，这里再次调用这个方法，就可以在一次只执行一个请求的情况下，将 pbftCore.certStore 中所有可以执行的请求都执行了。
	instance.executeOutstanding()
}
//设置新的水线为当前水线，
//清除certStore、reqBatchStore、checkpointStore、pset和qset中低于新的最低水线的requesBatch，清除chkpts中序号低于我的低水线h的全局状态。
//然后如果这个节点是主节点的话，会遍历outstandingReqBatches中没有开始共识的requestBatch，然后再构造并广播PrePrepare消息；
func (instance *pbftCore) moveWatermarks(n uint64) {
	// round down n to previous low watermark
	h := n / instance.K * instance.K

	//1。清除certStore和reqBatchStore中序号低于我的低水线h的requesBatch。
	for idx, cert := range instance.certStore {
		if idx.n <= h {
			logger.Debugf("Replica %d cleaning quorum certificate for view=%d/seqNo=%d",
				instance.id, idx.v, idx.n)
			instance.persistDelRequestBatch(cert.digest)
			delete(instance.reqBatchStore, cert.digest)
			delete(instance.certStore, idx)
		}
	}
	//2。清除checkpointStore中序号低于我的低水线h的checkpoint。
	for testChkpt := range instance.checkpointStore {
		if testChkpt.SequenceNumber <= h {
			logger.Debugf("Replica %d cleaning checkpoint message from replica %d, seqNo %d, b64 snapshot id %s",
				instance.id, testChkpt.ReplicaId, testChkpt.SequenceNumber, testChkpt.Id)
			delete(instance.checkpointStore, testChkpt)
		}
	}

	//3。清除pset和qset中序号低于我的低水线h的requesBatch。
	for n := range instance.pset {
		if n <= h {
			delete(instance.pset, n)
		}
	}

	for idx := range instance.qset {
		if idx.n <= h {
			delete(instance.qset, idx)
		}
	}
	//4。清除chkpts中序号低于我的低水线h的全局状态。
	for n := range instance.chkpts {
		if n < h {
			delete(instance.chkpts, n)
			instance.persistDelCheckpoint(n)
		}
	}

	//5。设置最低水位为h
	instance.h = h

	logger.Debugf("Replica %d updated low watermark to %d",
		instance.id, instance.h)

	//6。如果这个节点是主节点的话，会遍历outstandingReqBatches中没有开始共识的requestBatch，然后再构造并广播PrePrepare消息；
	instance.resubmitRequestBatches()
}
//判断这个checkpoint是否超过我的高水位，低于我的高水位水线删除hChkpts中对应的这个节点的最高水位值，返回false；
//超出最高水线，更新hChkpts字段中这个checkpoint对应的节点的最高水位，
//然后也根据pbftCore的hChkpts map，统计是否有f+1个节点的最高水线大于我的水线，有的话，移动水线更新相关缓存，并将skipInProgress置为true，然后返回true；
func (instance *pbftCore) weakCheckpointSetOutOfRange(chkpt *Checkpoint) bool {
	H := instance.h + instance.L

	// Track the last observed checkpoint sequence number if it exceeds our high watermark, keyed by replica to prevent unbounded growth
	//1.判断新的 checkpoint 中的序号是否小于我们目前的上限 H，如果小于则将新的 checkpoint 的发送者信息从 pbftCore.hChkpts 中删除。
	// pbftCore.hChkpts 这个字段记录了所有我们收集到的、序号超过我们上限 H 的 checkpoint 信息。
	if chkpt.SequenceNumber < H {
		// For non-byzantine nodes, the checkpoint sequence number increases monotonically
		delete(instance.hChkpts, chkpt.ReplicaId)
	} else {
		// We do not track the highest one, as a byzantine node could pick an arbitrarilly high sequence number
		// and even if it recovered to be non-byzantine, we would still believe it to be far ahead
		// 2. checkpoint的的序号大于H，就更新pbftCore中hChkpts map中这个节点的对应的最高检查点序号；
		instance.hChkpts[chkpt.ReplicaId] = chkpt.SequenceNumber

		// If f+1 other replicas have reported checkpoints that were (at one time) outside our watermarks
		// we need to check to see if we have fallen behind.
		//3. 如果有f+1个节点发送过来的checkpoint的序号大于我的最高水位H，说明我落后了，把 pbftCore.hChkpts 中记录的序号全部移到 chkptSeqNumArray 中并将其从低到高排序;
		if len(instance.hChkpts) >= instance.f+1 {
			chkptSeqNumArray := make([]uint64, len(instance.hChkpts))
			index := 0
			for replicaID, hChkpt := range instance.hChkpts {
				chkptSeqNumArray[index] = hChkpt
				index++
				if hChkpt < H {
					delete(instance.hChkpts, replicaID)
				}
			}
			sort.Sort(sortableUint64Slice(chkptSeqNumArray))

			// If f+1 nodes have issued checkpoints above our high water mark, then
			// we will never record 2f+1 checkpoints for that sequence number, we are out of date
			// (This is because all_replicas - missed - me = 3f+1 - f - 1 = 2f)
			//4. 如果 chkptSeqNumArray 中的后 f+1 个序号中最小的值大于 H，则可以说明 chkptSeqNumArray 中至少有 f+1 个序号是大于 H 的。说明我落后了。
			if m := chkptSeqNumArray[len(chkptSeqNumArray)-(instance.f+1)]; m > H {
				logger.Warningf("Replica %d is out of date, f+1 nodes agree checkpoint with seqNo %d exists but our high water mark is %d", instance.id, chkpt.SequenceNumber, H)
				instance.reqBatchStore = make(map[string]*RequestBatch) // Discard all our requests, as we will never know which were executed, to be addressed in #394
				instance.persistDelAllRequestBatches()
				//清除certStore、reqBatchStore、checkpointStore、pset和qset中低于新的最低水线的requesBatch，清除chkpts中序号低于我的低水线h的全局状态。
				//设置新的水线为当前水线，然后如果这个节点是主节点的话，会遍历outstandingReqBatches中没有开始共识的requestBatch，然后再构造并广播PrePrepare消息；
				instance.moveWatermarks(m)
				instance.outstandingReqBatches = make(map[string]*RequestBatch)
				//移动完水线后，将skipInProgress置为true，表明自己落后了。
				instance.skipInProgress = true
				instance.consumer.invalidateState()
				instance.stopTimer()

				// TODO, reprocess the already gathered checkpoints, this will make recovery faster, though it is presently correct

				return true
			}
		}
	}

	return false
}
//就是更新一下highStateTarget字段，
func (instance *pbftCore) witnessCheckpointWeakCert(chkpt *Checkpoint) {
	checkpointMembers := make([]uint64, instance.f+1) // Only ever invoked for the first weak cert, so guaranteed to be f+1
	i := 0//是witness
	for testChkpt := range instance.checkpointStore {
		if testChkpt.SequenceNumber == chkpt.SequenceNumber && testChkpt.Id == chkpt.Id {
			checkpointMembers[i] = testChkpt.ReplicaId
			logger.Debugf("Replica %d adding replica %d (handle %v) to weak cert", instance.id, testChkpt.ReplicaId, checkpointMembers[i])
			i++
		}
	}

	snapshotID, err := base64.StdEncoding.DecodeString(chkpt.Id)
	if nil != err {
		err = fmt.Errorf("Replica %d received a weak checkpoint cert which could not be decoded (%s)", instance.id, chkpt.Id)
		logger.Error(err.Error())
		return
	}

	target := &stateUpdateTarget{
		checkpointMessage: checkpointMessage{
			seqNo: chkpt.SequenceNumber,
			id:    snapshotID,
		},
		replicas: checkpointMembers,
	}
	instance.updateHighStateTarget(target)

	if instance.skipInProgress {
		logger.Debugf("Replica %d is catching up and witnessed a weak certificate for checkpoint %d, weak cert attested to by %d of %d (%v)",
			instance.id, chkpt.SequenceNumber, i, instance.replicaCount, checkpointMembers)
		// The view should not be set to active, this should be handled by the yet unimplemented SUSPECT, see https://github.com/hyperledger/fabric/issues/1120
		instance.retryStateTransfer(target)
	}
}
//vp节点收到检checkpoint消息后进行处理；
func (instance *pbftCore) recvCheckpoint(chkpt *Checkpoint) events.Event {
	logger.Debugf("Replica %d received checkpoint from replica %d, seqNo %d, digest %s",
		instance.id, chkpt.ReplicaId, chkpt.SequenceNumber, chkpt.Id)

	//1. 判断这个checkpoint是否大于我的高水位，
	//低于我的高水位水线删除hChkpts中这个checkpoint对应的这个节点的最高水位值，返回false，继续往下执行；
	//超出最高水线，更新hChkpts字段中这个checkpoint对应的节点的最高水位，
	//  然后也根据pbftCore的hChkpts map，统计是否有f+1个节点的最高水线大于我的水线，有的话，说明我落后了，
	//  移动水线更新相关缓存，并将skipInProgress置为true进行同步，然后返回true；这样到这里就结束了。
	if instance.weakCheckpointSetOutOfRange(chkpt) {
		return nil
	}
    //到这里说明我没落后，这个checkpoint的序号小于我的最高水线，继续往下处理
    //2. 如果这个checkpoint不在我的水线范围内（其实是小于我的最低水位），打印日志，返回nil。
	if !instance.inW(chkpt.SequenceNumber) {
		if chkpt.SequenceNumber != instance.h && !instance.skipInProgress {
			// It is perfectly normal that we receive checkpoints for the watermark we just raised, as we raise it after 2f+1, leaving f replies left
			logger.Warningf("Checkpoint sequence number outside watermarks: seqNo %d, low-mark %d", chkpt.SequenceNumber, instance.h)
		} else {
			logger.Debugf("Checkpoint sequence number outside watermarks: seqNo %d, low-mark %d", chkpt.SequenceNumber, instance.h)
		}
		return nil
	}

	//3. 到这里说明这个checkpoint在我的水线范围内
	//将这个checkpoint存到checkpointStore中；
	instance.checkpointStore[*chkpt] = true


	//diffValues 则代表序号相同但 ID(状态)不同的 checkpoint信息；
	diffValues := make(map[string]struct{})
	diffValues[chkpt.Id] = struct{}{}
	//matching 变量代表当前保存的 checkpoint消息中，与新接收到的 checkpoint中的 Sequence Number和 ID（区块链状态都相同的 checkpoint的数量。
	matching := 0

	//4. 从pbftCore.checkpointStore中，找出所有序号相同且数据ID相同的checkpoint的数量，同时找出所有序号相同但数据ID不同的ID。
	for testChkpt := range instance.checkpointStore {
		if testChkpt.SequenceNumber == chkpt.SequenceNumber {
			if testChkpt.Id == chkpt.Id {
				matching++
			} else {
				if _, ok := diffValues[testChkpt.Id]; !ok {
					diffValues[testChkpt.Id] = struct{}{}
				}
			}
		}
	}
	logger.Debugf("Replica %d found %d matching checkpoints for seqNo %d, digest %s",
		instance.id, matching, chkpt.SequenceNumber, chkpt.Id)

	//5. 如果diffValues的长度大于f+1,也即序号相同、但state数据的ID不同的checkpoint个数大于f+1，说明结点间的状态并不一致，这是一个严重的错误，因此立即调用panic函数停止运行。
	if count := len(diffValues); count > instance.f+1 {
		logger.Panicf("Network unable to find stable certificate for seqNo %d (%d different values observed already)",
			chkpt.SequenceNumber, count)
	}

	//6. 弱检查点的处理
	//如果有f+1个结点的checkpoint的序号和ID是相同的，但自己产生的、相同序号的checkpoint却有不同的ID，那么肯定是自己搞错了,用panic函数停止运行。
	if matching == instance.f+1 {
		if ownChkptID, ok := instance.chkpts[chkpt.SequenceNumber]; ok {
			if ownChkptID != chkpt.Id {
				logger.Panicf("Own checkpoint for seqNo %d (%s) different from weak checkpoint certificate (%s)",
					chkpt.SequenceNumber, ownChkptID, chkpt.Id)
			}
		}

		//如果是一致的，更新同步目标highStateTarget字段，表示要同步到这个序号。
		instance.witnessCheckpointWeakCert(chkpt)
	}

	//如果没收集到2f+1个checkpoint消息，返回nil
	if matching < instance.intersectionQuorum() {
		// We do not have a quorum yet
		return nil
	}

	//7. 稳定检查点处理
	//到这里说明有超过2f+1个节点产生了相同的checkpoint，这样就产生了稳定检查点；参数chkpt代表了一个稳定检查点；

	//这里的if判断说明，我们的chkpts中还没有这个稳定检查点，说明我们落后了；
		//我们落后了，并且我们已经开始同步了，如果节点的稳定检查点大于我这个节点水线[h,H]的中值，说明我已经落后好多了，
		//这里调用moveWatermarks()方法，设置我的最低水线pbftCore.h为最新的稳定检查点的序号，它大于[h,H]的中值；
		//注意我们当前的状态是在同步state数据，所以更新完pbftCore.h后且此次数据更新完成后，会检查检查最新的数据是不是小于pbftCore.h，
		//如果小于则走pbftCore.ProcessEvent()的stateUpdatedEvent分支，继续同步状态；
	if _, ok := instance.chkpts[chkpt.SequenceNumber]; !ok {
		logger.Debugf("Replica %d found checkpoint quorum for seqNo %d, digest %s, but it has not reached this checkpoint itself yet",
			instance.id, chkpt.SequenceNumber, chkpt.Id)

		if instance.skipInProgress {
			//logSafetyBound是[h,H]的中值；
			logSafetyBound := instance.h + instance.L/2
			// As an optimization, if we are more than half way out of our log and in state transfer, move our watermarks so we don't lose track of the network
			// if needed, state transfer will restart on completion to a more recent point in time

			if chkpt.SequenceNumber >= logSafetyBound {
				logger.Debugf("Replica %d is in state transfer, but, the network seems to be moving on past %d, moving our watermarks to stay with it", instance.id, logSafetyBound)
				instance.moveWatermarks(chkpt.SequenceNumber)
			}
		}
		//我们落后了还没开始同步，就返回nil，然后其他部分会发现落后，并进行同步处理的。
		return nil
	}

	logger.Debugf("Replica %d found checkpoint quorum for seqNo %d, digest %s",
		instance.id, chkpt.SequenceNumber, chkpt.Id)

	//8. 这里说明收到一个稳定检查点，并且我自己也生成了这个稳定检查点，就直接更新自己的最低水线为当前检查点。
	instance.moveWatermarks(chkpt.SequenceNumber)
	//9. 然后直接执行新视图处理方法；
	return instance.processNewView()
}

// used in view-change to fetch missing assigned, non-checkpointed requests
func (instance *pbftCore) fetchRequestBatches() (err error) {
	var msg *Message
	for digest := range instance.missingReqBatches {
		msg = &Message{Payload: &Message_FetchRequestBatch{FetchRequestBatch: &FetchRequestBatch{
			BatchDigest: digest,
			ReplicaId:   instance.id,
		}}}
		instance.innerBroadcast(msg)
	}

	return
}

func (instance *pbftCore) recvFetchRequestBatch(fr *FetchRequestBatch) (err error) {
	digest := fr.BatchDigest
	if _, ok := instance.reqBatchStore[digest]; !ok {
		return nil // we don't have it either
	}

	reqBatch := instance.reqBatchStore[digest]
	msg := &Message{Payload: &Message_ReturnRequestBatch{ReturnRequestBatch: reqBatch}}
	msgPacked, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("Error marshalling return-request-batch message: %v", err)
	}

	receiver := fr.ReplicaId
	err = instance.consumer.unicast(msgPacked, receiver)

	return
}

func (instance *pbftCore) recvReturnRequestBatch(reqBatch *RequestBatch) events.Event {
	digest := hash(reqBatch)
	if _, ok := instance.missingReqBatches[digest]; !ok {
		return nil // either the wrong digest, or we got it already from someone else
	}
	instance.reqBatchStore[digest] = reqBatch
	delete(instance.missingReqBatches, digest)
	instance.persistRequestBatch(digest)
	return instance.processNewView()
}

// =============================================================================
// Misc. methods go here
// =============================================================================

// Marshals a Message and hands it to the Stack. If toSelf is true,
// the message is also dispatched to the local instance's RecvMsgSync.
func (instance *pbftCore) innerBroadcast(msg *Message) error {
	//将msg用proto进行二进制编码；
	msgRaw, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("Cannot marshal message %s", err)
	}

	doByzantine := false
	//测试用的
	if instance.byzantine {
		rand1 := rand.New(rand.NewSource(time.Now().UnixNano()))
		doIt := rand1.Intn(3) // go byzantine about 1/3 of the time
		if doIt == 1 {
			doByzantine = true
		}
	}

	// testing byzantine fault.
	if doByzantine {
		rand2 := rand.New(rand.NewSource(time.Now().UnixNano()))
		ignoreidx := rand2.Intn(instance.N)
		for i := 0; i < instance.N; i++ {
			if i != ignoreidx && uint64(i) != instance.id { //Pick a random replica and do not send message
				instance.consumer.unicast(msgRaw, uint64(i))
			} else {
				logger.Debugf("PBFT byzantine: not broadcasting to replica %v", i)
			}
		}
	} else {//正常逻辑：将编码后的msg广播出去。
		instance.consumer.broadcast(msgRaw)
	}
	return nil
}

func (instance *pbftCore) updateViewChangeSeqNo() {
	if instance.viewChangePeriod <= 0 {
		return
	}
	// Ensure the view change always occurs at a checkpoint boundary
	instance.viewChangeSeqNo = instance.seqNo + instance.viewChangePeriod*instance.K - instance.seqNo%instance.K
	logger.Debugf("Replica %d updating view change sequence number to %d", instance.id, instance.viewChangeSeqNo)
}

//如果outstandingReqBatches中还有requestBatch，软启动newViewTimer，否则就重启nullRequestTimer定时器
func (instance *pbftCore) startTimerIfOutstandingRequests() {
	if instance.skipInProgress || instance.currentExec != nil {
		// Do not start the view change timer if we are executing or state transferring, these take arbitrarilly long amounts of time
		return
	}

	if len(instance.outstandingReqBatches) > 0 {
		getOutstandingDigests := func() []string {
			var digests []string
			for digest := range instance.outstandingReqBatches {
				digests = append(digests, digest)
			}
			return digests
		}()
		instance.softStartTimer(instance.requestTimeout, fmt.Sprintf("outstanding request batches %v", getOutstandingDigests))
	} else if instance.nullRequestTimeout > 0 {
		timeout := instance.nullRequestTimeout
		if instance.primary(instance.view) != instance.id {
			// we're waiting for the primary to deliver a null request - give it a bit more time
			timeout += instance.requestTimeout
		}
		instance.nullRequestTimer.Reset(timeout, nullRequestEvent{})
	}
}

//软启动newViewTimer定时器，不会清空pending events。
func (instance *pbftCore) softStartTimer(timeout time.Duration, reason string) {
	logger.Debugf("Replica %d soft starting new view timer for %s: %s", instance.id, timeout, reason)
	instance.newViewTimerReason = reason
	instance.timerActive = true
	instance.newViewTimer.SoftReset(timeout, viewChangeTimerEvent{})
}
//强制重新设置视图转换newViewTimer定时器，即使定时器正在运行，并清空pending queue中的事件；
func (instance *pbftCore) startTimer(timeout time.Duration, reason string) {
	logger.Debugf("Replica %d starting new view timer for %s: %s", instance.id, timeout, reason)
	instance.timerActive = true
	instance.newViewTimer.Reset(timeout, viewChangeTimerEvent{})
}
//关闭newTimer定时器
func (instance *pbftCore) stopTimer() {
	logger.Debugf("Replica %d stopping a running new view timer", instance.id)
	instance.timerActive = false
	instance.newViewTimer.Stop()
}
