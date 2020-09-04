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
	"fmt"
	"time"

	"github.com/hyperledger/fabric/consensus"
	"github.com/hyperledger/fabric/consensus/util/events"
	pb "github.com/hyperledger/fabric/protos"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/op/go-logging"
	"github.com/spf13/viper"
)
//obcBatch 对象是在一个稍高的层次上实现了 pbft 模块的共识接口 consensus.Consenter，所以 Consenter 接口肯定也是它实现的
type obcBatch struct {
	obcGeneric  //封装了consensus.Stack接口，*pbftCore结构体的引用；
	externalEventReceiver//实现了共识模块对外接口 Consenter，也封装了events.Manager接口；

	pbft        *pbftCore//这部分是共识模块的核心，实现了共识算法逻辑；
	broadcaster *broadcaster  //广播者结构体：含有广播、单播方法，有返回网络中的节点的IP、端口、类型vp nvp、公钥、PeerID的方法；

	batchSize        int//主节点进行打包的交易数量阈值，request数满足这个数量就打包；
	batchTimer       events.Timer //主节点打包定时器，即使没到交易数量，到时间也打包；
	batchTimerActive bool   //batchTimer定时器是否开启的标志位
	batchTimeout     time.Duration//batchTimer定时器的时间阈值

	manager events.Manager //接收事件模块的接口，由mangerImpl实现；
	incomingChan chan *batchMessage //Queues messages for processing by main thread
	idleChan     chan struct{}      //Idle channel, to be removed

	batchStore       []*Request//主节点才用的打包池，存打包一个batch的request（他的规模是最小的），产生requestBatch就清空他；
	reqStore *requestStore //主从节点都用，有outstandingRequests，pendingRequests，
	// 用来存outstanding交易(指收到的所有的request，他的范围是比pending 大的)和pending交易（他是已经打包的request）；
	deduplicator *deduplicator  //去重器:含有reqTimestamps、execTimestamps两个map；
	persistForward//实现了状态持久化接口，Stack.StatePersistor接口；
}

type batchMessage struct {
	msg    *pb.Message
	sender *pb.PeerID
}

// Event types

// batchMessageEvent is sent when a consensus message is received that is then to be sent to pbft
type batchMessageEvent batchMessage

// batchTimerEvent is sent when the batch timer expires
type batchTimerEvent struct{}
//初始化一个obcBatch
func newObcBatch(id uint64, config *viper.Viper, stack consensus.Stack) *obcBatch {
	var err error

	op := &obcBatch{
		obcGeneric: obcGeneric{stack: stack},
	}

	op.persistForward.persistor = stack

	logger.Debugf("Replica %d obtaining startup information", id)

	op.manager = events.NewManagerImpl() //创建manger接口实例；
	op.manager.SetReceiver(op)//这里将obcBatch设置为manger接口实例中的receiver；
	etf := events.NewTimerFactoryImpl(op.manager)
	op.pbft = newPbftCore(id, config, op, etf)//pbftCore.consumer就是obcBatch
	op.manager.Start()//开始持续监听manger.events通道中的事件；
	blockchainInfoBlob := stack.GetBlockchainInfoBlob()
	op.externalEventReceiver.manager = op.manager//将manager实例赋值给externalEventReceiver中的manger；
	op.broadcaster = newBroadcaster(id, op.pbft.N, op.pbft.f, op.pbft.broadcastTimeout, stack)
	op.manager.Queue() <- workEvent(func() {//？
		op.pbft.stateTransfer(&stateUpdateTarget{
			checkpointMessage: checkpointMessage{
				seqNo: op.pbft.lastExec,
				id:    blockchainInfoBlob,
			},
		})
	})

	op.batchSize = config.GetInt("general.batchsize")
	op.batchStore = nil
	op.batchTimeout, err = time.ParseDuration(config.GetString("general.timeout.batch"))
	if err != nil {
		panic(fmt.Errorf("Cannot parse batch timeout: %s", err))
	}
	logger.Infof("PBFT Batch size = %d", op.batchSize)
	logger.Infof("PBFT Batch timeout = %v", op.batchTimeout)

	if op.batchTimeout >= op.pbft.requestTimeout {
		op.pbft.requestTimeout = 3 * op.batchTimeout / 2
		logger.Warningf("Configured request timeout must be greater than batch timeout, setting to %v", op.pbft.requestTimeout)
	}

	if op.pbft.requestTimeout >= op.pbft.nullRequestTimeout && op.pbft.nullRequestTimeout != 0 {
		op.pbft.nullRequestTimeout = 3 * op.pbft.requestTimeout / 2
		logger.Warningf("Configured null request timeout must be greater than request timeout, setting to %v", op.pbft.nullRequestTimeout)
	}

	op.incomingChan = make(chan *batchMessage)

	op.batchTimer = etf.CreateTimer()

	op.reqStore = newRequestStore()

	op.deduplicator = newDeduplicator()

	op.idleChan = make(chan struct{})
	close(op.idleChan) // TODO remove eventually

	return op
}

// Close tells us to release resources we are holding
func (op *obcBatch) Close() {
	op.batchTimer.Halt()
	op.pbft.close()
}
//广播request，将request存到outstandingRequests中，没有进行同步、没有执行交易、没有视图转换并且outstandingRequests中有未处理完的 request，就软开启newViewTimer
//如果是主节点，那么判断是否到达打包阈值，到了就打包成 requestBatch;
func (op *obcBatch) submitToLeader(req *Request) events.Event {
	// Broadcast the request to the network, in case we're in the wrong view
	//广播交易，这是为了在自己成为主节点后，也能够打包交易到batch里面
	op.broadcastMsg(&BatchMessage{Payload: &BatchMessage_Request{Request: req}})
	op.logAddTxFromRequest(req)
	//存到outstandingRequests中
	op.reqStore.storeOutstanding(req)
	//如果没有在进行数据同步，没有在执行交易，没有在视图转换，并且outstandingRequests中有未处理完的 request，就软开启newViewTimer
	op.startTimerIfOutstandingRequests()
	//如果是主节点并且没有在视图转换；
	//主节点在收到request后，将不重复的request存到obcBatch.batchStore中，在满足打包阈值后打包发送requestBatch
	if op.pbft.primary(op.pbft.view) == op.pbft.id && op.pbft.activeView {
		return op.leaderProcReq(req)
	}
	return nil
}

func (op *obcBatch) broadcastMsg(msg *BatchMessage) {
	msgPayload, _ := proto.Marshal(msg)
	ocMsg := &pb.Message{
		Type:    pb.Message_CONSENSUS,
		Payload: msgPayload,
	}
	op.broadcaster.Broadcast(ocMsg)
}

// send a message to a specific replica
func (op *obcBatch) unicastMsg(msg *BatchMessage, receiverID uint64) {
	msgPayload, _ := proto.Marshal(msg)
	ocMsg := &pb.Message{
		Type:    pb.Message_CONSENSUS,
		Payload: msgPayload,
	}
	op.broadcaster.Unicast(ocMsg, receiverID)
}

// =============================================================================
// innerStack interface (functions called by pbft-core)
// =============================================================================

// multicast a message to all replicas
func (op *obcBatch) broadcast(msgPayload []byte) {
	op.broadcaster.Broadcast(op.wrapMessage(msgPayload))
}

// send a message to a specific replica
func (op *obcBatch) unicast(msgPayload []byte, receiverID uint64) (err error) {
	return op.broadcaster.Unicast(op.wrapMessage(msgPayload), receiverID)
}

func (op *obcBatch) sign(msg []byte) ([]byte, error) {
	return op.stack.Sign(msg)
}

// verify message signature
func (op *obcBatch) verify(senderID uint64, signature []byte, message []byte) error {
	senderHandle, err := getValidatorHandle(senderID)
	if err != nil {
		return err
	}
	return op.stack.Verify(senderHandle, signature, message)
}

// execute an opaque request which corresponds to an OBC Transaction
func (op *obcBatch) execute(seqNo uint64, reqBatch *RequestBatch) {
	var txs []*pb.Transaction
	//1. 通过for循环，将request转为Transaction
	for _, req := range reqBatch.GetBatch() {
		tx := &pb.Transaction{}
		if err := proto.Unmarshal(req.Payload, tx); err != nil {
			logger.Warningf("Batch replica %d could not unmarshal transaction %s", op.pbft.id, err)
			continue
		}
		logger.Debugf("Batch replica %d executing request with transaction %s from outstandingReqs, seqNo=%d", op.pbft.id, tx.Txid, seqNo)
		if outstanding, pending := op.reqStore.remove(req); !outstanding || !pending {
			logger.Debugf("Batch replica %d missing transaction %s outstanding=%v, pending=%v", op.pbft.id, tx.Txid, outstanding, pending)
		}
		txs = append(txs, tx)
		// 验证request的时间戳是不是大于最近刚被执行的request的时间戳。如果满足条件就更新这个节点的execTimestamps，并返回true。
		op.deduplicator.Execute(req)
	}
	meta, _ := proto.Marshal(&Metadata{seqNo})
	logger.Debugf("Batch replica %d received exec for seqNo %d containing %d transactions", op.pbft.id, seqNo, len(txs))
	//2. 调用 Executor 接口的 Execute 方法执行请求。其中 Executor 接口是由 Fabric 其它模块实现的。
	op.stack.Execute(meta, txs) // This executes in the background, we will receive an executedEvent once it completes
	//3. 其它模块执行完请求后还会调用 ExecutionConsumer 接口的 Executed 方法通知请求执行完成；
	//实现这一接口的是 externalEventReceiver.Executed 方法（也即 obcBatch.Executed），
	//它会将一个 executedEvent 消息事件插入消息队列中；最终的处理会进入到 pbftCore.ProcessEvent 方法中的 execDoneEvent 分支中，
	//这个分支中目前我们关心的是它调用 pbftCore.execDoneSync 方法，所以我们来看一下这个方法：
}

// =============================================================================
// functions specific to batch mode
// =============================================================================

//主节点在收到request后，将不重复的request存到obcBatch.batchStore中，在满足打包阈值后打包发送requestBatch
func (op *obcBatch) leaderProcReq(req *Request) events.Event {
	// XXX check req sig
	digest := hash(req)
	logger.Debugf("Batch primary %d queueing new request %s", op.pbft.id, digest)
	op.batchStore = append(op.batchStore, req)
	op.reqStore.storePending(req)

	if !op.batchTimerActive {
		op.startBatchTimer()
	}

	if len(op.batchStore) >= op.batchSize {
		return op.sendBatch()
	}

	return nil
}
//停止batchTimer定时器，清空batchStore；构造并返回RequestBatch；这个时候他会走obcBatch.default分支，进入到pbftCore.ProcessEvent()
func (op *obcBatch) sendBatch() events.Event {
	op.stopBatchTimer()
	if len(op.batchStore) == 0 {
		logger.Error("Told to send an empty batch store for ordering, ignoring")
		return nil
	}

	reqBatch := &RequestBatch{Batch: op.batchStore}
	op.batchStore = nil
	logger.Infof("Creating batch with %d requests", len(reqBatch.Batch))
	return reqBatch
}

func (op *obcBatch) txToReq(tx []byte) *Request {
	now := time.Now()
	req := &Request{
		Timestamp: &timestamp.Timestamp{
			Seconds: now.Unix(),
			Nanos:   int32(now.UnixNano() % 1000000000),
		},
		Payload:   tx,
		ReplicaId: op.pbft.id,
	}
	// XXX sign req
	return req
}
//处理【其他节点】发送过来的消息
func (op *obcBatch) processMessage(ocMsg *pb.Message, senderHandle *pb.PeerID) events.Event {
	//【其他节点】发送过来的消息共有三种类型：Message_CHAIN_TRANSACTION（交易）、Request（请求）、PbftMessage（共识消息）
	//第一种：Message_CHAIN_TRANSACTION，将被构造为Request；
	if ocMsg.Type == pb.Message_CHAIN_TRANSACTION {
		req := op.txToReq(ocMsg.Payload)//将tx构造为request
		return op.submitToLeader(req)//将request广播，并存储到自己的outstandingRequests中，如果是主节点就判断是否到了打包阈值
	}
	if ocMsg.Type != pb.Message_CONSENSUS {
		logger.Errorf("Unexpected message type: %s", ocMsg.Type)
		return nil
	}
	//将消息解码变成BatchMessage；BatchMessage包含如下四种类型
	//	*BatchMessage_Request// 走下面第二种类型，
	//	*BatchMessage_RequestBatch// 包含在BatchMessage_PbftMessage里面，所以走下面第三种类型
	//	*BatchMessage_PbftMessage//走下面第三种分支
	//	*BatchMessage_Complaint
	batchMsg := &BatchMessage{}
	err := proto.Unmarshal(ocMsg.Payload, batchMsg)
	if err != nil {
		logger.Errorf("Error unmarshaling message: %s", err)
		return nil
	}
	//第二种：Request，再进行去重验证后，加入outstandingRequests，  然后设置打包数量和时间阈值，到时生成brequestBatch
	if req := batchMsg.GetRequest(); req != nil {
		//进行去重判断，要是重复了，那么就直接return nil
		if !op.deduplicator.IsNew(req) {
			logger.Warningf("Replica %d ignoring request as it is too old", op.pbft.id)
			return nil
		}
		op.logAddTxFromRequest(req)
		//收到从其他节点发送过来的request，也存到outstandingRequests里面；
		op.reqStore.storeOutstanding(req)
        //同样，主节点的话，调用leaderProcReq，判断是否需要进行打包出块；
		if (op.pbft.primary(op.pbft.view) == op.pbft.id) && op.pbft.activeView {
			return op.leaderProcReq(req)
		}
		op.startTimerIfOutstandingRequests()
		return nil
	//第三种：pbftMessage类型消息，也就是其他节点发送过来的共识消息；
	} else if pbftMsg := batchMsg.GetPbftMessage(); pbftMsg != nil {
		senderID, err := getValidatorID(senderHandle) // who sent this?
		if err != nil {
			panic("Cannot map sender's PeerID to a valid replica ID")
		}
		msg := &Message{}
		err = proto.Unmarshal(pbftMsg, msg)
		if err != nil {
			logger.Errorf("Error unpacking payload from message: %s", err)
			return nil
		}
		//RequestBatch、PrePrepare、Prepare、Prepare、Checkpoint、ViewChange、NewView、FetchRequestBatch、FetchRequestBatch
		//都被封装为pbftMessageEvent事件，发送到obcBatch.manger.events事件队列，然后走到
		return pbftMessageEvent{//返回
			msg:    msg,
			sender: senderID,
		}
	}

	logger.Errorf("Unknown request: %+v", batchMsg)

	return nil
}

func (op *obcBatch) logAddTxFromRequest(req *Request) {
	if logger.IsEnabledFor(logging.DEBUG) {
		// This is potentially a very large expensive debug statement, guard
		tx := &pb.Transaction{}
		err := proto.Unmarshal(req.Payload, tx)
		if err != nil {
			logger.Errorf("Replica %d was sent a transaction which did not unmarshal: %s", op.pbft.id, err)
		} else {
			logger.Debugf("Replica %d adding request from %d with transaction %s into outstandingReqs", op.pbft.id, req.ReplicaId, tx.Txid)
		}
	}
}
//如果节点是主节点，找出来n个在outstandingRequests中，但是不在pendingRequests中的 request,用来构造一个新的块，并直接调用obcBatch.manager.Inject(msg)
func (op *obcBatch) resubmitOutstandingReqs() events.Event {
	op.startTimerIfOutstandingRequests()

	// If we are the primary, and know of outstanding requests, submit them for inclusion in the next batch until
	// we run out of requests, or a new batch message is triggered (this path will re-enter after execution)
	// Do not enter while an execution is in progress to prevent duplicating a request
	if op.pbft.primary(op.pbft.view) == op.pbft.id && op.pbft.activeView && op.pbft.currentExec == nil {
		needed := op.batchSize - len(op.batchStore)

		for op.reqStore.hasNonPending() {
			outstanding := op.reqStore.getNextNonPending(needed)

			// If we have enough outstanding requests, this will trigger a batch
			for _, nreq := range outstanding {
				if msg := op.leaderProcReq(nreq); msg != nil {
					op.manager.Inject(msg)
				}
			}
		}
	}
	return nil
}

// obcBatch会持续监控obcBatch.manger.events事件队列中的事件，有事件就调用obcBatch.ProcessEvent;
func (op *obcBatch) ProcessEvent(event events.Event) events.Event {
	logger.Debugf("Replica %d batch main thread looping", op.pbft.id)
	switch et := event.(type) {
	//【其他节点】发送过来的消息事件（都被封装成了batchMessageEvent）都进入到这个分支进行处理，将调用processMessage()；
	case batchMessageEvent:
		ocMsg := et
		return op.processMessage(ocMsg.msg, ocMsg.sender)

	//【本节点其他模块】产生的事件，从以下分支进行处理；
	case executedEvent:
		op.stack.Commit(nil, et.tag.([]byte))
	case committedEvent:
		logger.Debugf("Replica %d received committedEvent", op.pbft.id)
		return execDoneEvent{}
	case execDoneEvent:
		if res := op.pbft.ProcessEvent(event); res != nil {
			// This may trigger a view change, if so, process it, we will resubmit on new view
			return res
		}
		return op.resubmitOutstandingReqs()
	case batchTimerEvent:
		logger.Infof("Replica %d batch timer expired", op.pbft.id)
		if op.pbft.activeView && (len(op.batchStore) > 0) {
			return op.sendBatch()
		}
	case *Commit:
		// TODO, this is extremely hacky, but should go away when batch and core are merged
		res := op.pbft.ProcessEvent(event)
		op.startTimerIfOutstandingRequests()
		return res
	case viewChangedEvent:
		op.batchStore = nil
		// Outstanding reqs doesn't make sense for batch, as all the requests in a batch may be processed
		// in a different batch, but PBFT core can't see through the opaque structure to see this
		// so, on view change, clear it out
		op.pbft.outstandingReqBatches = make(map[string]*RequestBatch)

		logger.Debugf("Replica %d batch thread recognizing new view", op.pbft.id)
		if op.batchTimerActive {
			op.stopBatchTimer()
		}

		if op.pbft.skipInProgress {
			// If we're the new primary, but we're in state transfer, we can't trust ourself not to duplicate things
			op.reqStore.outstandingRequests.empty()
		}

		op.reqStore.pendingRequests.empty()
		for i := op.pbft.h + 1; i <= op.pbft.h+op.pbft.L; i++ {
			if i <= op.pbft.lastExec {
				continue
			}

			cert, ok := op.pbft.certStore[msgID{v: op.pbft.view, n: i}]
			if !ok || cert.prePrepare == nil {
				continue
			}

			if cert.prePrepare.BatchDigest == "" {
				// a null request
				continue
			}

			if cert.prePrepare.RequestBatch == nil {
				logger.Warningf("Replica %d found a non-null prePrepare with no request batch, ignoring")
				continue
			}

			op.reqStore.storePendings(cert.prePrepare.RequestBatch.GetBatch())
		}

		return op.resubmitOutstandingReqs()
	case stateUpdatedEvent:
		// When the state is updated, clear any outstanding requests, they may have been processed while we were gone
		op.reqStore = newRequestStore()
		return op.pbft.ProcessEvent(event)
	//【节点间的共识消息事件】，pbftMessageEvent，将会走这个分支，最终到达pbftCore.ProcessEvent();
	default:
		return op.pbft.ProcessEvent(event)
	}
	return nil
}

func (op *obcBatch) startBatchTimer() {
	op.batchTimer.Reset(op.batchTimeout, batchTimerEvent{})
	logger.Debugf("Replica %d started the batch timer", op.pbft.id)
	op.batchTimerActive = true
}

func (op *obcBatch) stopBatchTimer() {
	op.batchTimer.Stop()
	logger.Debugf("Replica %d stopped the batch timer", op.pbft.id)
	op.batchTimerActive = false
}

// Wraps a payload into a batch message, packs it and wraps it into
// a Fabric message. Called by broadcast before transmission.
func (op *obcBatch) wrapMessage(msgPayload []byte) *pb.Message {
	batchMsg := &BatchMessage{Payload: &BatchMessage_PbftMessage{PbftMessage: msgPayload}}
	packedBatchMsg, _ := proto.Marshal(batchMsg)
	ocMsg := &pb.Message{
		Type:    pb.Message_CONSENSUS,
		Payload: packedBatchMsg,
	}
	return ocMsg
}

// Retrieve the idle channel, only used for testing
func (op *obcBatch) idleChannel() <-chan struct{} {
	return op.idleChan
}

// TODO, temporary
func (op *obcBatch) getManager() events.Manager {
	return op.manager
}

//如果没有在进行数据同步，没有在执行交易，没有在视图转换，并且outstandingRequests中有未处理完的 request，就软开启newViewTimer
func (op *obcBatch) startTimerIfOutstandingRequests() {
	if op.pbft.skipInProgress || op.pbft.currentExec != nil || !op.pbft.activeView {
		// Do not start view change timer if some background event is in progress
		logger.Debugf("Replica %d not starting timer because skip in progress or current exec or in view change", op.pbft.id)
		return
	}

	if !op.reqStore.hasNonPending() {
		// Only start a timer if we are aware of outstanding requests
		logger.Debugf("Replica %d not starting timer because all outstanding requests are pending", op.pbft.id)
		return
	}
	op.pbft.softStartTimer(op.pbft.requestTimeout, "Batch outstanding requests")
}
