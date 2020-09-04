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
	"reflect"

	"github.com/hyperledger/fabric/consensus/util/events"
)

// viewChangeQuorumEvent is returned to the event loop when a new ViewChange message is received which is part of a quorum cert
type viewChangeQuorumEvent struct{}
//
func (instance *pbftCore) correctViewChange(vc *ViewChange) bool {
	for _, p := range append(vc.Pset, vc.Qset...) {
		//这个H是节点的稳定检查点
		if !(p.View < vc.View && p.SequenceNumber > vc.H && p.SequenceNumber <= vc.H+instance.L) {
			logger.Debugf("Replica %d invalid p entry in view-change: vc(v:%d h:%d) p(v:%d n:%d)",
				instance.id, vc.View, vc.H, p.View, p.SequenceNumber)
			return false
		}
	}

	for _, c := range vc.Cset {
		// PBFT: the paper says c.n > vc.h
		if !(c.SequenceNumber >= vc.H && c.SequenceNumber <= vc.H+instance.L) {
			logger.Debugf("Replica %d invalid c entry in view-change: vc(v:%d h:%d) c(n:%d)",
				instance.id, vc.View, vc.H, c.SequenceNumber)
			return false
		}
	}

	return true
}

//计算并返回pset,先将certStore中到达prepared状态的batch的<v,n,d>放到pset里面，然后再返回pset。
func (instance *pbftCore) calcPSet() map[uint64]*ViewChange_PQ {
	pset := make(map[uint64]*ViewChange_PQ)

	for n, p := range instance.pset {
		pset[n] = p
	}

	// P set: requests that have prepared here
	//
	// "<n,d,v> has a prepared certificate, and no request
	// prepared in a later view with the same number"

	for idx, cert := range instance.certStore {
		if cert.prePrepare == nil {
			continue
		}

		digest := cert.digest
		if !instance.prepared(digest, idx.v, idx.n) {
			continue
		}

		if p, ok := pset[idx.n]; ok && p.View > idx.v {
			continue
		}

		pset[idx.n] = &ViewChange_PQ{
			SequenceNumber: idx.n,
			BatchDigest:    digest,
			View:           idx.v,
		}
	}

	return pset
}

func (instance *pbftCore) calcQSet() map[qidx]*ViewChange_PQ {
	qset := make(map[qidx]*ViewChange_PQ)

	for n, q := range instance.qset {
		qset[n] = q
	}

	// Q set: requests that have pre-prepared here (pre-prepare or
	// prepare sent)
	//
	// "<n,d,v>: requests that pre-prepared here, and did not
	// pre-prepare in a later view with the same number"

	for idx, cert := range instance.certStore {
		if cert.prePrepare == nil {
			continue
		}

		digest := cert.digest
		if !instance.prePrepared(digest, idx.v, idx.n) {
			continue
		}

		qi := qidx{digest, idx.n}
		if q, ok := qset[qi]; ok && q.View > idx.v {
			continue
		}

		qset[qi] = &ViewChange_PQ{
			SequenceNumber: idx.n,
			BatchDigest:   	digest,
			View:           idx.v,
		}
	}


	return qset
}

func (instance *pbftCore) sendViewChange() events.Event {
	//1. 将timerActive置为false，停止newViewTimer定时器，此计时器在超时时会调用 pbftCore.sendViewChange；
	instance.stopTimer()

	//2. 将当前的newView消息从newViewStore中删除，此字段记录了自己发送的或接收的、最新的、但还没处理完的 new-view 消息，在接收到 new-view 消息时，用这个字段判断相同域编号的 new-view 消息是否已经在处理中；
	//给当前视图编号view加1，将newViewTimer置为false，表示开始视图转换
	delete(instance.newViewStore, instance.view)
	instance.view++
	instance.activeView = false

	//3.计算并更新pset和qset。将certStore中到达prepared和preprepared的barequestBatch所对应消息<n,d,v>追加到pset和qset;
	instance.pset = instance.calcPSet()
	instance.qset = instance.calcQSet()

	//4.删除certStore中之前视图中的共识消息，因为上个视图中没有达成共识的消息，会在下一个视图重新走一遍。
	//清除viewChangeStore中之前的视图转换消息，视图编号小于现在视图编号的视图转换消息
	for idx := range instance.certStore {
		if idx.v < instance.view {
			delete(instance.certStore, idx)
		}
	}
	for idx := range instance.viewChangeStore {
		if idx.v < instance.view {
			delete(instance.viewChangeStore, idx)
		}
	}

	//开始构造viewChange消息
	vc := &ViewChange{
		View:      instance.view,
		H:         instance.h,
		ReplicaId: instance.id,
	}

	//5. 把自己的checkpoint消息都放到vc的Cset里面，没有把其他节点发送过来的checkpoint也放进来；
	for n, id := range instance.chkpts {
		vc.Cset = append(vc.Cset, &ViewChange_C{
			SequenceNumber: n,
			Id:             id,
		})
	}

	//6. 设置pSet，这里放了自己的到达Prepared的消息；
	for _, p := range instance.pset {
		if p.SequenceNumber < instance.h {
			logger.Errorf("BUG! Replica %d should not have anything in our pset less than h, found %+v", instance.id, p)
		}
		vc.Pset = append(vc.Pset, p)
	}
	//7. 设置qSet，这里放了自己的PrePrepare消息；
	for _, q := range instance.qset {
		if q.SequenceNumber < instance.h {
			logger.Errorf("BUG! Replica %d should not have anything in our qset less than h, found %+v", instance.id, q)
		}
		vc.Qset = append(vc.Qset, q)
	}

	//8。对vc进行签名
	instance.sign(vc)

	logger.Infof("Replica %d sending view-change, v:%d, h:%d, |C|:%d, |P|:%d, |Q|:%d",
		instance.id, vc.View, vc.H, len(vc.Cset), len(vc.Pset), len(vc.Qset))

	//9.广播vc消息
	instance.innerBroadcast(&Message{Payload: &Message_ViewChange{ViewChange: vc}})

	//10. 重置vcResendTimer定时器
	instance.vcResendTimer.Reset(instance.vcResendTimeout, viewChangeResendTimerEvent{})
	//11. 自己调用pbftCore.recvViewChange()方法接收自己的ViewChange消息进行处理
	return instance.recvViewChange(vc)
}

//vp节点收到viewChange消息后，进行处理
func (instance *pbftCore) recvViewChange(vc *ViewChange) events.Event {
	logger.Infof("Replica %d received view-change from replica %d, v:%d, h:%d, |C|:%d, |P|:%d, |Q|:%d",
		instance.id, vc.ReplicaId, vc.View, vc.H, len(vc.Cset), len(vc.Pset), len(vc.Qset))

	//1。 验签
	if err := instance.verify(vc); err != nil {
		logger.Warningf("Replica %d found incorrect signature in view-change message: %s", instance.id, err)
		return nil
	}

	//2。验证vc中的视图编号是不是大于等于自己的
	if vc.View < instance.view {
		logger.Warningf("Replica %d found view-change message for old view", instance.id)
		return nil
	}

	//3。判断 view-change 消息中的一些数据是否正确，验证 pset 和 qset 中的视图编号是否比 view-change 中新的域编号小，请求序号是否位于高低水线内，cset 中的Checkpoint的序号是否在高低水线范围内；
	if !instance.correctViewChange(vc) {
		logger.Warningf("Replica %d found view-change message incorrect", instance.id)
		return nil
	}

	//4。判断是否已接收过同一结点发送的域编号相同的 view-change 消息。
	if _, ok := instance.viewChangeStore[vcidx{vc.View, vc.ReplicaId}]; ok {
		logger.Warningf("Replica %d already has a view change message for view %d from replica %d", instance.id, vc.View, vc.ReplicaId)
		return nil
	}

	//5。 将vc放到viewChangeStore里面
	instance.viewChangeStore[vcidx{vc.View, vc.ReplicaId}] = vc

	// PBFT TOCS 4.5.1 Liveness: "if a replica receives a set of
	// f+1 valid VIEW-CHANGE messages from other replicas for
	// views greater than its current view, it sends a VIEW-CHANGE
	// message for the smallest view in the set, even if its timer
	// has not expired"

	//6。 论文的4.5.2小节说了，如果一个节点收到了f+1个有效的并给视图编号比自己大的vc，
	//即使自己的newViewTimer定时器还未到时，也会发送一个视图编号是这f+1个vc中最小的vc;

	//统计总共收到了多少个节点发送了视图编号比节点自己的大的 view-change 消息；
	//并计算比自己视图大的最小的视图
	replicas := make(map[uint64]bool)
	minView := uint64(0)
	for idx := range instance.viewChangeStore {
		if idx.v <= instance.view {
			continue
		}

		replicas[idx.id] = true
		if minView == 0 || idx.v < minView {
			minView = idx.v
		}
	}

	// 符合要求的话，自己就发送vc
	if len(replicas) >= instance.f+1 {
		logger.Infof("Replica %d received f+1 view-change messages, triggering view-change to view %d",
			instance.id, minView)
		// subtract one, because sendViewChange() increments
		instance.view = minView - 1
		return instance.sendViewChange()
	}

	//7。 统计和自己 view-change 消息编号一样的vc的数量，如果>=2f+1，
	//停止vcResendTimer定时器，用 lastNewViewTimeout 开启newViewTimer,并将vcResendTimer设置为原来的2倍，
	//并返回 viewChangeQuorumEvent 对象。

	quorum := 0
	for idx := range instance.viewChangeStore {
		if idx.v == instance.view {
			quorum++
		}
	}
	logger.Debugf("Replica %d now has %d view change requests for view %d", instance.id, quorum, instance.view)

	if !instance.activeView && vc.View == instance.view && quorum >= instance.allCorrectReplicasQuorum() {
		instance.vcResendTimer.Stop()

		instance.startTimer(instance.lastNewViewTimeout, "new view change")
		instance.lastNewViewTimeout = 2 * instance.lastNewViewTimeout
		return viewChangeQuorumEvent{}
	}

	//8。 //不符合则直接返回nil；
	return nil
}
//新视图的主节点发送构造并发送newView消息
func (instance *pbftCore) sendNewView() events.Event {

	//1。如果自己 已经发送过当前视图编号的 view-change 消息
	if _, ok := instance.newViewStore[instance.view]; ok {
		logger.Debugf("Replica %d already has new view in store for view %d, skipping", instance.id, instance.view)
		return nil
	}

	//2。将pbftCore.viewChangeStore中存的vc放到vset中
	vset := instance.getViewChanges()

	//3。cp是在所有的vc中，选出最近的稳定检查点（序号值最大的）
	cp, ok, _ := instance.selectInitialCheckpoint(vset)
	if !ok {
		logger.Infof("Replica %d could not find consistent checkpoint: %+v", instance.id, instance.viewChangeStore)
		return nil
	}

	//4。从找到的最近的稳定检查点cp开始，处理vset中的数据，生成Xset
	msgList := instance.assignSequenceNumbers(vset, cp.SequenceNumber)
	if msgList == nil {
		logger.Infof("Replica %d could not assign sequence numbers for new view", instance.id)
		return nil
	}

	//5.构造并生成newview消息
	nv := &NewView{
		View:      instance.view,
		Vset:      vset,
		Xset:      msgList,
		ReplicaId: instance.id,
	}

	logger.Infof("Replica %d is new primary, sending new-view, v:%d, X:%+v",
		instance.id, nv.View, nv.Xset)

	//6。将newview消息广播出去并存储到自己的newViewStore字段中；
	instance.innerBroadcast(&Message{Payload: &Message_NewView{NewView: nv}})
	instance.newViewStore[instance.view] = nv
	//7。然后调用processNewView（）进行处理
	return instance.processNewView()
}
//vp节点收到newView消息后的处理方法
func (instance *pbftCore) recvNewView(nv *NewView) events.Event {
	logger.Infof("Replica %d received new-view %d",
		instance.id, nv.View)
 //1。验证newView的合法性
	if !(nv.View > 0 && nv.View >= instance.view && instance.primary(nv.View) == nv.ReplicaId && instance.newViewStore[nv.View] == nil) {
		logger.Infof("Replica %d rejecting invalid new-view from %d, v:%d",
			instance.id, nv.ReplicaId, nv.View)
		return nil
	}

	for _, vc := range nv.Vset {
		if err := instance.verify(vc); err != nil {
			logger.Warningf("Replica %d found incorrect view-change signature in new-view message: %s", instance.id, err)
			return nil
		}
	}

	//2。将newView放到自己的newViewStore中
	instance.newViewStore[nv.View] = nv
	//3。然后调用processNewView()方法
	return instance.processNewView()
}

//节点收到NewView后，最终调用processNewView()方法进行处理
func (instance *pbftCore) processNewView() events.Event {
	var newReqBatchMissing bool
	//1。判断newViewStore中是否有newView消息需要处理，
	nv, ok := instance.newViewStore[instance.view]
	if !ok {
		logger.Debugf("Replica %d ignoring processNewView as it could not find view %d in its newViewStore", instance.id, instance.view)
		return nil
	}
	//2。是不是在视图转换期间，没有的话返回nil
	if instance.activeView {
		logger.Infof("Replica %d ignoring new-view from %d, v:%d: we are active in view %d",
			instance.id, nv.ReplicaId, nv.View, instance.view)
		return nil
	}
	//3。获取最近的稳定检查点
	cp, ok, replicas := instance.selectInitialCheckpoint(nv.Vset)
	if !ok {
		logger.Warningf("Replica %d could not determine initial checkpoint: %+v",
			instance.id, instance.viewChangeStore)
		return instance.sendViewChange()
	}

	//4。获取节点当前执行的requestbatch的序号n并赋值给speculativeLastExec
	speculativeLastExec := instance.lastExec
	if instance.currentExec != nil {
		speculativeLastExec = *instance.currentExec
	}

	// If we have not reached the sequence number, check to see if we can reach it without state transfer
	// In general, executions are better than state transfer
	//5。判断当前结点的最新序号小于 cp 时，是否可以自己执行请求到序号等于 cp.SequenceNumber 的情况。
	//当请求执行得太慢，而这些未执行的请求其实都已经处于 committed 状态时，就会发生这种情况。如果可以执行到 cp，就直接返回等待请求的执行。当请求执行完之后，会在pbftCore.ProcessEvent()的execDoneEvent分支再次调用processNewView
	if speculativeLastExec < cp.SequenceNumber {
		canExecuteToTarget := true
	outer:
		for seqNo := speculativeLastExec + 1; seqNo <= cp.SequenceNumber; seqNo++ {
			found := false
			for idx, cert := range instance.certStore {
				if idx.n != seqNo {
					continue
				}

				quorum := 0
				for _, p := range cert.commit {
					// Was this committed in the previous view
					if p.View == idx.v && p.SequenceNumber == seqNo {
						quorum++
					}
				}

				if quorum < instance.intersectionQuorum() {
					logger.Debugf("Replica %d missing quorum of commit certificate for seqNo=%d, only has %d of %d", instance.id, quorum, instance.intersectionQuorum())
					continue
				}

				found = true
				break
			}


			if !found {
				//将canExecuteToTarget设置为false
				canExecuteToTarget = false
				logger.Debugf("Replica %d missing commit certificate for seqNo=%d", instance.id, seqNo)
				break outer
			}

		}

		//如果可以执行就直接返回nil等待请求被执行；注意在pbftCore.ProcessEvent()中的execDoneEvent分支进行处理后，将再次调用pbftCore.ProcessEvent()。
		if canExecuteToTarget {
			logger.Debugf("Replica %d needs to process a new view, but can execute to the checkpoint seqNo %d, delaying processing of new view", instance.id, cp.SequenceNumber)
			return nil
		}

		logger.Infof("Replica %d cannot execute to the view change checkpoint with seqNo %d", instance.id, cp.SequenceNumber)
	}

	//6。到这里说明节点的当前执行的序号可能大于Vset中的最近稳定检查点cp；
	//也有可能小于，但是节点不能自己执行从最新序号speculativeLastExec到Vset中的最近稳定检查点cp；

	//7。判断 new-view 消息中的 xset 是否正确，不正确发送vc
	msgList := instance.assignSequenceNumbers(nv.Vset, cp.SequenceNumber)
	if msgList == nil {
		logger.Warningf("Replica %d could not assign sequence numbers: %+v",
			instance.id, instance.viewChangeStore)
		return instance.sendViewChange()
	}

	if !(len(msgList) == 0 && len(nv.Xset) == 0) && !reflect.DeepEqual(msgList, nv.Xset) {
		logger.Warningf("Replica %d failed to verify new-view Xset: computed %+v, received %+v",
			instance.id, msgList, nv.Xset)
		return instance.sendViewChange()
	}

	//8。 自己的最低检查点低于Vset中的最近稳定检查点cp，移动水线；
	if instance.h < cp.SequenceNumber {
		instance.moveWatermarks(cp.SequenceNumber)
	}

	//9。如果节点最大请求序号小于 cp，就发起数据同步。
	if speculativeLastExec < cp.SequenceNumber {
		logger.Warningf("Replica %d missing base checkpoint %d (%s), our most recent execution %d", instance.id, cp.SequenceNumber, cp.Id, speculativeLastExec)

		snapshotID, err := base64.StdEncoding.DecodeString(cp.Id)
		if nil != err {
			err = fmt.Errorf("Replica %d received a view change whose hash could not be decoded (%s)", instance.id, cp.Id)
			logger.Error(err.Error())
			return nil
		}

		target := &stateUpdateTarget{
			checkpointMessage: checkpointMessage{
				seqNo: cp.SequenceNumber,
				id:    snapshotID,
			},
			replicas: replicas,
		}

		instance.updateHighStateTarget(target)
		instance.stateTransfer(target)
	}

	//10。由于后面结点要把 NewView.Xset 中的信息作为 pre-prepare 消息进行处理，因此这里首先判断自己是否存储了 xset 中的请求。
	//如果有未存储某请求，则将其记录到 pbftCore.missingReqBatches 中，并调用 pbftCore.fetchRequestBatches 同步这些请求；
	//如果已全部存储了这些请求，则调用 pbftCore.ProcessNewView2 继续处理。
	for n, d := range nv.Xset {
		// PBFT: why should we use "h ≥ min{n | ∃d : (<n,d> ∈ X)}"?
		// "h ≥ min{n | ∃d : (<n,d> ∈ X)} ∧ ∀<n,d> ∈ X : (n ≤ h ∨ ∃m ∈ in : (D(m) = d))"
		if n <= instance.h {
			continue
		} else {
			if d == "" {
				// NULL request; skip
				continue
			}

			if _, ok := instance.reqBatchStore[d]; !ok {
				logger.Warningf("Replica %d missing assigned, non-checkpointed request batch %s",
					instance.id, d)
				if _, ok := instance.missingReqBatches[d]; !ok {
					logger.Warningf("Replica %v requesting to fetch batch %s",
						instance.id, d)
					newReqBatchMissing = true
					instance.missingReqBatches[d] = true
				}
			}
		}
	}

	if len(instance.missingReqBatches) == 0 {
		return instance.processNewView2(nv)
	} else if newReqBatchMissing {
		instance.fetchRequestBatches()
	}

	return nil
}
//这里是实际进入到新的视图，进行preprepare、prepare的相关构造和广播
func (instance *pbftCore) processNewView2(nv *NewView) events.Event {
	logger.Infof("Replica %d accepting new-view to view %d", instance.id, instance.view)

	//1。在之前的recvNewView、processNewView中把异常情况都排除了，所以这里的话就是在新的视图继续进行处理了

	//1。关闭newTimer定时器，并将timerActive = false
	instance.stopTimer()
	instance.nullRequestTimer.Stop()

	//2。将activeView = true，表明结束视图转换，开始进入一个有效的视图；
	//并删除newViewStore中存的之前那个视图的vc；
	instance.activeView = true
	delete(instance.newViewStore, instance.view-1)

	//3。主从节点使用 NewView.Xset 中的每一个元素构造 pre-prepare消息，并存储到 pbftCore.certStore 中。
	instance.seqNo = instance.h
	for n, d := range nv.Xset {
		if n <= instance.h {
			continue
		}

		reqBatch, ok := instance.reqBatchStore[d]
		if !ok && d != "" {
			logger.Criticalf("Replica %d is missing request batch for seqNo=%d with digest '%s' for assigned prepare after fetching, this indicates a serious bug", instance.id, n, d)
		}
		preprep := &PrePrepare{
			View:           instance.view,
			SequenceNumber: n,
			BatchDigest:    d,
			RequestBatch:   reqBatch,
			ReplicaId:      instance.id,
		}
		cert := instance.getCert(instance.view, n)
		cert.prePrepare = preprep
		cert.digest = d
		if n > instance.seqNo {
			instance.seqNo = n
		}
		instance.persistQSet()
	}

	//4。更新自己的pbftCore.viewChangeSeqNo 字段，当处理的请求达到此字段代表应该进行 view-change 了。
	instance.updateViewChangeSeqNo()

	//5。如果自己不是新的主节点，则继续处理 new-view 消息中的 xset 数据，为 xset 中的每一项生成一个 prepare 消息，并处理和广播；
	//如果自己是新的主结点，则调用 pbftCore.resubmitRequestBatches 将之前收到的、还未处理的请求进行处理。
	if instance.primary(instance.view) != instance.id {
		for n, d := range nv.Xset {
			prep := &Prepare{
				View:           instance.view,
				SequenceNumber: n,
				BatchDigest:    d,
				ReplicaId:      instance.id,
			}
			if n > instance.h {
				cert := instance.getCert(instance.view, n)
				cert.sentPrepare = true
				instance.recvPrepare(prep)
			}
			instance.innerBroadcast(&Message{Payload: &Message_Prepare{Prepare: prep}})
		}
	} else {//是主节点的话，构造preprepare消息，
		logger.Debugf("Replica %d is now primary, attempting to resubmit requests", instance.id)
		instance.resubmitRequestBatches()
	}

	//6。如果outstandingReqBatches中还有requestBatch，软启动newViewTimer，否则就重启nullRequestTimer定时器
	instance.startTimerIfOutstandingRequests()

	logger.Debugf("Replica %d done cleaning view change artifacts, calling into consumer", instance.id)

	//7。最终返回 viewChangedEvent 对象。obcBatch 对象的消息循环中在收到这个事件时，会做一些事情来应对域发生转变的这种情况，比如清空一些数据等。
	//这些应对中比较重要的是检查自己是不是新的主结点，如果是则要调用 obcBatch.resubmitOutstandingReqs 将自己之前收到的、没有被处理的请求进行处理。
	return viewChangedEvent{}
}

func (instance *pbftCore) getViewChanges() (vset []*ViewChange) {
	for _, vc := range instance.viewChangeStore {
		vset = append(vset, vc)
	}

	return
}

func (instance *pbftCore) selectInitialCheckpoint(vset []*ViewChange) (checkpoint ViewChange_C, ok bool, replicas []uint64) {
	checkpoints := make(map[ViewChange_C][]*ViewChange)
	for _, vc := range vset {
		for _, c := range vc.Cset { // TODO, verify that we strip duplicate checkpoints from this set
			checkpoints[*c] = append(checkpoints[*c], vc)
			logger.Debugf("Replica %d appending checkpoint from replica %d with seqNo=%d, h=%d, and checkpoint digest %s", instance.id, vc.ReplicaId, vc.H, c.SequenceNumber, c.Id)
		}
	}

	if len(checkpoints) == 0 {
		logger.Debugf("Replica %d has no checkpoints to select from: %d %s",
			instance.id, len(instance.viewChangeStore), checkpoints)
		return
	}

	for idx, vcList := range checkpoints {
		// need weak certificate for the checkpoint
		if len(vcList) <= instance.f { // type casting necessary to match types
			logger.Debugf("Replica %d has no weak certificate for n:%d, vcList was %d long",
				instance.id, idx.SequenceNumber, len(vcList))
			continue
		}

		quorum := 0
		// Note, this is the whole vset (S) in the paper, not just this checkpoint set (S') (vcList)
		// We need 2f+1 low watermarks from S below this seqNo from all replicas
		// We need f+1 matching checkpoints at this seqNo (S')
		for _, vc := range vset {
			if vc.H <= idx.SequenceNumber {
				quorum++
			}
		}

		if quorum < instance.intersectionQuorum() {
			logger.Debugf("Replica %d has no quorum for n:%d", instance.id, idx.SequenceNumber)
			continue
		}

		replicas = make([]uint64, len(vcList))
		for i, vc := range vcList {
			replicas[i] = vc.ReplicaId
		}

		if checkpoint.SequenceNumber <= idx.SequenceNumber {
			checkpoint = idx
			ok = true
		}
	}

	return
}

func (instance *pbftCore) assignSequenceNumbers(vset []*ViewChange, h uint64) (msgList map[uint64]string) {
	msgList = make(map[uint64]string)

	maxN := h + 1

	// "for all n such that h < n <= h + L"
nLoop:
	for n := h + 1; n <= h+instance.L; n++ {
		// "∃m ∈ S..."
		for _, m := range vset {
			// "...with <n,d,v> ∈ m.P"
			for _, em := range m.Pset {
				quorum := 0
				// "A1. ∃2f+1 messages m' ∈ S"
			mpLoop:
				for _, mp := range vset {
					if mp.H >= n {
						continue
					}
					// "∀<n,d',v'> ∈ m'.P"
					for _, emp := range mp.Pset {
						if n == emp.SequenceNumber && !(emp.View < em.View || (emp.View == em.View && emp.BatchDigest == em.BatchDigest)) {
							continue mpLoop
						}
					}
					quorum++
				}

				if quorum < instance.intersectionQuorum() {
					continue
				}

				quorum = 0
				// "A2. ∃f+1 messages m' ∈ S"
				for _, mp := range vset {
					// "∃<n,d',v'> ∈ m'.Q"
					for _, emp := range mp.Qset {
						if n == emp.SequenceNumber && emp.View >= em.View && emp.BatchDigest == em.BatchDigest {
							quorum++
						}
					}
				}

				if quorum < instance.f+1 {
					continue
				}

				// "then select the request with digest d for number n"
				msgList[n] = em.BatchDigest
				maxN = n

				continue nLoop
			}
		}

		quorum := 0
		// "else if ∃2f+1 messages m ∈ S"
	nullLoop:
		for _, m := range vset {
			// "m.P has no entry"
			for _, em := range m.Pset {
				if em.SequenceNumber == n {
					continue nullLoop
				}
			}
			quorum++
		}

		if quorum >= instance.intersectionQuorum() {
			// "then select the null request for number n"
			msgList[n] = ""

			continue nLoop
		}

		logger.Warningf("Replica %d could not assign value to contents of seqNo %d, found only %d missing P entries", instance.id, n, quorum)
		return nil
	}

	// prune top null requests
	for n, msg := range msgList {
		if n > maxN && msg == "" {
			delete(msgList, n)
		}
	}

	return
}
