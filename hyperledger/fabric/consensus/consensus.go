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

package consensus

import (
	pb "github.com/hyperledger/fabric/protos"
)
// =============================================================================
//    consensus.go文件 抽象了共识过程中的接口
// =============================================================================

//这里抽象了共识过程中的接口
// ExecutionConsumer allows callbacks from asychronous execution and statetransfer
//本节点对非pbft模块传送消息的接口，相应的4种操作完成后，将回调这个接口里面的方法，生成对应的事件。
//externalEventReceiver对象是对这个接口的具体实现
type ExecutionConsumer interface {
	Executed(tag interface{})                                // Called whenever Execute completes
	Committed(tag interface{}, target *pb.BlockchainInfo)    // Called whenever Commit completes
	RolledBack(tag interface{})                              // Called whenever a Rollback completes
	StateUpdated(tag interface{}, target *pb.BlockchainInfo) // Called when state transfer completes, if target is nil, this indicates a failure and a new target should be supplied
}

// Consenter is used to receive messages from the network
// Every consensus plugin needs to implement this interface

//共识模块对外提供的接口，所有共识插件都需要实现这个接口，这个接口被obcbatch中的externalEventReceiver对象实现；
type Consenter interface {
	//接收【其他节点】通过gRPC发送过来的message，封装成batchMessageEvent事件放到obcBatch.manager.events通道中;
	RecvMsg(msg *pb.Message, senderHandle *pb.PeerID) error
	//【本节点非共识模块】发送过来的消息，封装成相应的事件executedEvent、committedEvent、rolledBackEvent、stateUpdatedEvent
	ExecutionConsumer
}

// Inquirer is used to retrieve info about the validating network
//询问者接口：返回网络中的节点的PeerEndpoint（含有IP、端口、类型vp nvp、公钥）、PeerID（节点的name）
type Inquirer interface {
	GetNetworkInfo() (self *pb.PeerEndpoint, network []*pb.PeerEndpoint, err error)
	GetNetworkHandles() (self *pb.PeerID, network []*pb.PeerID, err error)
}

// Communicator is used to send messages to other validators
//发送者接口：含有广播、单点传播两种方法
type Communicator interface {
	Broadcast(msg *pb.Message, peerType pb.PeerEndpoint_Type) error
	Unicast(msg *pb.Message, receiverHandle *pb.PeerID) error
}

// NetworkStack is used to retrieve network info and send messages
//网络方面的封装为了，给stack用
type NetworkStack interface {
	Communicator
	Inquirer
}

// SecurityUtils is used to access the sign/verify methods from the crypto package
//签名和验签的封装
type SecurityUtils interface {
	Sign(msg []byte) ([]byte, error)
	Verify(peerID *pb.PeerID, signature []byte, message []byte) error
}

// ReadOnlyLedger is used for interrogating the blockchain
//获取区块链层面的信息、和区块层面的信息
type ReadOnlyLedger interface {
	GetBlock(id uint64) (block *pb.Block, err error)
	GetBlockchainSize() uint64
	GetBlockchainInfo() *pb.BlockchainInfo
	GetBlockchainInfoBlob() []byte
	GetBlockHeadMetadata() ([]byte, error)
}

// LegacyExecutor is used to invoke transactions, potentially modifying the backing ledger
//？
type LegacyExecutor interface {
	BeginTxBatch(id interface{}) error
	ExecTxs(id interface{}, txs []*pb.Transaction) ([]byte, error)
	CommitTxBatch(id interface{}, metadata []byte) (*pb.Block, error)
	RollbackTxBatch(id interface{}) error
	PreviewCommitTxBatch(id interface{}, metadata []byte) ([]byte, error)
}

// Executor is intended to eventually supplant the old Executor interface
// The problem with invoking the calls directly above, is that they must be coordinated
// with state transfer, to eliminate possible races and ledger corruption
//本节点其他模块需要对共识模块提供的接口
type Executor interface {
	Start()                                                                     // Bring up the resources needed to use this interface
	Halt()                                                                      // Tear down the resources needed to use this interface
	Execute(tag interface{}, txs []*pb.Transaction)                             // Executes a set of transactions, this may be called in succession
	Commit(tag interface{}, metadata []byte)                                    // Commits whatever transactions have been executed
	Rollback(tag interface{})                                                   // Rolls back whatever transactions have been executed
	UpdateState(tag interface{}, target *pb.BlockchainInfo, peers []*pb.PeerID) // Attempts to synchronize state to a particular target, implicitly calls rollback if needed
}

// LedgerManager is used to manipulate（修改） the state of the ledger
//查询账本是不是在合法的状态，也就是是不是最新的状态，或者是落后的状态
type LedgerManager interface {
	//Invalidate表明账本已经过期，应该拒绝查询
	InvalidateState() // Invalidate informs the ledger that it is out of date and should reject queries
	//Validate表明账本已经恢复到最新状态，并且应该恢复对查询的响应
	ValidateState()   // Validate informs the ledger that it is back up to date and should resume replying to queries
}

// StatePersistor is used to store consensus state which should survive a process crash
//状态持久化接口
type StatePersistor interface {
	StoreState(key string, value []byte) error
	ReadState(key string) ([]byte, error)
	ReadStateSet(prefix string) (map[string][]byte, error)
	DelState(key string)
}

// Stack is the set of stack-facing methods available to the consensus plugin

//其他模块需要对共识模块提供的接口，由Helper实现，
type Stack interface {
	NetworkStack  //网络模块的接口，拥有Communicator、Inquirer接口，拥有广播、单播、获取节点身份信息等方法；
	SecurityUtils //密码模块的接口，提供了签名和验签的方法；
	Executor      //执行模块需要对共识模块提供的接口，有Start()、Halt()、Execute()、Commit()、Rollback()、UpdateState()方法
	LegacyExecutor //
	LedgerManager//判断账本落没落后的接口，决定是不是接受查询账本的操作；
	ReadOnlyLedger//获取链和区块层面信息的接口
	StatePersistor//状态持久化接口，拥有StoreState、ReadState、ReadStateSet、DelState方法。
}
