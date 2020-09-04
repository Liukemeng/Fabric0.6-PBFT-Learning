# Fabric0.6-PBFT-Learning

**主要内容**:  
1. hyperledger是fabric0.6版本的代码，其中consensus中的代码均加入详细的注释；

2. learning是整理的逻辑流程图，主从节点交互的时序图及调用过程说明文档；


**主要理解以下内容将帮助学习者更好的学习PBFT共识算法**:  
1.接口 
  (1)共识模块对外提供的接口Consenter；  
 （2）其他模块需要向共识模块提供的接口Stack;  
 
2.结构体
 （1）pbftCore实现了共识模块核心的算法逻辑，包括RequestBatch、PrePrepare、Prepare、Commit、Execute、ExecDoneSync、Checkpoint、ViewChange、NewView、StateUpdate等阶段；
  (2)obcBatch是对共识模块在一个高层次的封装，主要是将tx封装为request并将request广播给其他节点，以及其他模块执行、提交、数据同步完之后共识模块需要处理的方法；
  
3.消息循环
  fabric0.6-pbft是以事件驱动模型进行实现，本节点收到其他节点发送过来的消息或者本节点其他模块产生的消息，将封装为batchMessageEvent放入到名为events的channle中，然后开启对该channel的监听，
  随后，将调用obcbatch.ProcessEvent(）进行处理，最终共识的事件，将调用pbftCore.ProcessEvent()进行处理；
  
4.重要缓存字段的生命周期
 （1）obcBatch中batchStore、pendingRequests、outstandingRequests字段的生命周期;
 （2）pbftCore中outstandingReqBatches、reqBatchStore、CerStore字段的生命周期；
 
5.ViewChange过程的关键点
fabric0.6-PBFT中之后ViewChange消息是签名的，其他所有的共识消息是没有签名的，所以视图转换的流程是参照的99年短论文，但是视图转换过程相关变量的计算是参照的02年的长论文
99年短论文《Practical Byzantine Fault Tolerance》
02年长论文《Practical Byzantine Fault Tolerance and Proactive Recovery》
