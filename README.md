# Fabric0.6-PBFT-Learning

**主要内容**:  
1. hyperledger是fabric0.6版本的代码，其中consensus中的代码均加入详细的注释；

2. learning是整理的逻辑流程图，主从节点交互的时序图及调用过程说明文档，**读者可参照文档和时序图阅读源码**；

3. learning中还有从PBFT、Tendermint、Hotstuff等共识算法的分享ppt；

4. paper是PBFT99年短论文、和02年长论文，主要区别在消息体是否签名，所以在vc时流程差别较大；


**主要理解以下内容将帮助学习者更好地学习PBFT共识算法**:  
1. 接口  
  (1) 共识模块对外提供的接口Consenter； 
  
   (2) 其他模块需要向共识模块提供的接口Stack;  
 
2. 结构体  
  (1) pbftCore实现了共识模块核心的算法逻辑，包括RequestBatch、PrePrepare、Prepare、Commit、Execute、ExecDoneSync、Checkpoint、ViewChange、NewView、StateUpdate等阶段流程; 
  
    (2)obcBatch是对共识模块在一个高层次的封装，主要是将tx封装为request并将request广播给其他节点，以及其他模块执行、提交、数据同步完之后共识模块需要处理的方法；  
  
3. 消息循环  
  fabric0.6-pbft是以事件驱动模型进行实现，本节点收到其他节点发送过来的消息或者本节点其他模块产生的消息，将封装为batchMessageEvent放入到名为events的channel中，然后开启对该channel的监听，
  随后，将调用obcbatch.ProcessEvent(）进行处理，最终，共识事件将调用pbftCore.ProcessEvent()进行处理；  
  
4. 重要缓存字段的生命周期  
 （1）obcBatch中batchStore、pendingRequests、outstandingRequests字段的生命周期; 
 
    （2）pbftCore中outstandingReqBatches、reqBatchStore、CerStore字段的生命周期；  
    
   
5. 重要定时器的生命周期及softreStartTimer()和reStartTimer()的区别  
 （1）obcBatch中batchTimer;  
 
    （2）pbftCore中newViewTimer(正常一个块的共识时间，或者发送vc到收到quorum个vc的时间间隔)、nullRequestTimer、vcResendTimer；  
    
6. ViewChange过程的关键点  
 （1）fabric0.6-PBFT中,只有ViewChange消息是签名的，其他所有的共识消息是没有签名的，所以视图转换的流程是参照的99年短论文，但是视图转换过程相关变量的计算是参照的02年的长论文；
 
    （2）99年短论文《Practical Byzantine Fault Tolerance》、02年长论文《Practical Byzantine Fault Tolerance and Proactive Recovery》；
