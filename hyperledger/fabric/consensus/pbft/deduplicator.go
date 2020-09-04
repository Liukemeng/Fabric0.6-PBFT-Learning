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
	"time"
)

// deduplicator maintains the most recent Request timestamp for each
// replica.  Two timestamps are maintained per replica.  One timestamp
// tracks the most recent Request received from a replica, the other
// timeout tracks the most recent executed Request.

//去重器：记录所有的replica中的两个时间戳：一个是"最近"的被接收的请求的时间，另一个是最近被执行的交易的时间戳。
//作用是在共识开始前过滤掉过期的request，和在执行交易前避免执行过期的request
type deduplicator struct {
	reqTimestamps  map[uint64]time.Time
	execTimestamps map[uint64]time.Time
}

// newDeduplicator creates a new deduplicator.
//构造器
func newDeduplicator() *deduplicator {
	d := &deduplicator{}
	d.reqTimestamps = make(map[uint64]time.Time)
	d.execTimestamps = make(map[uint64]time.Time)
	return d
}

// Request updates the received request timestamp for the submitting
// replica.  If the request is older than any previously received or
// executed request, Request() will return false, indicating a stale
// request.
//收到一个 request后，判断它是不是大于最近的接收request的时间戳或者大于最近刚被执行的request的时间戳，更新这个节点的reqTimestamps，如果满足条件返回true。
func (d *deduplicator) Request(req *Request) bool {
	reqTime := time.Unix(req.Timestamp.Seconds, int64(req.Timestamp.Nanos))
	if !reqTime.After(d.reqTimestamps[req.ReplicaId]) ||
		!reqTime.After(d.execTimestamps[req.ReplicaId]) {
		return false
	}
	d.reqTimestamps[req.ReplicaId] = reqTime
	return true
}

// Execute updates the executed request timestamp for the submitting
// replica.  If the request is older than any previously executed
// request from the same replica, Execute() will return false,
// indicating a stale request.
//在执行 request之前，验证request的时间戳是不是大于最近刚被执行的request的时间戳。如果满足条件就更新这个节点的execTimestamps，并返回true。
func (d *deduplicator) Execute(req *Request) bool {
	reqTime := time.Unix(req.Timestamp.Seconds, int64(req.Timestamp.Nanos))
	if !reqTime.After(d.execTimestamps[req.ReplicaId]) {
		return false
	}
	d.execTimestamps[req.ReplicaId] = reqTime
	return true
}

// IsNew returns true if this Request is newer than any previously
// executed request of the submitting replica.
//如果这个消息的时间戳大于这个节点的execTimestamps，就返回true。
func (d *deduplicator) IsNew(req *Request) bool {
	reqTime := time.Unix(req.Timestamp.Seconds, int64(req.Timestamp.Nanos))
	return reqTime.After(d.execTimestamps[req.ReplicaId])
}
