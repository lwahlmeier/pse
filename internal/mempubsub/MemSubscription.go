package mempubsub

import (
	"context"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"githb.com/lwahlmeier/go-pubsub-emulator/internal/base"
	"github.com/google/uuid"
	"github.com/lwahlmeier/GoScheduler"
	unboundchannel "github.com/lwahlmeier/unboundChannel"
	"google.golang.org/genproto/googleapis/pubsub/v1"
)

var ack_check_time = time.Second * 5

type MsgTime struct {
	msg     *pubsub.ReceivedMessage
	expTime time.Time
}

func NewMsgTime(msg *pubsub.ReceivedMessage, expDeadline time.Duration) *MsgTime {
	return &MsgTime{
		msg:     msg,
		expTime: time.Now().Add(expDeadline),
	}
}

func (mt *MsgTime) ExtendTime(newDeadLine time.Duration) {
	mt.expTime = time.Now().Add(newDeadLine)
}

func (mt *MsgTime) IsExpired() bool {
	return time.Until(mt.expTime) < 0
}

type MemSubscription struct {
	name          string
	topic         *MemTopic
	sub           *pubsub.Subscription
	acks          map[uuid.UUID]*MsgTime
	ackLock       sync.Mutex
	msgChannel    *unboundchannel.UnboundChannel[*pubsub.ReceivedMessage]
	running       atomic.Bool
	streamClients map[string]*StreamingSubcription
	clientLock    sync.Mutex
	cf            context.CancelFunc
}

func NewMemSub(name string, topic *MemTopic, sub *pubsub.Subscription) *MemSubscription {
	ms := &MemSubscription{
		name:          name,
		topic:         topic,
		sub:           sub,
		msgChannel:    unboundchannel.NewUnboundChannel[*pubsub.ReceivedMessage](),
		acks:          make(map[uuid.UUID]*MsgTime),
		streamClients: make(map[string]*StreamingSubcription),
		running:       atomic.Bool{},
	}
	ms.running.Store(true)
	ctx, cf := context.WithCancel(context.Background())
	ms.cf = cf
	GoScheduler.GetDefaultScheduler().ScheduleWithContext(ack_check_time, true, ms.checkNack, ctx)
	logger.Info("Created Sub:{} for Topic:{} and Project:{}", name, topic.name, topic.project.name)
	return ms
}

func (ms *MemSubscription) GetTopic() base.BaseTopic {
	return ms.topic
}
func (ms *MemSubscription) GetName() string {
	return ms.name
}
func (ms *MemSubscription) GetSubscriptionPubSub() *pubsub.Subscription {
	return ms.sub
}
func (ms *MemSubscription) UpdateAcks(acks []string, secs int32) {
	if !ms.running.Load() {
		return
	}
	ms.ackLock.Lock()
	defer ms.ackLock.Unlock()
	addTime := time.Second * time.Duration(secs)
	for _, ack := range acks {
		uack := uuid.MustParse(ack)
		if mt, ok := ms.acks[uack]; ok {
			mt.ExtendTime(addTime)
		}
	}
}
func (ms *MemSubscription) UpdateAck(ack string, addTime time.Duration) {
	if !ms.running.Load() {
		return
	}
	ms.ackLock.Lock()
	defer ms.ackLock.Unlock()
	uack := uuid.MustParse(ack)
	if mt, ok := ms.acks[uack]; ok {
		mt.ExtendTime(addTime)
	}
}

func (ms *MemSubscription) AckMessages(acks []string) {
	if !ms.running.Load() {
		return
	}
	ms.ackLock.Lock()
	defer ms.ackLock.Unlock()
	for _, ack := range acks {
		uack := uuid.MustParse(ack)
		delete(ms.acks, uack)
		ms.runAck(uack)
	}
}
func (ms *MemSubscription) getDefaultAckDeadline() time.Duration {
	fsecs := float64(ms.sub.AckDeadlineSeconds)
	fsecs = math.Max(10, fsecs)
	secs := int(math.Min(600, fsecs))
	return time.Second * time.Duration(secs)
}

func (ms *MemSubscription) GetMessages(maxMsgs int32, maxWait time.Duration) []*pubsub.ReceivedMessage {
	logger.Info("Poll Messages from:{}:{}:{}", ms.topic.project.name, ms.topic.name, ms.name)
	msgs := make([]*pubsub.ReceivedMessage, 0)
	timer := time.NewTimer(maxWait)
	ackDelay := ms.getDefaultAckDeadline()
	select {
	case msg := <-ms.msgChannel.Get():
		msgs = append(msgs, msg)
		ms.watchMesageAck(msg, ackDelay)
		waitTime := time.NewTimer(time.Millisecond * 5)
		for len(msgs) < int(maxMsgs) {
			runtime.Gosched()
			select {
			case msg := <-ms.msgChannel.Get():
				msgs = append(msgs, msg)
				ms.watchMesageAck(msg, ackDelay)
			// We need to wait more then default as there is not constant pressure on these queues
			case <-waitTime.C:
				return msgs
			}
		}
	case <-timer.C:
	}

	return msgs
}
func (ms *MemSubscription) CreateStreamingSubscription(firstRecvMsg *pubsub.StreamingPullRequest, streamingServer pubsub.Subscriber_StreamingPullServer) base.BaseStreamingSubcription {
	cid := uuid.NewString()
	logger.Info("Project:{}:Topic:{}:Sub:{}, Creating SubStream:{}", ms.topic.project.name, ms.topic.name, ms.name, cid)
	maxMsg := int64(10)
	if firstRecvMsg.MaxOutstandingMessages > 0 {
		maxMsg = firstRecvMsg.MaxOutstandingMessages
	}
	ss := &StreamingSubcription{
		sub:             ms,
		streamingServer: streamingServer,
		timer:           time.NewTimer(ack_check_time),
		clientId:        cid,
		maxMsgs:         maxMsg,
		maxBytes:        firstRecvMsg.MaxOutstandingBytes,
		deadline:        time.Second * time.Duration(firstRecvMsg.StreamAckDeadlineSeconds),
		running:         true,
		acker:           unboundchannel.NewUnboundChannel[uuid.UUID](),
		pendingMsgs:     make(map[uuid.UUID]bool),
		recvChan:        make(chan *pubsub.StreamingPullRequest),
	}
	ms.clientLock.Lock()
	defer ms.clientLock.Unlock()
	ms.streamClients[cid] = ss
	go ss.watchRecv()

	return ss
}

func (ms *MemSubscription) DeleteStreamingSubscription(ss *StreamingSubcription) {
	ms.clientLock.Lock()
	defer ms.clientLock.Unlock()
	ss.running = false
	delete(ms.streamClients, ss.clientId)
	ss.streamingServer.Context().Done()
	ss.acker.Stop()
}

func (ms *MemSubscription) stop() {
	if ms.running.CompareAndSwap(true, false) {
		ms.cf()
		ms.clientLock.Lock()
		defer ms.clientLock.Unlock()
		for _, ss := range ms.streamClients {
			ss.running = false
			ss.streamingServer.Context().Done()
			ss.acker.Stop()
		}
		ms.msgChannel.Stop()
	}
}

func (ms *MemSubscription) checkNack() {
	logger.Debug("Checking For Nacks")
	ms.ackLock.Lock()
	defer ms.ackLock.Unlock()
	for mid, mt := range ms.acks {
		if mt.IsExpired() {
			logger.Info("Nacking Message:{}", mid.String())
			delete(ms.acks, mid)
			ms.runAck(mid)
			mt.msg.DeliveryAttempt++
			ms.msgChannel.Add() <- mt.msg
		}
	}
}

func (ms *MemSubscription) runAck(ackUUID uuid.UUID) {
	ms.clientLock.Lock()
	defer ms.clientLock.Unlock()
	for _, client := range ms.streamClients {
		client.acker.Add() <- ackUUID
	}
}

func (ms *MemSubscription) PublishMessage(msg *pubsub.PubsubMessage) {
	logger.Debug("Project:{}:Topic:{}:Sub:{}, Got Message:{}", ms.topic.project.name, ms.topic.name, ms.name, msg.MessageId)
	ms.msgChannel.Add() <- &pubsub.ReceivedMessage{
		Message:         msg,
		AckId:           msg.MessageId,
		DeliveryAttempt: 0,
	}
}

func (ms *MemSubscription) watchMesageAck(msg *pubsub.ReceivedMessage, deadline time.Duration) {
	ms.ackLock.Lock()
	defer ms.ackLock.Unlock()
	ms.acks[uuid.MustParse(msg.AckId)] = NewMsgTime(msg, deadline)
}

type StreamingSubcription struct {
	streamingServer pubsub.Subscriber_StreamingPullServer
	sub             *MemSubscription
	timer           *time.Timer
	maxMsgs         int64
	maxBytes        int64
	pendingMsgs     map[uuid.UUID]bool
	acker           *unboundchannel.UnboundChannel[uuid.UUID]
	currentBytes    int64
	clientId        string
	deadline        time.Duration
	recvChan        chan *pubsub.StreamingPullRequest
	running         bool
}

func (ss *StreamingSubcription) Run() {
	defer ss.sub.DeleteStreamingSubscription(ss)
	for ss.running {
		if int64(len(ss.pendingMsgs)) >= ss.maxMsgs {
			ss.noMsgSelect()
			continue
		}
		msgs := ss.msgSelect()
		if len(msgs) > 0 {
			err := ss.streamingServer.Send(&pubsub.StreamingPullResponse{
				ReceivedMessages: msgs,
			})
			if err != nil {
				logger.Warn("Error StreamSending Message:{}", err.Error())
				ss.running = false
				ss.streamingServer.Context().Err()
				return
			}
		}
	}
}

func (ss *StreamingSubcription) msgSelect() []*pubsub.ReceivedMessage {
	msgs := make([]*pubsub.ReceivedMessage, 0)
	select {
	case msg := <-ss.sub.msgChannel.Get():
		ss.pendingMsgs[uuid.MustParse(msg.AckId)] = true
		ss.sub.watchMesageAck(msg, ss.deadline)
		msgs = append(msgs, msg)
		waitTime := time.NewTimer(time.Millisecond)
		for len(ss.pendingMsgs) < int(ss.maxMsgs) {
			select {
			case msg := <-ss.sub.msgChannel.Get():
				ss.pendingMsgs[uuid.MustParse(msg.AckId)] = true
				ss.sub.watchMesageAck(msg, ss.deadline)
				msgs = append(msgs, msg)
			case <-waitTime.C:
				return msgs
			}
		}
	case aid := <-ss.acker.Get():
		delete(ss.pendingMsgs, aid)
	case recvMsg := <-ss.recvChan:
		if len(recvMsg.AckIds) > 0 {
			ss.sub.AckMessages(recvMsg.AckIds)
		}
		if len(recvMsg.ModifyDeadlineAckIds) > 0 {
			for i := range recvMsg.ModifyDeadlineAckIds {
				ss.sub.UpdateAcks([]string{recvMsg.ModifyDeadlineAckIds[i]}, recvMsg.ModifyDeadlineSeconds[i])
			}
		}
	case <-ss.timer.C:
		ss.timer.Reset(ack_check_time)
	}
	return msgs
}
func (ss *StreamingSubcription) noMsgSelect() {
	select {
	case aid := <-ss.acker.Get():
		delete(ss.pendingMsgs, aid)
	case recvMsg := <-ss.recvChan:
		if len(recvMsg.AckIds) > 0 {
			ss.sub.AckMessages(recvMsg.AckIds)
		}
		if len(recvMsg.ModifyDeadlineAckIds) > 0 {
			for i := range recvMsg.ModifyDeadlineAckIds {
				ss.sub.UpdateAcks([]string{recvMsg.ModifyDeadlineAckIds[i]}, recvMsg.ModifyDeadlineSeconds[i])
			}
		}
	case <-ss.timer.C:
		ss.timer.Reset(ack_check_time)
	}
}

func (ss *StreamingSubcription) watchRecv() {
	for ss.running {
		msg, err := ss.streamingServer.Recv()
		if !ss.running || err != nil {
			logger.Warn("Error StreamRecv:{}", err.Error())
			ss.running = false
			return
		}
		ss.recvChan <- msg
	}
}
