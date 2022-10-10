package fspubsub

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"runtime"
	"strings"
	"sync"
	"time"

	"githb.com/lwahlmeier/go-pubsub-emulator/internal/base"
	"githb.com/lwahlmeier/go-pubsub-emulator/internal/utils"
	"github.com/google/uuid"
	"google.golang.org/genproto/googleapis/pubsub/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

var ack_check_time = time.Second * 5

type FSSubscription struct {
	name               string
	topic              *FSTopic
	subPath            string
	pubsubSubscription *pubsub.Subscription
	msgLock            sync.Mutex
	ackLock            sync.Mutex
	streamClients      map[string]*StreamingSubcription
	clientLock         sync.Mutex
	msgsChannel        *utils.DynamicUUIDChannel
	running            bool
}

func CreateSubscription(basePath string, topic *FSTopic, sub *pubsub.Subscription) (*FSSubscription, error) {
	subName, err := GetSubscriptionName(sub.Name)
	if err != nil {
		return nil, err
	}
	subPath := path.Join(basePath, subName)
	_, err = os.Stat(subPath)
	if err == nil {
		return nil, status.Error(codes.AlreadyExists, "Sub Already Exists")
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	err = os.MkdirAll(subPath, os.ModePerm)
	if err != nil {
		return nil, err
	}
	fsSub := &FSSubscription{
		name:               subName,
		topic:              topic,
		subPath:            subPath,
		pubsubSubscription: sub,
		streamClients:      make(map[string]*StreamingSubcription),
		msgsChannel:        utils.NewDynamicUUIDChannel(),
		running:            true,
	}

	sfp := fsSub.GetSubFilePath()
	data, err := proto.Marshal(sub)
	if err != nil {
		return nil, err
	}
	err = os.WriteFile(sfp, data, os.ModePerm)
	if err != nil {
		return nil, err
	}
	logger.Info("Created Sub:{} for Topic:{} in Project:{}", subName, topic.name, topic.project.name)
	go fsSub.watchAcks()
	return fsSub, nil
}

func LoadSubscription(subName string, topic *FSTopic) (*FSSubscription, error) {
	subPath := path.Join(topic.topicPath, subName)
	fsSub := &FSSubscription{
		name:          subName,
		topic:         topic,
		subPath:       subPath,
		streamClients: make(map[string]*StreamingSubcription),
		msgsChannel:   utils.NewDynamicUUIDChannel(),
		running:       true,
	}
	psSub := &pubsub.Subscription{}
	fp, err := os.Open(fsSub.GetSubFilePath())
	if err != nil {
		return nil, err
	}
	data, err := ioutil.ReadAll(fp)
	if err != nil {
		return nil, err
	}
	err = proto.Unmarshal(data, psSub)
	if err != nil {
		return nil, err
	}
	fsSub.pubsubSubscription = psSub
	fse, err := os.ReadDir(fsSub.subPath)
	if err != nil {
		return nil, err
	}
	for _, fsi := range fse {
		if !strings.HasSuffix(fsi.Name(), ".msg.proto") {
			continue
		}
		smid := fsi.Name()[:36]
		mid, err := uuid.Parse(smid)
		if err != nil {
			continue
		}
		fsSub.msgsChannel.Add() <- mid
	}
	logger.Info("Loaded Sub:{} for Topic:{} in Project:{}", subName, topic.name, topic.project.name)
	go fsSub.watchAcks()
	return fsSub, nil
}

func (fss *FSSubscription) watchAcks() {
	ackTimer := time.NewTimer(ack_check_time)
	for fss.running {
		<-ackTimer.C
		fss.nackMessages()
		ackTimer.Reset(ack_check_time)
	}
}

func (fss *FSSubscription) GetTopic() base.BaseTopic {
	return fss.topic
}

func (fss *FSSubscription) GetName() string {
	return fss.name
}

func (fss *FSSubscription) GetSubFilePath() string {
	return path.Join(fss.subPath, fmt.Sprintf("%s.sub.proto", fss.name))
}

func (fss *FSSubscription) GetSubscriptionPubSub() *pubsub.Subscription {
	return fss.pubsubSubscription
}

func (fss *FSSubscription) Publish(msg *pubsub.PubsubMessage) {
	data, _ := proto.Marshal(msg)
	tmpPath := path.Join(fss.subPath, fmt.Sprintf("%s.tmp-msg.proto", msg.MessageId))
	msgPath := path.Join(fss.subPath, fmt.Sprintf("%s.msg.proto", msg.MessageId))
	ioutil.WriteFile(tmpPath, data, os.ModePerm)
	// We Rename to keep from sending notify too soon before we write data
	os.Rename(tmpPath, msgPath)
	logger.Debug("Sub:{} New Message mid:{}", fss.name, msg.MessageId)
	fss.msgsChannel.Add() <- uuid.MustParse(msg.MessageId)
}

func (fss *FSSubscription) UpdateAcks(ackIds []string, deadline int32) {
	doTime := time.Second * time.Duration(deadline)
	if doTime < time.Second {
		doTime = time.Duration(0)
	} else if doTime > base.MAX_ACK_TIME {
		doTime = base.MAX_ACK_TIME
	}
	ackTill := time.Now().Add(doTime)
	fss.ackLock.Lock()
	defer fss.ackLock.Unlock()
	// We do this here to save multipule locks if we do UpdateAck
	for _, mid := range ackIds {
		logger.Debug("Sub:{} Updating add Ack:{} by:{}", fss.name, mid, doTime)
		msgPath := path.Join(fss.subPath, fmt.Sprintf("%s.ack.proto", mid))
		_, err := os.Stat(msgPath)
		if err != nil {
			continue
		}
		os.Chtimes(msgPath, ackTill, ackTill)
	}
}

func (fss *FSSubscription) UpdateAck(ackId string, addTime time.Duration) {
	doTime := addTime
	if addTime < time.Second {
		doTime = time.Duration(0)
	} else if addTime > base.MAX_ACK_TIME {
		doTime = base.MAX_ACK_TIME
	}
	ackTill := time.Now().Add(doTime)
	fss.ackLock.Lock()
	defer fss.ackLock.Unlock()
	msgPath := path.Join(fss.subPath, fmt.Sprintf("%s.ack.proto", ackId))
	_, err := os.Stat(msgPath)
	if err != nil {
		return
	}
	os.Chtimes(msgPath, ackTill, ackTill)

}

func (fss *FSSubscription) nackMessages() {
	fss.ackLock.Lock()
	defer fss.ackLock.Unlock()
	nacks := make([]uuid.UUID, 0)
	fsil, _ := os.ReadDir(fss.subPath)
	for _, de := range fsil {
		if !strings.HasSuffix(de.Name(), ".ack.proto") {
			continue
		}
		ackPath := path.Join(fss.subPath, de.Name())
		mid := strings.Split(de.Name(), ".")[0]
		msgPath := path.Join(fss.subPath, fmt.Sprintf("%s.msg.proto", mid))
		fsi, err := os.Stat(ackPath)
		if err != nil {
			continue
		}
		if time.Until(fsi.ModTime()) <= time.Duration(0) {
			logger.Info("{}:{}:{}: Deadline hit, NACKing message:{}", fss.topic.project.name, fss.topic.name, fss.name, mid)
			os.Rename(ackPath, msgPath)
			nacks = append(nacks, uuid.MustParse(mid))
		}
	}
	if len(nacks) > 0 {
		fss.clientLock.Lock()
		defer fss.clientLock.Unlock()
		for _, nack := range nacks {
			for _, ss := range fss.streamClients {
				ss.acker.Add() <- nack
			}
		}
		for _, nack := range nacks {
			fss.msgsChannel.Add() <- nack
		}
	}
}

func (fss *FSSubscription) AckMessages(ackIds []string) {
	for _, ackid := range ackIds {
		logger.Debug("Sub:{} Acking:{}", fss.name, ackid)
		ackFile := path.Join(fss.subPath, fmt.Sprintf("%s.ack.proto", ackid))
		os.RemoveAll(ackFile)
	}
	fss.clientLock.Lock()
	defer fss.clientLock.Unlock()
	for _, ackid := range ackIds {
		ack_uuid := uuid.MustParse(ackid)
		for _, ss := range fss.streamClients {
			ss.acker.Add() <- ack_uuid
		}
	}
}

func (fss *FSSubscription) getLocalMessages(msgId uuid.UUID) *pubsub.ReceivedMessage {
	fss.msgLock.Lock()
	defer fss.msgLock.Unlock()
	msgPath := path.Join(fss.subPath, fmt.Sprintf("%s.msg.proto", msgId))
	msg, err := ReadMessage(msgPath)
	if err != nil {
		return nil
	}
	err = fss.msgToAck(msgId)
	if err != nil {
		return nil
	}
	return &pubsub.ReceivedMessage{
		AckId:   msg.MessageId,
		Message: msg,
	}
}

func (fss *FSSubscription) msgToAck(mid uuid.UUID) error {
	smid := mid.String()
	msgPath := path.Join(fss.subPath, fmt.Sprintf("%s.msg.proto", smid))
	ackPath := path.Join(fss.subPath, fmt.Sprintf("%s.ack.proto", smid))
	setDeadline := fss.pubsubSubscription.AckDeadlineSeconds
	ackTill := time.Now().Add(time.Second * time.Duration(setDeadline))
	fss.ackLock.Lock()
	defer fss.ackLock.Unlock()
	err := os.Rename(msgPath, ackPath)
	if err != nil {
		return err
	}
	_, err = os.Stat(ackPath)
	if err != nil {
		return err
	}
	os.Chtimes(ackPath, ackTill, ackTill)
	return nil
}

func (fss *FSSubscription) GetMessages(max int32, maxWait time.Duration) []*pubsub.ReceivedMessage {
	msgs := make([]*pubsub.ReceivedMessage, 0)
	timer := time.NewTimer(maxWait)
	for {
		select {
		case mid := <-fss.msgsChannel.Get():
			msg := fss.getLocalMessages(mid)
			if msg == nil {
				continue
			}
			msgs = append(msgs, msg)
			for int32(len(msgs)) < max {
				select {
				case mid := <-fss.msgsChannel.Get():
					msg := fss.getLocalMessages(mid)
					if msg != nil {
						msgs = append(msgs, msg)
					}
				case <-time.After(time.Millisecond):
					return msgs
				}
			}
			return msgs
		case <-timer.C:
			return msgs
		}
	}
}

func (fss *FSSubscription) Delete() {
	fss.running = false
	fss.clientLock.Lock()
	defer fss.clientLock.Unlock()
	for _, ss := range fss.streamClients {
		ss.running = false
		ss.streamingServer.Context().Done()
		ss.acker.Stop()
	}
	fss.msgsChannel.Stop()
	os.RemoveAll(fss.subPath)
}

func (fss *FSSubscription) CreateStreamingSubscription(firstRecvMsg *pubsub.StreamingPullRequest, streamingServer pubsub.Subscriber_StreamingPullServer) base.BaseStreamingSubcription {
	cid := uuid.NewString()
	logger.Info("Project:{}:Topic:{}:Sub:{}, Creating SubStream:{}", fss.topic.project.name, fss.topic.name, fss.name, cid)
	maxMsg := int64(10)
	if firstRecvMsg.MaxOutstandingMessages > 0 {
		maxMsg = firstRecvMsg.MaxOutstandingMessages
	}
	ss := &StreamingSubcription{
		sub:             fss,
		streamingServer: streamingServer,
		clientId:        cid,
		maxMsgs:         maxMsg,
		maxBytes:        firstRecvMsg.MaxOutstandingBytes,
		deadline:        time.Second * time.Duration(firstRecvMsg.StreamAckDeadlineSeconds),
		running:         true,
		pendingMsgs:     make(map[uuid.UUID]bool),
		acker:           utils.NewDynamicUUIDChannel(),
		recvChan:        make(chan *pubsub.StreamingPullRequest),
	}
	fss.clientLock.Lock()
	defer fss.clientLock.Unlock()
	fss.streamClients[cid] = ss
	go ss.watchRecv()

	return ss
}

func (fss *FSSubscription) DeleteStreamingSubscription(ss *StreamingSubcription) {
	fss.clientLock.Lock()
	defer fss.clientLock.Unlock()
	delete(fss.streamClients, ss.clientId)
}

type StreamingSubcription struct {
	streamingServer pubsub.Subscriber_StreamingPullServer
	sub             *FSSubscription
	maxMsgs         int64
	maxBytes        int64
	pendingMsgs     map[uuid.UUID]bool
	currentBytes    int64
	clientId        string
	deadline        time.Duration
	recvChan        chan *pubsub.StreamingPullRequest
	acker           *utils.DynamicUUIDChannel
	running         bool
}

func (ss *StreamingSubcription) Run() {
	defer ss.sub.DeleteStreamingSubscription(ss)
	for ss.running {
		currentMessageCount := int64(len(ss.pendingMsgs))
		if currentMessageCount >= ss.maxMsgs {
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
				return
			}
		}
	}
}

func (ss *StreamingSubcription) msgSelect() []*pubsub.ReceivedMessage {
	msgs := make([]*pubsub.ReceivedMessage, 0)
	select {
	case mid := <-ss.sub.msgsChannel.Get():
		msg := ss.sub.getLocalMessages(mid)
		if msg == nil {
			return msgs
		}
		logger.Debug("Sub:{} Sending mid:{}", ss.sub.name, msg.AckId)
		ss.pendingMsgs[mid] = true
		msgs = append(msgs, msg)
		maxWait := time.NewTimer(time.Millisecond * 5)
		for len(ss.pendingMsgs) < int(ss.maxMsgs) {
			runtime.Gosched()
			select {
			case mid := <-ss.sub.msgsChannel.Get():
				msg := ss.sub.getLocalMessages(mid)
				if msg != nil {
					ss.pendingMsgs[mid] = true
					logger.Debug("Sub:{} Sending mid:{}", ss.sub.name, msg.AckId)
					msgs = append(msgs, msg)
				}
			case <-maxWait.C:
				return msgs
			}
		}
		return msgs
	case aid := <-ss.acker.Get():
		delete(ss.pendingMsgs, aid)
	case recvMsg := <-ss.recvChan:
		if len(recvMsg.AckIds) > 0 {
			ss.sub.AckMessages(recvMsg.AckIds)
		}
		if len(recvMsg.ModifyDeadlineAckIds) > 0 {
			for i := range recvMsg.ModifyDeadlineAckIds {
				ss.sub.UpdateAck(recvMsg.ModifyDeadlineAckIds[i], time.Duration(recvMsg.ModifyDeadlineSeconds[i])*time.Second)
			}
		}
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
				ss.sub.UpdateAck(recvMsg.ModifyDeadlineAckIds[i], time.Duration(recvMsg.ModifyDeadlineSeconds[i])*time.Second)
			}
		}
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
