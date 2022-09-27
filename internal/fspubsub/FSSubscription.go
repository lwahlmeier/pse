package fspubsub

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"githb.com/lwahlmeier/go-pubsub-emulator/internal/base"
	"githb.com/lwahlmeier/go-pubsub-emulator/internal/utils"
	"github.com/fsnotify/fsnotify"
	"github.com/google/uuid"
	"google.golang.org/genproto/googleapis/pubsub/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

//TODO: This can probably be simplified to using a single select loop for
//managing messages and acks through channel -> disk -> notify
//possibly better then using inotify
type FSSubscriptions struct {
	name               string
	topic              *FSTopic
	subPath            string
	pubsubSubscription *pubsub.Subscription
	newMsg             chan string
	msgLock            sync.Mutex
	ackLock            sync.Mutex
	streamClients      map[string]*StreamingSubcription
	clientLock         sync.Mutex
}

func CreateSubscription(basePath string, topic *FSTopic, sub *pubsub.Subscription) (*FSSubscriptions, error) {
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
	fsSub := &FSSubscriptions{
		name:               subName,
		topic:              topic,
		subPath:            subPath,
		pubsubSubscription: sub,
		newMsg:             make(chan string),
		streamClients:      make(map[string]*StreamingSubcription),
	}

	sfp := fsSub.GetSubFilePath()
	data, err := proto.Marshal(sub)
	if err != nil {
		return nil, err
	}
	err = ioutil.WriteFile(sfp, data, os.ModePerm)
	if err != nil {
		return nil, err
	}
	go fsSub.watchMessages()
	return fsSub, nil
}

func LoadSubscription(subName string, topic *FSTopic) (*FSSubscriptions, error) {
	logger.Info("Project:{}:Topic:{} loading Sub:{}", topic.project.name, topic.name, subName)
	subPath := path.Join(topic.topicPath, subName)
	fsSub := &FSSubscriptions{
		name:          subName,
		topic:         topic,
		subPath:       subPath,
		newMsg:        make(chan string),
		streamClients: make(map[string]*StreamingSubcription),
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
	go fsSub.watchMessages()
	return fsSub, nil
}

func (fss *FSSubscriptions) watchMessages() {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		logger.Fatal(err)
	}
	defer watcher.Close()
	five_sec := time.Second * 5
	ackTimer := time.NewTimer(five_sec)
	watcher.Add(fss.subPath)
	for {
		select {
		case <-ackTimer.C:
			fss.nackMessages()
			ackTimer.Reset(five_sec)
		case event, ok := <-watcher.Events:
			if !ok {
				return
			}
			if event.Op == fsnotify.Create {
				if !strings.HasSuffix(event.Name, ".msg.proto") {
					break
				}
				logger.Info("New Message:{}", event)
				select {
				case fss.newMsg <- event.Name:
				default:
				}
			}
		case err, ok := <-watcher.Errors:
			if !ok {
				return
			}
			log.Println("error:", err)
		}
	}

}

func (fss *FSSubscriptions) GetTopic() base.BaseTopic {
	return fss.topic
}

func (fss *FSSubscriptions) GetName() string {
	return fss.name
}

func (fss *FSSubscriptions) GetSubFilePath() string {
	return path.Join(fss.subPath, fmt.Sprintf("%s.sub.proto", fss.name))
}

func (fss *FSSubscriptions) GetSubscriptionPubSub() *pubsub.Subscription {
	return fss.pubsubSubscription
}

func (fss *FSSubscriptions) Publish(msg *pubsub.PubsubMessage) {
	data, _ := proto.Marshal(msg)
	tmpPath := path.Join(fss.subPath, fmt.Sprintf("%s.tmp-msg.proto", msg.MessageId))
	msgPath := path.Join(fss.subPath, fmt.Sprintf("%s.msg.proto", msg.MessageId))
	ioutil.WriteFile(tmpPath, data, os.ModePerm)
	// We Rename to keep from sending notify too soon before we write data
	os.Rename(tmpPath, msgPath)
}

func (fss *FSSubscriptions) UpdateAcks(ackIds []string, deadline int32) {
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
		logger.Info("Updating Ack for Message:{} extending {}", mid, doTime)
		msgPath := path.Join(fss.subPath, fmt.Sprintf("%s.ack.proto", mid))
		_, err := os.Stat(msgPath)
		if err != nil {
			continue
		}
		os.Chtimes(msgPath, ackTill, ackTill)
	}
}

func (fss *FSSubscriptions) UpdateAck(ackId string, addTime time.Duration) {
	doTime := addTime
	if addTime < time.Second {
		doTime = time.Duration(0)
	} else if addTime > base.MAX_ACK_TIME {
		doTime = base.MAX_ACK_TIME
	}
	ackTill := time.Now().Add(doTime)
	fss.ackLock.Lock()
	defer fss.ackLock.Unlock()
	logger.Info("Updating Ack for Message:{} extending {}", ackId, doTime)
	msgPath := path.Join(fss.subPath, fmt.Sprintf("%s.ack.proto", ackId))
	_, err := os.Stat(msgPath)
	if err != nil {
		return
	}
	os.Chtimes(msgPath, ackTill, ackTill)

}

func (fss *FSSubscriptions) nackMessages() {
	fss.ackLock.Lock()
	defer fss.ackLock.Unlock()
	nacks := make([]string, 0)
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
			logger.Info("Message Hit Nack Deadline:{}", mid)
			os.Rename(ackPath, msgPath)
			nacks = append(nacks, mid)
		}
	}
	if len(nacks) > 0 {
		fss.clientLock.Lock()
		defer fss.clientLock.Unlock()
		for _, nack := range nacks {
			nack_uuid := uuid.MustParse(nack)
			for _, ss := range fss.streamClients {
				ss.acker.Add() <- nack_uuid
			}
		}
	}
}

func (fss *FSSubscriptions) AckMessages(ackIds []string) {
	logger.Info("Got ACK:{}", ackIds)
	for _, ackid := range ackIds {
		ackFile := path.Join(fss.subPath, fmt.Sprintf("%s.ack.proto", ackid))
		// os.Rename(ackFile, path.Join(fss.subPath, fmt.Sprintf("%s.done.proto", ackid)))
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

func (fss *FSSubscriptions) getLocalMessages(max int32) []*pubsub.ReceivedMessage {
	logger.Info("getLocalMessages:{}", max)
	fss.msgLock.Lock()
	defer fss.msgLock.Unlock()
	msgs := make([]*pubsub.ReceivedMessage, 0)
	fsil, _ := os.ReadDir(fss.subPath)
	for _, fsi := range fsil {
		if !strings.HasSuffix(fsi.Name(), ".msg.proto") {
			continue
		}
		mid := strings.Split(fsi.Name(), ".")[0]
		msgPath := path.Join(fss.subPath, fsi.Name())
		msg, err := ReadMessage(msgPath)
		if err != nil {
			logger.Warn("Error:{}", err)
			continue
		}

		rmsg := &pubsub.ReceivedMessage{
			AckId:   msg.MessageId,
			Message: msg,
		}
		err = fss.msgToAck(mid)
		if err != nil {
			continue
		}
		msgs = append(msgs, rmsg)
		if int32(len(msgs)) >= max {
			break
		}
	}
	return msgs
}

func (fss *FSSubscriptions) msgToAck(mid string) error {
	msgPath := path.Join(fss.subPath, fmt.Sprintf("%s.msg.proto", mid))
	ackPath := path.Join(fss.subPath, fmt.Sprintf("%s.ack.proto", mid))
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

func (fss *FSSubscriptions) GetMessages(max int32, maxWait time.Duration) []*pubsub.ReceivedMessage {
	msgs := fss.getLocalMessages(max)
	if len(msgs) > 0 {
		return msgs
	}

	timer := time.NewTimer(maxWait)
	for len(msgs) == 0 {
		select {
		case <-fss.newMsg:
			msgs = fss.getLocalMessages(max)
		case <-timer.C:
			return msgs
		}
	}
	if !timer.Stop() {
		<-timer.C
	}
	return msgs
}

func (fss *FSSubscriptions) Delete() {
	os.RemoveAll(fss.subPath)
	fss.topic.DeleteSub(fss.name)
}

func (fss *FSSubscriptions) CreateStreamingSubscription(firstRecvMsg *pubsub.StreamingPullRequest, streamingServer pubsub.Subscriber_StreamingPullServer) base.BaseStreamingSubcription {
	cid := uuid.NewString()
	logger.Info("Creating SubStream:{}", cid)
	ss := &StreamingSubcription{
		sub:             fss,
		streamingServer: streamingServer,
		clientId:        cid,
		maxMsgs:         firstRecvMsg.MaxOutstandingMessages,
		maxBytes:        firstRecvMsg.MaxOutstandingBytes,
		deadline:        time.Second * time.Duration(firstRecvMsg.StreamAckDeadlineSeconds),
		running:         true,
		pendingMsgs:     make(map[uuid.UUID]bool),
		acker:           utils.NewDynamicUUIDChannel(),
	}
	fss.clientLock.Lock()
	defer fss.clientLock.Unlock()
	fss.streamClients[cid] = ss
	go ss.watchRecv()

	return ss
}

func (fss *FSSubscriptions) DeleteStreamingSubscription(ss *StreamingSubcription) {
	fss.clientLock.Lock()
	defer fss.clientLock.Unlock()
	delete(fss.streamClients, ss.clientId)
}

type StreamingSubcription struct {
	streamingServer pubsub.Subscriber_StreamingPullServer
	sub             *FSSubscriptions
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
	case <-ss.sub.newMsg:
		currentMessageCount := int64(len(ss.pendingMsgs))
		msgs = ss.sub.getLocalMessages(int32(ss.maxMsgs - currentMessageCount))
		for _, msg := range msgs {
			ss.pendingMsgs[uuid.MustParse(msg.AckId)] = true
		}
	case aid := <-ss.acker.Get():
		delete(ss.pendingMsgs, aid)
	case recvMsg := <-ss.recvChan:
		if len(recvMsg.AckIds) > 0 {
			logger.Info("{}: Acking:{}", ss.clientId, len(recvMsg.AckIds))
			ss.sub.AckMessages(recvMsg.AckIds)
		}
		if len(recvMsg.ModifyDeadlineAckIds) > 0 {
			logger.Info("{}: ModAck:{}", ss.clientId, (recvMsg.ModifyDeadlineSeconds))
			for i := range recvMsg.ModifyDeadlineAckIds {
				ss.sub.UpdateAcks([]string{recvMsg.ModifyDeadlineAckIds[i]}, recvMsg.ModifyDeadlineSeconds[i])
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
			logger.Info("{}: Acking:{}", ss.clientId, len(recvMsg.AckIds))
			ss.sub.AckMessages(recvMsg.AckIds)
		}
		if len(recvMsg.ModifyDeadlineAckIds) > 0 {
			logger.Info("{}: ModAck:{}", ss.clientId, (recvMsg.ModifyDeadlineSeconds))
			for i := range recvMsg.ModifyDeadlineAckIds {
				ss.sub.UpdateAcks([]string{recvMsg.ModifyDeadlineAckIds[i]}, recvMsg.ModifyDeadlineSeconds[i])
			}
		}
	}
}

func (ss *StreamingSubcription) watchRecv() {
	for ss.running {
		msg, err := ss.streamingServer.Recv()
		logger.Info("{}: WatchRecv: {}", ss.clientId, msg)
		if !ss.running || err != nil {
			logger.Warn("Error StreamRecv:{}", err.Error())
			ss.running = false
			return
		}
		ss.recvChan <- msg
	}
}
