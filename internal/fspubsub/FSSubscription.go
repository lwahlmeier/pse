package fspubsub

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	"google.golang.org/genproto/googleapis/pubsub/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type FSSubscriptions struct {
	name               string
	topic              *FSTopic
	subPath            string
	pubsubSubscription *pubsub.Subscription
	newMsg             chan string
	msgLock            sync.Mutex
	ackLock            sync.Mutex
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
	logger.Info("Project:{}:Topic:{} loading Sub:{}", topic.project.Name, topic.name, subName)
	subPath := path.Join(topic.topicPath, subName)
	fsSub := &FSSubscriptions{
		name:    subName,
		topic:   topic,
		subPath: subPath,
		newMsg:  make(chan string),
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

func (fss *FSSubscriptions) GetSubFilePath() string {
	return path.Join(fss.subPath, fmt.Sprintf("%s.sub.proto", fss.name))
}

func (fss *FSSubscriptions) GetPubSubSubscription() *pubsub.Subscription {
	return fss.pubsubSubscription
}

func (fss *FSSubscriptions) Publish(msg *pubsub.PubsubMessage) {
	data, _ := proto.Marshal(msg)
	msgPath := path.Join(fss.subPath, fmt.Sprintf("%s.msg.proto", msg.MessageId))
	ioutil.WriteFile(msgPath, data, os.ModePerm)
}

func (fss *FSSubscriptions) UpdateAcks(ackIds []string, deadline int32) {
	setDeadline := math.Max(0, float64(deadline))
	setDeadline = math.Min(600, float64(setDeadline))
	ackTill := time.Now().Add(time.Second * time.Duration(setDeadline))
	fss.ackLock.Lock()
	defer fss.ackLock.Unlock()
	for _, mid := range ackIds {
		logger.Info("Updating Ack for Message:{} extending {}s", mid, int(setDeadline))
		msgPath := path.Join(fss.subPath, fmt.Sprintf("%s.ack.proto", mid))
		_, err := os.Stat(msgPath)
		if err != nil {
			continue
		}
		os.Chtimes(msgPath, ackTill, ackTill)
	}
}

func (fss *FSSubscriptions) nackMessages() {
	fss.ackLock.Lock()
	defer fss.ackLock.Unlock()
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
}

func (fss *FSSubscriptions) getLocalMessages(max int32) []*pubsub.ReceivedMessage {
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

func (fss *FSSubscriptions) GetMessages(max int32) []*pubsub.ReceivedMessage {
	msgs := fss.getLocalMessages(max)
	startTime := time.Now()
	for len(msgs) == 0 && time.Since(startTime) < time.Second*30 {
		select {
		case <-fss.newMsg:
			msgs = fss.getLocalMessages(max)
		case <-time.After(time.Second * 1):

		}
	}
	return msgs
}

func (fss *FSSubscriptions) Delete() {
	os.RemoveAll(fss.subPath)
	fss.topic.RemoveSub(fss.name)
}
