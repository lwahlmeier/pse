package mempubsub

import (
	"sync"

	"githb.com/lwahlmeier/go-pubsub-emulator/internal/base"
	"google.golang.org/genproto/googleapis/pubsub/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type MemTopic struct {
	project *MemProject
	name    string
	topic   *pubsub.Topic
	subs    map[string]*MemSubscription
	subLock sync.Mutex
}

func NewMemTopic(project *MemProject, topic *pubsub.Topic) *MemTopic {
	return &MemTopic{
		project: project,
		topic:   topic,
		subs:    make(map[string]*MemSubscription),
	}
}

func (mt *MemTopic) GetProject() base.BaseProject {
	return mt.project
}
func (mt *MemTopic) GetName() string {
	return mt.name
}
func (mt *MemTopic) CreateSub(sub *pubsub.Subscription) error {
	_, subName, err := mt.project.memBase.ParseProjectAndSubscriptionName(sub.Name)
	if err != nil {
		return err
	}
	mt.subLock.Lock()
	defer mt.subLock.Unlock()
	if _, ok := mt.subs[subName]; ok {
		return status.Error(codes.AlreadyExists, "Sub Already Exists")
	}
	mt.subs[subName] = NewMemSub(subName, mt, sub)
	return nil
}
func (mt *MemTopic) GetSub(subName string) base.BaseSubscription {
	mt.subLock.Lock()
	defer mt.subLock.Unlock()
	if sub, ok := mt.subs[subName]; ok {
		return sub
	}
	return nil
}
func (mt *MemTopic) DeleteSub(subName string) {
	mt.subLock.Lock()
	defer mt.subLock.Unlock()
	delete(mt.subs, subName)
}
func (mt *MemTopic) GetAllSubs() []base.BaseSubscription {
	baseSubs := make([]base.BaseSubscription, 0)
	mt.subLock.Lock()
	defer mt.subLock.Unlock()
	for _, sub := range mt.subs {
		baseSubs = append(baseSubs, sub)
	}
	return baseSubs
}
func (mt *MemTopic) GetTopicPubSub() *pubsub.Topic {
	return mt.topic
}
func (mt *MemTopic) PublishMessage(msg *pubsub.PubsubMessage) error {
	mt.subLock.Lock()
	defer mt.subLock.Unlock()
	for _, sub := range mt.subs {
		sub.PublishMessage(msg)
	}
	return nil
}
