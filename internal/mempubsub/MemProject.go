package mempubsub

import (
	"sync"

	"githb.com/lwahlmeier/go-pubsub-emulator/internal/base"
	"google.golang.org/genproto/googleapis/pubsub/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type MemProject struct {
	memBase   *MemBase
	name      string
	topics    map[string]*MemTopic
	topicLock sync.Mutex
}

func NewMemProject(name string, memBase *MemBase) *MemProject {
	logger.Info("Creating Project:{}", name)
	return &MemProject{
		memBase: memBase,
		name:    name,
		topics:  make(map[string]*MemTopic),
	}
}

func (mp *MemProject) GetBase() base.BaseBackend {
	return mp.memBase
}
func (mp *MemProject) GetName() string {
	return mp.name
}
func (mp *MemProject) CreateTopic(topic *pubsub.Topic) error {
	_, topicName, err := mp.memBase.ParseProjectAndTopicName(topic.Name)
	if err != nil {
		return err
	}
	mp.topicLock.Lock()
	defer mp.topicLock.Unlock()
	if _, ok := mp.topics[topicName]; ok {
		return status.Errorf(codes.AlreadyExists, "Topic Already exists")
	}
	mp.topics[topicName] = NewMemTopic(topicName, mp, topic)
	return nil
}
func (mp *MemProject) GetTopic(topicName string) base.BaseTopic {
	mp.topicLock.Lock()
	defer mp.topicLock.Unlock()
	if topic, ok := mp.topics[topicName]; ok {
		return topic
	}
	return nil
}

func (mp *MemProject) DeleteTopic(topicName string) {
	mp.topicLock.Lock()
	defer mp.topicLock.Unlock()
	if _, ok := mp.topics[topicName]; ok {
		delete(mp.topics, topicName)
		logger.Info("Deleted Topic:{} for Project:{}", topicName, mp.name)
	}
}

func (mp *MemProject) GetAllTopics() map[string]base.BaseTopic {
	mp.topicLock.Lock()
	defer mp.topicLock.Unlock()
	topics := make(map[string]base.BaseTopic)
	for topicName, topic := range mp.topics {
		topics[topicName] = topic
	}
	return topics
}
func (mp *MemProject) GetAllSubscriptions() map[string]base.BaseSubscription {
	mp.topicLock.Lock()
	defer mp.topicLock.Unlock()
	subs := make(map[string]base.BaseSubscription)
	for _, topic := range mp.topics {
		for _, sub := range topic.GetAllSubs() {
			subs[sub.GetName()] = sub
		}
	}
	return subs
}
