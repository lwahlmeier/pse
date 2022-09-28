package fspubsub

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"sync"

	"githb.com/lwahlmeier/go-pubsub-emulator/internal/base"
	"google.golang.org/genproto/googleapis/pubsub/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type FSTopic struct {
	project     *FSProject
	name        string
	topicPath   string
	pubsubTopic *pubsub.Topic
	subs        map[string]*FSSubscriptions
	lock        sync.Mutex
}

func CreateFSTopic(topicName string, project *FSProject, topic *pubsub.Topic) (*FSTopic, error) {
	basePath := path.Join(project.projectPath, topicName)
	_, err := os.Stat(basePath)
	if err == nil {
		return nil, status.Error(codes.AlreadyExists, "Topic Already Exists")
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	logger.Info("Creating Topic:{} for Project:{}", topicName, project.name)
	err = os.MkdirAll(basePath, os.ModePerm)
	if err != nil {
		return nil, err
	}
	fst := &FSTopic{
		project:     project,
		name:        topicName,
		topicPath:   basePath,
		pubsubTopic: topic,
		subs:        make(map[string]*FSSubscriptions, 0),
	}
	tfp := fst.GetTopicFilePath()
	data, err := proto.Marshal(topic)
	if err != nil {
		return nil, err
	}
	err = ioutil.WriteFile(tfp, data, os.ModePerm)
	if err != nil {
		return nil, err
	}
	return fst, nil
}

func LoadFSTopic(topicName string, project *FSProject) (*FSTopic, error) {
	basePath := path.Join(project.projectPath, topicName)
	fst := &FSTopic{
		project:   project,
		name:      topicName,
		topicPath: basePath,
		subs:      make(map[string]*FSSubscriptions, 0),
	}
	tfp := fst.GetTopicFilePath()
	fp, err := os.Open(tfp)
	if err != nil {
		return nil, err
	}

	data, err := ioutil.ReadAll(fp)
	if err != nil {
		return nil, err
	}
	topic := &pubsub.Topic{}
	err = proto.Unmarshal(data, topic)
	if err != nil {
		return nil, err
	}
	fst.pubsubTopic = topic
	logger.Info("Loaded Topic:{} for Project:{} ", topicName, project.name)
	fst.loadSubs()
	return fst, nil
}

func (fst *FSTopic) loadSubs() error {
	fsil, err := os.ReadDir(fst.topicPath)
	if err != nil {
		return err
	}
	fsSubs := make([]*FSSubscriptions, 0)
	for _, fsi := range fsil {
		if !fsi.IsDir() {
			continue
		}
		fsSub, err := LoadSubscription(fsi.Name(), fst)
		if err != nil {
			continue
		}
		fsSubs = append(fsSubs, fsSub)
	}
	fst.lock.Lock()
	defer fst.lock.Unlock()
	for _, fsSub := range fsSubs {
		fst.subs[fsSub.name] = fsSub
	}
	return nil
}

func (fst *FSTopic) GetTopicFilePath() string {
	return path.Join(fst.topicPath, fmt.Sprintf("%s.topic.proto", fst.name))
}

func (fst *FSTopic) GetTopicPubSub() *pubsub.Topic {
	return fst.pubsubTopic
}

func (fst *FSTopic) GetProject() base.BaseProject {
	return fst.project
}

func (fst *FSTopic) GetName() string {
	return fst.name
}

func (fst *FSTopic) CreateSub(sub *pubsub.Subscription) error {
	subName, err := GetSubscriptionName(sub.Name)
	if err != nil {
		return err
	}
	fst.lock.Lock()
	defer fst.lock.Unlock()
	if _, ok := fst.subs[subName]; ok {
		return status.Error(codes.AlreadyExists, "Sub Already Exists")
	}
	fsSub, err := CreateSubscription(fst.topicPath, fst, sub)
	if err != nil {
		return err
	}
	fst.subs[subName] = fsSub
	return nil
}

func (fst *FSTopic) GetSub(subName string) base.BaseSubscription {
	fst.lock.Lock()
	defer fst.lock.Unlock()
	if sub, ok := fst.subs[subName]; ok {
		return sub
	}
	return nil
}

func (fst *FSTopic) DeleteSub(subName string) {
	fst.lock.Lock()
	defer fst.lock.Unlock()
	if sub, ok := fst.subs[subName]; ok {
		delete(fst.subs, subName)
		os.RemoveAll(sub.subPath)
		logger.Info("Deleted Sub:{} for Topic:{} in Project:{}", subName, fst.name, fst.project.name)
	}
}

func (fst *FSTopic) GetAllSubs() []base.BaseSubscription {
	fsSubs := make([]base.BaseSubscription, 0)
	for _, fsSub := range fst.subs {
		fsSubs = append(fsSubs, fsSub)
	}
	return fsSubs
}

func (fst *FSTopic) PublishMessage(msg *pubsub.PubsubMessage) error {
	fst.lock.Lock()
	defer fst.lock.Unlock()
	for _, fsSub := range fst.subs {
		fsSub.Publish(msg)
	}
	return nil
}
