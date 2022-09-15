package fspubsub

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"sync"

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

func CreateFSTopic(project *FSProject, topic *pubsub.Topic) (*FSTopic, error) {
	topicName, err := GetTopicName(topic.Name)
	if err != nil {
		return nil, err
	}
	basePath := path.Join(project.ProjectPath, topicName)
	_, err = os.Stat(basePath)
	if err == nil {
		return nil, status.Error(codes.AlreadyExists, "Topic Already Exists")
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
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

func LoadTopic(project *FSProject, topicName string) (*FSTopic, error) {
	basePath := path.Join(project.ProjectPath, topicName)
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
	return fst, nil
}

func (fst *FSTopic) Delete() {
	logger.Info("Project:{}:Topic:{}, Deleteing", fst.project.Name, fst.name)
	os.RemoveAll(fst.topicPath)
	fst.project.RemoveTopic(fst.name)
}

func (fst *FSTopic) GetTopicFilePath() string {
	return path.Join(fst.topicPath, fmt.Sprintf("%s.topic.proto", fst.name))
}

func (fst *FSTopic) GetPubSubTopic() *pubsub.Topic {
	return fst.pubsubTopic
}

func (fst *FSTopic) AddSub(sub *pubsub.Subscription) error {
	return nil
}

func (fst *FSTopic) RemoveSub(subName string) {
	fst.lock.Lock()
	defer fst.lock.Unlock()
	delete(fst.subs, subName)
}

func (fst *FSTopic) GetSubscription(subName string) *FSSubscriptions {
	return nil
}

func (fst *FSTopic) GetAllSubscriptions() []*FSSubscriptions {
	return nil
}

func (fst *FSTopic) Publish(msg *pubsub.PubsubMessage) error {
	return nil
}
