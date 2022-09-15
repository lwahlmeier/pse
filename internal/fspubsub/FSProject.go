package fspubsub

import (
	"errors"
	"os"
	"path"
	"sync"

	"google.golang.org/genproto/googleapis/pubsub/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type FSProject struct {
	Name          string
	Topics        map[string]*FSTopic
	Subscriptions map[string]*FSSubscriptions
	ProjectPath   string
	projectLock   sync.Mutex
}

func CreateProject(name, basePath string) (*FSProject, error) {
	projectPath := path.Join(basePath, name)
	_, err := os.Stat(projectPath)
	if err == nil {
		return nil, status.Error(codes.AlreadyExists, "Project Already Exists!")
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	err = os.MkdirAll(projectPath, os.ModePerm)
	if err != nil {
		return nil, err
	}
	return &FSProject{
		Name:          name,
		ProjectPath:   projectPath,
		Topics:        make(map[string]*FSTopic),
		Subscriptions: make(map[string]*FSSubscriptions),
	}, nil

}

func LoadProject(name, basePath string) (*FSProject, error) {
	projectPath := path.Join(basePath, name)
	stat, err := os.Stat(projectPath)
	if err != nil {
		return nil, err
	}
	if !stat.IsDir() {
		return nil, status.Error(codes.Internal, "Can not create project")
	}
	pj := &FSProject{
		ProjectPath:   projectPath,
		Name:          name,
		Topics:        make(map[string]*FSTopic),
		Subscriptions: make(map[string]*FSSubscriptions),
	}
	return pj, nil
	// pjt.projectLock.Lock()
	// defer pjt.projectLock.Unlock()
	// fi, err := os.Stat(pjt.ProjectPath)
	// if err != nil {
	// 	if !errors.Is(err, os.ErrNotExist) {
	// 		panic(err)
	// 	}
	// 	err = os.MkdirAll(pjt.ProjectPath, os.ModePerm)
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// }
	// if fi != nil && !fi.IsDir() {
	// 	panic(err)
	// }

	// topics := GetAllTopics(pjt.ProjectPath)
	// pjt.Topics = make(map[string]*FSTopic)
	// pjt.TopicSubMap = make(map[string][]string)
	// pjt.Subscriptions = make(map[string]*pubsub.Subscription)
	// for _, topic := range topics {
	// 	tn, err := GetTopicName(topic.Name)
	// 	if err != nil {
	// 		logger.Warn("Problems getting topicName:{}, {}", topic.Name, err.Error())
	// 		continue
	// 	}
	// 	pjt.Topics[tn] = topic
	// 	pjt.TopicSubMap[tn] = make([]string, 0)
	// }
	// subs := GetAllSubs(pjt.ProjectPath)
	// for _, sub := range subs {
	// 	tn, err := GetTopicName(sub.Topic)
	// 	if err != nil {
	// 		logger.Warn("Problems getting topicName:{}, {}", sub.Topic, err.Error())
	// 		continue
	// 	}
	// 	subName, err := GetSubscriptionName(sub.Name)
	// 	if err != nil {
	// 		logger.Warn("Problems getting subName:{}, {}", sub.Name, err.Error())
	// 		continue
	// 	}
	// 	if _, ok := pjt.Topics[tn]; ok {
	// 		pjt.Subscriptions[subName] = sub
	// 		pjt.TopicSubMap[tn] = append(pjt.TopicSubMap[tn], subName)
	// 	} else {
	// 		logger.Warn("Subject:{} missing topic:{}", subName, tn)
	// 	}
	// }
}

func (pjt *FSProject) GetTopic(topicName string) *FSTopic {
	pjt.projectLock.Lock()
	defer pjt.projectLock.Unlock()
	return pjt.Topics[topicName]
}

func (pjt *FSProject) AddTopic(topic *pubsub.Topic) error {
	tn, err := GetTopicName(topic.Name)
	if err != nil {
		return err
	}
	pjt.projectLock.Lock()
	defer pjt.projectLock.Unlock()
	if _, ok := pjt.Topics[tn]; ok {
		return status.Error(codes.AlreadyExists, "Topic Already Exists")
	}
	fsTopic, err := CreateFSTopic(pjt, topic)
	if err != nil {
		return err
	}
	pjt.Topics[tn] = fsTopic
	return nil
}

func (pjt *FSProject) RemoveTopic(topicName string) {
	pjt.projectLock.Lock()
	defer pjt.projectLock.Unlock()
	delete(pjt.Topics, topicName)
}

func (pjt *FSProject) AddSub(sub *pubsub.Subscription) error {
	topicName, err := GetTopicName(sub.Topic)
	if err != nil {
		return nil
	}
	fsTopic := pjt.GetTopic(topicName)
	if fsTopic == nil {
		return status.Error(codes.NotFound, "Topic does not Exist")
	}
	subName, err := GetSubscriptionName(sub.Name)
	if err != nil {
		return nil
	}
	fsSub := pjt.GetSubscription(subName)
	if fsSub == nil {
		return status.Error(codes.AlreadyExists, "Subscription Already Exits")
	}

	return fsTopic.AddSub(sub)
}

func (pjt *FSProject) GetSubscription(subName string) *FSSubscriptions {
	pjt.projectLock.Lock()
	defer pjt.projectLock.Unlock()
	return pjt.Subscriptions[subName]
}

func (pjt *FSProject) DeleteSubscription(subName string) error {
	fsSub := pjt.GetSubscription(subName)
	if fsSub == nil {
		return status.Error(codes.NotFound, "No such Subject Exists")
	}
	pjt.projectLock.Lock()
	defer pjt.projectLock.Unlock()
	fsSub.Delete()
	delete(pjt.Subscriptions, subName)
	return nil
}

func (pjt *FSProject) GetAllTopics() []*pubsub.Topic {
	pjt.projectLock.Lock()
	defer pjt.projectLock.Unlock()
	topics := make([]*pubsub.Topic, 0)
	for _, topic := range pjt.Topics {
		topics = append(topics, topic.pubsubTopic)
	}
	return topics
}

func (pjt *FSProject) GetAllSubscription() []*pubsub.Subscription {
	pjt.projectLock.Lock()
	defer pjt.projectLock.Unlock()
	subs := make([]*pubsub.Subscription, 0)
	for _, sub := range pjt.Subscriptions {
		subs = append(subs, sub.pubsubSubscription)
	}
	return subs
}

func (pjt *FSProject) GetSubsForTopic(topicName string) []*pubsub.Subscription {
	fsTopic := pjt.GetTopic(topicName)
	subs := make([]*pubsub.Subscription, 0)
	if fsTopic == nil {
		return subs
	}
	fsSubs := fsTopic.GetAllSubscriptions()

	for _, fsSub := range fsSubs {
		subs = append(subs, fsSub.pubsubSubscription)
	}

	return subs
}

func (pjt *FSProject) PublishMessage(topicName string, msg *pubsub.PubsubMessage) error {
	fsTopic := pjt.GetTopic(topicName)
	if fsTopic == nil {
		return status.Error(codes.NotFound, "No Such Topic")
	}
	return fsTopic.Publish(msg)
	// pjt.projectLock.Lock()
	// defer pjt.projectLock.Unlock()
	// tn, err := GetTopicName(topic.Name)
	// if err != nil {
	// 	return err
	// }

	// msg_data, err := proto.Marshal(msg)
	// if err != nil {
	// 	logger.Warn("Problem Marshaling protobuf:{}", err.Error())
	// 	return err
	// }
	// if subs, ok := pjt.TopicSubMap[tn]; ok {
	// 	for _, sub := range subs {
	// 		sub_path := path.Join(pjt.ProjectPath, tn, sub)
	// 		msg_path := path.Join(sub_path, msg.MessageId)
	// 		err := ioutil.WriteFile(msg_path, msg_data, os.ModePerm)
	// 		if err != nil {
	// 			logger.Warn("Problem problem writing message:{}:{}", msg.MessageId, err)
	// 		}
	// 	}
	// 	return nil
	// }
	// return nil
}
