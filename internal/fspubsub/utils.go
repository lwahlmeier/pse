package fspubsub

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"

	"google.golang.org/genproto/googleapis/pubsub/v1"
	"google.golang.org/protobuf/proto"
)

func GetProjectAndTopic(topicName string) (string, string, error) {
	pjName, err := GetProjectName(topicName)
	if err != nil {
		return "", "", err
	}
	tn, err := GetTopicName(topicName)
	if err != nil {
		return "", "", err
	}
	return pjName, tn, nil
}

func GetTopicName(topic string) (string, error) {
	items := strings.Split(topic, "/")
	if len(items) >= 4 && items[0] == "projects" && items[2] == "topics" {
		return items[3], nil
	}
	return "", fmt.Errorf("bad topic name: %s", topic)
}

func GetProjectName(topic string) (string, error) {
	items := strings.Split(topic, "/")
	if len(items) >= 2 && items[0] == "projects" {
		return items[1], nil
	}
	return "", fmt.Errorf("bad project name: %s", topic)
}

func GetSubscriptionName(subPath string) (string, error) {
	items := strings.Split(subPath, "/")
	if len(items) >= 2 && items[0] == "projects" && items[2] == "subscriptions" {
		return items[3], nil
	}
	return "", fmt.Errorf("bad subscription name: %s", subPath)
}

func MakeTopic(project, topic string) *pubsub.Topic {
	return &pubsub.Topic{Name: fmt.Sprintf("projects/%s/topics/%s", project, topic)}
}

func MakeSubscription(project, subscription string) *pubsub.Subscription {
	return &pubsub.Subscription{Name: fmt.Sprintf("projects/%s/subscriptions/%s", project, subscription)}
}

func GetAllTopics(projectPath string) []*pubsub.Topic {
	topics := make([]*pubsub.Topic, 0)
	lfi, err := ioutil.ReadDir(projectPath)
	if err != nil {
		logger.Warn("Problem reading topic:{}", err.Error())
		return topics
	}
	for _, fi := range lfi {
		if strings.HasSuffix(fi.Name(), ".topic.proto") {
			fp, err := os.Open(path.Join(projectPath, fi.Name()))
			if err != nil {
				logger.Warn("Problem opening topic: {}, {}", fi.Name(), err.Error())
				continue
			}
			data, err := ioutil.ReadAll(fp)
			if err != nil {
				logger.Warn("Problem reading topic: {}, {}", fi.Name(), err.Error())
				continue
			}
			topic := &pubsub.Topic{}
			err = proto.Unmarshal(data, topic)
			if err != nil {
				logger.Warn("Problem parsing topic: {}, {}", fi.Name(), err.Error())
				continue
			}
			topics = append(topics, topic)
		}
	}
	return topics
}

func WriteTopic(projectPath string, topic *pubsub.Topic) error {
	tn, err := GetTopicName(topic.Name)
	if err != nil {
		return err
	}
	tp := path.Join(projectPath, fmt.Sprintf("%s.topic.proto", tn))
	data, err := proto.Marshal(topic)
	if err != nil {
		return err
	}
	err = ioutil.WriteFile(tp, data, os.ModePerm)
	if err != nil {
		return err
	}
	return nil
}

func WriteSub(projectPath string, sub *pubsub.Subscription) error {
	subName, err := GetSubscriptionName(sub.Name)
	if err != nil {
		return err
	}
	tp := path.Join(projectPath, fmt.Sprintf("%s.sub.proto", subName))
	data, err := proto.Marshal(sub)
	if err != nil {
		return err
	}
	err = ioutil.WriteFile(tp, data, os.ModePerm)
	if err != nil {
		return err
	}
	return nil
}

func GetAllSubs(projectPath string) []*pubsub.Subscription {
	subs := make([]*pubsub.Subscription, 0)
	lfi, err := ioutil.ReadDir(projectPath)
	if err != nil {
		logger.Warn("Problem reading subs:{}", err.Error())
		return subs
	}
	for _, fi := range lfi {
		if strings.HasSuffix(fi.Name(), ".sub.proto") {
			fp, err := os.Open(path.Join(projectPath, fi.Name()))
			if err != nil {
				logger.Warn("Problem opening sub: {}, {}", fi.Name(), err.Error())
				continue
			}
			data, err := ioutil.ReadAll(fp)
			if err != nil {
				logger.Warn("Problem reading sub: {}, {}", fi.Name(), err.Error())
				continue
			}
			sub := &pubsub.Subscription{}
			err = proto.Unmarshal(data, sub)
			if err != nil {
				logger.Warn("Problem parsing sub: {}, {}", fi.Name(), err.Error())
				continue
			}
			subs = append(subs, sub)
		}
	}
	return subs
}

func GetAllSubsForTopic(basePath string, topic *pubsub.Topic) []*pubsub.Subscription {
	tsubs := make([]*pubsub.Subscription, 0)
	for _, sub := range GetAllSubs(basePath) {
		if sub.Topic == topic.Name {
			tsubs = append(tsubs, sub)
		}
	}
	return tsubs
}

func ReadMessage(path string) (*pubsub.PubsubMessage, error) {
	fp, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	data, err := ioutil.ReadAll(fp)
	if err != nil {
		return nil, err
	}
	msg := &pubsub.PubsubMessage{}
	err = proto.Unmarshal(data, msg)
	if err != nil {
		return nil, err
	}
	return msg, nil
}
