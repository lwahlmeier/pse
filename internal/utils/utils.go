package utils

import (
	"fmt"
	"strings"

	"google.golang.org/genproto/googleapis/pubsub/v1"
)

func GetTopicName(topic *pubsub.Topic) (string, error) {
	items := strings.Split(topic.Name, "/")
	if len(items) >= 4 && items[0] == "projects" && items[2] == "topics" {
		return items[3], nil
	}
	return "", fmt.Errorf("bad topic name: %s", topic.Name)
}

func GetProjectName(topic *pubsub.Topic) (string, error) {
	items := strings.Split(topic.Name, "/")
	if len(items) >= 2 && items[0] == "projects" {
		return items[1], nil
	}
	return "", fmt.Errorf("bad project name: %s", topic.Name)
}

func GetSubscriptionName(topic *pubsub.Subscription) (string, error) {
	items := strings.Split(topic.Name, "/")
	if len(items) >= 2 && items[0] == "projects" && items[2] == "subscriptions" {
		return items[1], nil
	}
	return "", fmt.Errorf("bad subscription name: %s", topic.Name)
}

func MakeTopicPath(project, topic string) *pubsub.Topic {
	return &pubsub.Topic{Name: fmt.Sprintf("projects/%s/topics/%s", project, topic)}
}

func MakeSubscriptionPath(project, subscription string) *pubsub.Subscription {
	return &pubsub.Subscription{Name: fmt.Sprintf("projects/%s/subscriptions/%s", project, subscription)}
}
