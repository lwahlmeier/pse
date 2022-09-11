package publisher

import (
	"context"
	"errors"
	"io/ioutil"
	"os"
	"path"

	"githb.com/lwahlmeier/go-pubsub-emulator/internal/utils"
	"github.com/google/uuid"
	"github.com/lwahlmeier/lcwlog"
	"google.golang.org/genproto/googleapis/pubsub/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

var logger = lcwlog.GetLoggerWithPrefix("publisher")

type PublisherServer struct {
	basePath string
}

func (ps *PublisherServer) getTopicFullPath(topic *pubsub.Topic) (string, error) {
	project, err := utils.GetProjectName(topic)
	if err != nil {
		return "", err
	}
	topic_name, err := utils.GetTopicName(topic)
	if err != nil {
		return "", err
	}
	return path.Join(ps.basePath, path.Clean(path.Join("/", project, topic_name))), nil
}

func (ps *PublisherServer) getAllSubs(topic *pubsub.Topic) ([]string, error) {
	topic_path, err := ps.getTopicFullPath(topic)
	if err != nil {
		return nil, err
	}
	subs_path := path.Join(topic_path, "SUBS")

	files, err := ioutil.ReadDir(subs_path)
	if err != nil {
		return nil, err
	}
	subs := make([]string, 0)
	for _, fi := range files {
		if fi.IsDir() {
			subs = append(subs, path.Join(subs_path, fi.Name()))
		}
	}
	return subs, nil
}

func (ps *PublisherServer) getAllTopics(project string) []*pubsub.Topic {
	project_path := path.Join(ps.basePath, path.Clean(path.Join("/", project)))
	topics_path := path.Join(ps.basePath, project_path)
	topics := make([]*pubsub.Topic, 0)
	files, err := ioutil.ReadDir(topics_path)
	if err != nil {
		return topics
	}
	for _, fi := range files {
		if fi.IsDir() {
			topics = append(topics, &pubsub.Topic{Name: path.Join(project_path, fi.Name())})
		}
	}
	return topics
}

func (ps *PublisherServer) CreateTopic(ctx context.Context, topic *pubsub.Topic) (*pubsub.Topic, error) {
	topic_path, err := ps.getTopicFullPath(topic)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	stat, err := os.Stat(topic_path)
	if errors.Is(err, os.ErrNotExist) {
		logger.Info("Created topic:{}", topic.Name)
		os.MkdirAll(topic_path, os.ModePerm)
		os.MkdirAll(path.Join(topic_path, "SUBS"), os.ModePerm)
		os.MkdirAll(path.Join(topic_path, "CONFIG"), os.ModePerm)
		return topic, nil
	}
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if stat.IsDir() {
		return nil, status.Error(codes.AlreadyExists, "Topic Already Exists")
	}
	return nil, status.Error(codes.Unknown, "Topic Already Exists")
}

func (ps *PublisherServer) UpdateTopic(ctx context.Context, utr *pubsub.UpdateTopicRequest) (*pubsub.Topic, error) {
	topic_path, err := ps.getTopicFullPath(utr.Topic)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	stat, err := os.Stat(topic_path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, status.Errorf(codes.Internal, "No Such Topic")
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	if stat.IsDir() {
		return utr.Topic, nil
	}
	return nil, status.Errorf(codes.Unknown, "Unknown Error")
}

func (ps *PublisherServer) Publish(ctx context.Context, pr *pubsub.PublishRequest) (*pubsub.PublishResponse, error) {
	topic := &pubsub.Topic{Name: pr.Topic}
	subs, err := ps.getAllSubs(topic)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	mids := make([]string, 0)
	for _, msg := range pr.Messages {
		msg.MessageId = uuid.New().String()

		msg_data, err := proto.Marshal(msg)
		if err != nil {
			logger.Warn("Problem Marshaling protobuf:{}", err.Error())
			return nil, err
		}
		mids = append(mids, msg.MessageId)
		for _, sub_path := range subs {
			msg_path := path.Join(sub_path, msg.MessageId)
			err := ioutil.WriteFile(msg_path, msg_data, os.ModePerm)
			if err != nil {
				logger.Warn("Problem problem writing message:{}:{}", msg.MessageId, err)
				return nil, err
			}
		}
	}
	return &pubsub.PublishResponse{MessageIds: mids}, nil
}
func (ps *PublisherServer) GetTopic(ctx context.Context, rTopic *pubsub.GetTopicRequest) (*pubsub.Topic, error) {
	topic := &pubsub.Topic{Name: rTopic.Topic}
	topic_path, err := ps.getTopicFullPath(topic)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	stat, err := os.Stat(topic_path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, status.Errorf(codes.NotFound, "Topic Not found")
	}
	if stat.IsDir() {
		return topic, nil
	}
	logger.Warn("Got request for topic that exists but is not a directory: {}", topic_path)
	return nil, status.Errorf(codes.Unknown, "Problem finding topic")
}
func (ps *PublisherServer) ListTopics(ctx context.Context, ltr *pubsub.ListTopicsRequest) (*pubsub.ListTopicsResponse, error) {
	return &pubsub.ListTopicsResponse{Topics: ps.getAllTopics(ltr.Project)}, nil
}
func (ps *PublisherServer) ListTopicSubscriptions(ctx context.Context, ltsr *pubsub.ListTopicSubscriptionsRequest) (*pubsub.ListTopicSubscriptionsResponse, error) {
	topic := &pubsub.Topic{Name: ltsr.Topic}
	sub_paths, err := ps.getAllSubs(topic)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	project_name, err := utils.GetProjectName(topic)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	subs := make([]string, 0)
	for _, sp := range sub_paths {
		sub_name := path.Base(sp)
		subs = append(subs, utils.MakeSubscriptionPath(project_name, sub_name).Name)
	}
	return &pubsub.ListTopicSubscriptionsResponse{Subscriptions: subs}, nil
}

func (pc *PublisherServer) ListTopicSnapshots(context.Context, *pubsub.ListTopicSnapshotsRequest) (*pubsub.ListTopicSnapshotsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ListTopicSnapshots not implemented")
}
func (pc *PublisherServer) DeleteTopic(context.Context, *pubsub.DeleteTopicRequest) (*emptypb.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method DeleteTopic not implemented")
}
func (pc *PublisherServer) DetachSubscription(context.Context, *pubsub.DetachSubscriptionRequest) (*pubsub.DetachSubscriptionResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method DetachSubscription not implemented")
}
