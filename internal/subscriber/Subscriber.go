package subscriber

import (
	"context"

	"google.golang.org/genproto/googleapis/pubsub/v1"
	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Subscriber_StreamingPullServer struct {
	grpc.ServerStream
}

func (ssps *Subscriber_StreamingPullServer) Send(*pubsub.StreamingPullResponse) error {
	return nil
}

func (ssps *Subscriber_StreamingPullServer) Recv() (*pubsub.StreamingPullRequest, error) {
	return nil, nil
}

type SubscriberServer struct {
}

func (ss *SubscriberServer) CreateSubscription(context.Context, *pubsub.Subscription) (*pubsub.Subscription, error) {
	return nil, status.Errorf(codes.Unimplemented, "method CreateSubscription not implemented")
}
func (ss *SubscriberServer) GetSubscription(context.Context, *pubsub.GetSubscriptionRequest) (*pubsub.Subscription, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetSubscription not implemented")
}
func (ss *SubscriberServer) UpdateSubscription(context.Context, *pubsub.UpdateSubscriptionRequest) (*pubsub.Subscription, error) {
	return nil, status.Errorf(codes.Unimplemented, "method UpdateSubscription not implemented")
}
func (ss *SubscriberServer) ListSubscriptions(context.Context, *pubsub.ListSubscriptionsRequest) (*pubsub.ListSubscriptionsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ListSubscriptions not implemented")
}
func (ss *SubscriberServer) DeleteSubscription(context.Context, *pubsub.DeleteSubscriptionRequest) (*emptypb.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method DeleteSubscription not implemented")
}
func (ss *SubscriberServer) ModifyAckDeadline(context.Context, *pubsub.ModifyAckDeadlineRequest) (*emptypb.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ModifyAckDeadline not implemented")
}
func (ss *SubscriberServer) Acknowledge(context.Context, *pubsub.AcknowledgeRequest) (*emptypb.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Acknowledge not implemented")
}
func (ss *SubscriberServer) Pull(context.Context, *pubsub.PullRequest) (*pubsub.PullResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Pull not implemented")
}
func (ss *SubscriberServer) StreamingPull(pubsub.Subscriber_StreamingPullServer) error {
	return status.Errorf(codes.Unimplemented, "method StreamingPull not implemented")
}
func (ss *SubscriberServer) ModifyPushConfig(context.Context, *pubsub.ModifyPushConfigRequest) (*emptypb.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ModifyPushConfig not implemented")
}
func (ss *SubscriberServer) GetSnapshot(context.Context, *pubsub.GetSnapshotRequest) (*pubsub.Snapshot, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetSnapshot not implemented")
}
func (ss *SubscriberServer) ListSnapshots(context.Context, *pubsub.ListSnapshotsRequest) (*pubsub.ListSnapshotsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ListSnapshots not implemented")
}
func (ss *SubscriberServer) CreateSnapshot(context.Context, *pubsub.CreateSnapshotRequest) (*pubsub.Snapshot, error) {
	return nil, status.Errorf(codes.Unimplemented, "method CreateSnapshot not implemented")
}
func (ss *SubscriberServer) UpdateSnapshot(context.Context, *pubsub.UpdateSnapshotRequest) (*pubsub.Snapshot, error) {
	return nil, status.Errorf(codes.Unimplemented, "method UpdateSnapshot not implemented")
}
func (ss *SubscriberServer) DeleteSnapshot(context.Context, *pubsub.DeleteSnapshotRequest) (*emptypb.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method DeleteSnapshot not implemented")
}
func (ss *SubscriberServer) Seek(context.Context, *pubsub.SeekRequest) (*pubsub.SeekResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Seek not implemented")
}
