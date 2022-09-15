package main

import (
	"log"
	"net"

	"githb.com/lwahlmeier/go-pubsub-emulator/internal/fspubsub"

	pubsub "google.golang.org/genproto/googleapis/pubsub/v1"
	"google.golang.org/grpc"
)

func main() {
	lis, err := net.Listen("tcp", "localhost:8681")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)
	mypub := fspubsub.NewFSPubSubServer("/tmp/pubsub")
	pubsub.RegisterPublisherServer(grpcServer, mypub)
	pubsub.RegisterSubscriberServer(grpcServer, mypub)
	grpcServer.Serve(lis)
}
