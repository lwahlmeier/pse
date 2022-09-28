package main

import (
	"fmt"
	"net"
	"os"
	"strings"
	"sync"

	"githb.com/lwahlmeier/go-pubsub-emulator/internal/base"
	"githb.com/lwahlmeier/go-pubsub-emulator/internal/fspubsub"
	"githb.com/lwahlmeier/go-pubsub-emulator/internal/mempubsub"
	"github.com/lwahlmeier/lcwlog"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	pubsub "google.golang.org/genproto/googleapis/pubsub/v1"
	"google.golang.org/grpc"
)

var version string
var config *viper.Viper

func main() {

	config = viper.New()
	config.SetEnvPrefix("pse")
	config.AutomaticEnv()
	config.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	if version == "" || version == "latest" {
		version = "unknown"
	}

	var cmd = &cobra.Command{
		Use:   "pse",
		Short: "launch pubsub emulator service",
		Long:  "launch pubsub emulator service",
		Run:   parseArgs,
	}

	cmd.PersistentFlags().String("loglevel", "info", "level to show logs at (warn, info, debug, trace)")
	config.BindPFlag("loglevel", cmd.PersistentFlags().Lookup("loglevel"))

	cmd.PersistentFlags().String("listenAddress", "0.0.0.0:8681", "The ip and port to listen on for the pubsub emulator, takes list (192.168.1.1:8681,192.168.4.5:8682), default)")
	config.BindPFlag("listenAddress", cmd.PersistentFlags().Lookup("listenAddress"))

	cmd.PersistentFlags().String("type", "memory", "They type of pubsub emulator to run, valid options are memory or filesystem, defaults to memory")
	config.BindPFlag("type", cmd.PersistentFlags().Lookup("type"))

	cmd.PersistentFlags().String("fspath", "/tmp/pubsub", "The path where we store data when using type filesystem, defaults to /tmp/pubsbu")
	config.BindPFlag("fspath", cmd.PersistentFlags().Lookup("fspath"))

	cmd.Execute()
}

func parseArgs(cmd *cobra.Command, args []string) {
	if config.GetBool("version") {
		fmt.Printf("%s\n", version)
		os.Exit(0)
	}
	var ll lcwlog.Level
	switch strings.ToLower(config.GetString("loglevel")) {
	case "info":
		ll = lcwlog.InfoLevel
	case "warn":
		ll = lcwlog.WarnLevel
	case "debug":
		ll = lcwlog.DebugLevel
	case "trace":
		ll = lcwlog.TraceLevel
	}
	lcwlog.GetLoggerConfig().SetLevel(ll)
	listenAddress := strings.Split(config.GetString("listenAddress"), ",")
	psetype := config.GetString("type")
	fspath := config.GetString("fspath")
	startPSE(listenAddress, psetype, fspath)
}

func startPSE(listenAddresses []string, pseType, fspath string) {
	logger.Info("Starting pubsub emulator, addresses:{}, type:{}", listenAddresses, pseType)
	var err error
	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)
	var backend base.BaseBackend
	if pseType == "memory" {
		backend = mempubsub.NewMemBase()
	} else if pseType == "filesystem" {
		backend, err = fspubsub.StartFSBase("/tmp/pubsub2")
		if err != nil {
			logger.Fatal("error starting FileSystem backend {}", err)
		}
	} else {
		logger.Fatal("Bad pubsub emulator type:{}", pseType)
	}
	pse := &PubSubEmulator{
		baseBackend: backend,
	}

	pubsub.RegisterPublisherServer(grpcServer, pse)
	pubsub.RegisterSubscriberServer(grpcServer, pse)
	waiter := sync.WaitGroup{}
	for _, address := range listenAddresses {
		waiter.Add(1)
		lis, err := net.Listen("tcp", address)
		if err != nil {
			logger.Fatal("failed to listen:{}: {}", address, err)
		}
		go func() {
			defer waiter.Done()
			grpcServer.Serve(lis)
		}()
	}
	waiter.Wait()
}
