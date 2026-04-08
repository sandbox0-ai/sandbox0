package main

import (
	"flag"
	"log"
	"os"
	"syscall"

	"github.com/fsnotify/fsnotify"
	ctldfuseplugin "github.com/sandbox0-ai/sandbox0/internal/ctld/fuseplugin"
	pluginapi "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"
)

var (
	mountsAllowed = 5
)

func main() {
	flag.IntVar(&mountsAllowed, "mounts-allowed", 100, "maximum times the fuse device can be mounted")
	flag.Parse()

	log.Println("Starting")
	defer func() { log.Println("Stopped:") }()

	log.Println("Starting FS watcher.")
	watcher, err := ctldfuseplugin.NewFSWatcher(pluginapi.DevicePluginPath)
	if err != nil {
		log.Println("Failed to created FS watcher.")
		os.Exit(1)
	}
	defer watcher.Close()

	log.Println("Starting OS watcher.")
	sigs := ctldfuseplugin.NewOSWatcher(syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	restart := true
	var devicePlugin *ctldfuseplugin.DevicePlugin

L:
	for {
		if restart {
			if devicePlugin != nil {
				devicePlugin.Stop()
			}

			devicePlugin = ctldfuseplugin.NewDevicePlugin(mountsAllowed)
			if err := devicePlugin.Serve(); err != nil {
				log.Println("Could not contact Kubelet, retrying. Did you enable the device plugin feature gate?")
			} else {
				restart = false
			}
		}

		select {
		case event := <-watcher.Events:
			if event.Name == pluginapi.KubeletSocket && event.Op&fsnotify.Create == fsnotify.Create {
				log.Printf("inotify: %s created, restarting.", pluginapi.KubeletSocket)
				restart = true
			}

		case err := <-watcher.Errors:
			log.Printf("inotify: %s", err)

		case s := <-sigs:
			switch s {
			case syscall.SIGHUP:
				log.Println("Received SIGHUP, restarting.")
				restart = true
			default:
				log.Printf("Received signal \"%v\", shutting down.", s)
				devicePlugin.Stop()
				break L
			}
		}
	}
}
