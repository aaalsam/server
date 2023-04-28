package main

import (
	"os"
	"os/signal"
	"pkg/apiserver"
	"pkg/storage"
	"pkg/streaming"
)

func main() {

	st := storage.NewStorage()
	csh := storage.NewCache(st)
	sh := streaming.NewStreamingHandler(st)

	api := apiserver.NewApi(csh)

	signalChan := make(chan os.Signal, 1)
	cleanupDone := make(chan bool)
	signal.Notify(signalChan, os.Interrupt)

	go func() {
		for range signalChan {
			sh.Finish()
			api.Finish()

			cleanupDone <- true
		}
	}()
	<-cleanupDone

}
