package main

import (
	"log"

	"github.com/mkobetic/go-dogstatsd"
)

func main() {
	c, err := dogstatsd.New("127.0.0.1:8125")
	if err != nil {
		log.Fatal(err)
	}
	err = c.Event("test event", "description", []string{"testing:yes"})
	if err != nil {
		log.Fatal(err)
	}
	log.Print("Sent")
}
