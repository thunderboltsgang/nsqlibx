package nsqlibx

import (
	nsq "github.com/nsqio/go-nsq"
	"log"
	"sync"
	"time"
)

type (
	ConsumeHandler struct {
		QChannel chan []byte
		Mutex    sync.Mutex
	}
)

var (
	QServer   []string = make([]string, 0, 0)
	SecretTxt string   = ""
	Qmutex    sync.Mutex
)

func (q *ConsumeHandler) HandleMessage(m *nsq.Message) error {
	q.Mutex.Lock()
	defer q.Mutex.Unlock()

	if len(m.Body) == 0 {
		return nil
	}

	q.QChannel <- m.Body

	return nil
}

func AddQServer(serv string) {
	Qmutex.Lock()
	defer Qmutex.Unlock()

	QServer = append(QServer, serv)
}

func GetChannel(topic string, channel string) chan []byte {
	log.Printf("%s\n", "start to create channel")
	handler := new(ConsumeHandler)
	handler.QChannel = make(chan []byte)

	for serv := range QServer {
		log.Printf("%s\n", "starting to create consumer")
		go func(server string) {
			log.Printf("connecting to %s\n", server)
			config := nsq.NewConfig()
			consumer, err := nsq.NewConsumer(topic, channel, config)
			if err != nil {
				log.Fatal(err)
			}

			consumer.AddHandler(handler)

			for {
				err := consumer.ConnectToNSQD(server)
				if err != nil {
					log.Printf("%v\n", err)
					time.Sleep(time.Second)
					continue
				}
				break
			}

		}(QServer[serv])
	}

	return handler.QChannel
}
