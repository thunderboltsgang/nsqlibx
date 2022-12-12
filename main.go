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
	}
)

var (
	QServer []string = make([]string, 0, 0)
	Qmutex  sync.Mutex
)

func (q *ConsumeHandler) HandleMessage(m *nsq.Message) error {

	if len(m.Body) == 0 {
		return nil
	}

	q.QChannel <- m.Body

	return nil
}

func AddQServer(serv string) {
	go func() {
		Qmutex.Lock()
		defer Qmutex.Unlock()

		QServer = append(QServer, serv)
	}()
}

func GetChannel(topic string, channel string) chan []byte {
	handler := new(ConsumeHandler)
	handler.QChannel = make(chan []byte)
	for serv := range QServer {
		go func(server string) {
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
