package nsqlibx

import (
	"errors"
	nsq "github.com/nsqio/go-nsq"
	"log"
	"math/rand"
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
	QServer      []string = make([]string, 0, 0)
	SecretTxt    string   = ""
	Qmutex       sync.RWMutex
	randomx      *rand.Rand    = rand.New(rand.NewSource(time.Now().UnixNano()))
	PoolInterval time.Duration = 10 * time.Second
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

	Qmutex.RLock()
	defer Qmutex.RUnlock()

	for serv := range QServer {
		log.Printf("%s\n", "starting to create consumer")
		go func(server string) {
			log.Printf("connecting to %s\n", server)
			config := nsq.NewConfig()
			config.Set("lookupd_poll_interval", PoolInterval)
			consumer, err := nsq.NewConsumer(topic, channel, config)
			if err != nil {
				log.Fatal(err)
			}

			consumer.AddHandler(handler)

			for {
				err := consumer.ConnectToNSQD(server)
				if err != nil {
					log.Printf("%v\n", err)
					time.Sleep(PoolInterval)
					continue
				}
				break
			}

		}(QServer[serv])
	}

	return handler.QChannel
}

func Publish(topic string, mess []byte) error {
	Qmutex.RLock()
	defer Qmutex.RUnlock()

	lenQServer := len(QServer)
	switch lenQServer {
	case 0:
		return errors.New("No Server")
	default:
		config := nsq.NewConfig()
		count := 0
		server := randomx.Intn(lenQServer)
		for {
			servID := server + count
			if servID >= lenQServer {
				servID -= lenQServer
			}
			producer, err := nsq.NewProducer(QServer[servID], config)
			if err != nil {
				count += 1
				if count >= lenQServer {
					return errors.New("No NSQ to Publish")
				}
				continue
			}
			err = producer.Publish(topic, mess)
			if err != nil {
				count += 1
				if count >= lenQServer {
					return errors.New("No NSQ to Publish")
				}

				continue
			}
			producer.Stop()
			break
		}
	}

	return nil
}
