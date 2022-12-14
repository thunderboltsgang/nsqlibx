package nsqlibx

import (
	"errors"
	"log"
	"math/rand"
	"sync"
	"time"

	nsq "github.com/nsqio/go-nsq"
)

type (
	ConsumeHandler struct {
		QChannel chan []byte
		Mutex    sync.Mutex
	}

	nsqlogger struct{}
)

var (
	QServer      []string = make([]string, 0, 0)
	SecretTxt    string   = ""
	Qmutex       sync.RWMutex
	randomx      *rand.Rand    = rand.New(rand.NewSource(time.Now().UnixNano()))
	PollInterval time.Duration = time.Second
	logging      bool          = false
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

func (l nsqlogger) Output(calldepth int, s string) error {
	switch logging {
	case true:
		log.Printf("%s\n", s)
	}
	return nil

}

func AddQServer(serv string) {
	Qmutex.Lock()
	defer Qmutex.Unlock()

	QServer = append(QServer, serv)
}

func SetPollInterval(intv time.Duration) {
	PollInterval = intv
}

func Logging(t bool) {
	logging = t
}

func GetChannel(topic string, channel string) chan []byte {
	handler := new(ConsumeHandler)
	handler.QChannel = make(chan []byte)

	Qmutex.RLock()
	defer Qmutex.RUnlock()

	for serv := range QServer {
		go func(server string) {
			config := nsq.NewConfig()
			config.Set("lookupd_poll_interval", PollInterval)
			consumer, err := nsq.NewConsumer(topic, channel, config)
			if err != nil {
				log.Fatal(err)
			}

			consumer.SetLogger(&nsqlogger{}, nsq.LogLevelInfo)
			consumer.AddHandler(handler)

			for {
				err := consumer.ConnectToNSQD(server)
				if err != nil {
					time.Sleep(PollInterval)
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
			producer.SetLogger(&nsqlogger{}, nsq.LogLevelInfo)
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
