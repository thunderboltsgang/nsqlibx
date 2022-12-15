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
		QChannel    chan []byte
		ExitChannel chan bool
		Consumers   []*nsq.Consumer
		Running     bool
		Mutex       sync.RWMutex
	}

	nsqlogger struct{}
)

var (
	QServer         []string = make([]string, 0, 0)
	SecretTxt       string   = ""
	Qmutex          sync.RWMutex
	randomx         *rand.Rand    = rand.New(rand.NewSource(time.Now().UnixNano()))
	PollInterval    time.Duration = time.Second
	logging         bool          = false
	TimeoutDuration time.Duration = time.Minute
	RandomTopicLen  int           = 20
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

func SetTimeOut(t time.Duration) {
	TimeoutDuration = t
}

func SetPollInterval(intv time.Duration) {
	PollInterval = intv
}

func Logging(t bool) {
	logging = t
}

func GetChannel(topic string, channel string) chan []byte {
	ch, _ := GetChannelWithExit(topic, channel)
	return ch
}

func GetChannelWithExit(topic string, channel string) (chan []byte, chan bool) {
	handler := new(ConsumeHandler)
	handler.QChannel = make(chan []byte)
	handler.ExitChannel = make(chan bool)
	handler.Consumers = make([]*nsq.Consumer, 0)
	handler.Running = true

	go func() {
		<-handler.ExitChannel

		handler.Mutex.Lock()
		handler.Running = false
		close(handler.QChannel)
		close(handler.ExitChannel)
		handler.Mutex.Unlock()

		for cx := range handler.Consumers {
			go func(cxx int) {
				handler.Consumers[cxx].Stop()
			}(cx)
		}
	}()

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
				handler.Mutex.RLock()
				if !handler.Running {
					handler.Mutex.RUnlock()
					return
				}
				handler.Mutex.RUnlock()
				err := consumer.ConnectToNSQD(server)
				if err != nil {
					time.Sleep(PollInterval)
					continue
				}
				break
			}
			handler.Mutex.Lock()
			handler.Consumers = append(handler.Consumers, consumer)
			handler.Mutex.Unlock()
		}(QServer[serv])
	}

	return handler.QChannel, handler.ExitChannel
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

func PublishReply(topic string, mess []byte) []byte {
	return PublishReplyWithTimeout(topic, mess, TimeoutDuration)
}

func PublishReplyWithTimeout(topic string, mess []byte, timeout time.Duration) []byte {
	replyTopic := GetRandomTopic(RandomTopicLen)
	reply := make(chan interface{})
	timeoutCh := make(chan bool)

	go func() {
		ch, exitc := GetChannelWithExit(replyTopic, "reply")

		select {
		case replyData := <-ch:
			reply <- replyData
		case <-timeoutCh:
			reply <- true
		}
		exitc <- true
	}()

	go func() {
		time.Sleep(timeout)
		timeoutCh <- true
	}()

	getreply := <-reply
	switch getreply.(type) {
	case []byte:
		return getreply.([]byte)
	case bool:
	}
	return make([]byte, 0, 0)
}

func GetRandomTopic(replylen int) string {
	alphabet := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	output := ""
	for i := 0; i < len(alphabet); i++ {
		output += string(alphabet[randomx.Intn(len(alphabet))])
	}
	return output
}
