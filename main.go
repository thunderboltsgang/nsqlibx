package nsqlibx

import (
	b64 "encoding/base64"
	"errors"
	"log"
	"math/rand"
	"strings"
	"sync"
	"time"

	mc "github.com/authapon/mcryptzero"
	nsq "github.com/nsqio/go-nsq"
)

type (
	ConsumeHandler struct {
		QChannel    chan string
		ExitChannel chan bool
		Consumers   []*nsq.Consumer
		Running     bool
		Mutex       sync.RWMutex
	}

	nsqlogger struct{}
)

var (
	QServer         []string = make([]string, 0, 0)
	SecretText      string   = ""
	Qmutex          sync.RWMutex
	randomx         *rand.Rand    = rand.New(rand.NewSource(time.Now().UnixNano()))
	PollInterval    time.Duration = time.Second
	logging         bool          = false
	TimeoutDuration time.Duration = time.Minute
	RandomTopicLen  int           = 20
	SaltSize        int           = 8
)

func (q *ConsumeHandler) HandleMessage(m *nsq.Message) error {
	q.Mutex.Lock()
	defer q.Mutex.Unlock()

	if len(m.Body) == 0 {
		return nil
	}

	data := strings.SplitN(Decrypt(string(m.Body)), ":", 3)

	switch len(data) {
	case 3:
		if data[1] == SecretText {
			q.QChannel <- data[0] + ":" + data[2]
		}
	}
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

func SetSecretText(secret string) {
	SecretText = secret
}

func SetSaltSize(salt int) {
	SaltSize = salt
}

func Logging(t bool) {
	logging = t
}

func GetChannel(topic string, channel string) chan string {
	ch, _ := GetChannelWithExit(topic, channel)
	return ch
}

func GetChannelWithExit(topic string, channel string) (chan string, chan bool) {
	handler := new(ConsumeHandler)
	handler.QChannel = make(chan string)
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

func Publish(topic string, mess string) error {
	return Publishx(topic, "", mess)
}

func Publishx(topic string, reply string, mess string) error {
	Qmutex.RLock()
	defer Qmutex.RUnlock()

	lenQServer := len(QServer)
	switch lenQServer {
	case 0:
		return errors.New("No Server")
	default:
		config := nsq.NewConfig()
		server := getRandomServer()
		success := false
		for _, v := range server {
			producer, err := nsq.NewProducer(QServer[server[v]], config)
			if err != nil {
				continue
			}
			producer.SetLogger(&nsqlogger{}, nsq.LogLevelInfo)
			datax := Encrypt(reply + ":" + SecretText + ":" + mess)
			err = producer.Publish(topic, []byte(datax))
			if err != nil {
				continue
			}
			producer.Stop()
			success = true
			break
		}
		if !success {
			return errors.New("No NSQ to publish ...")
		}
	}

	return nil
}

func getRandomServer() []int {
	lenQserver := len(QServer)
	switch lenQserver {
	case 0:
		return make([]int, 0)
	case 1:
		output := make([]int, 1)
		output[0] = 0
		return output
	default:
		output := make([]int, 0)
		serverMarks := make([]int, lenQserver)

		for i := lenQserver; i > 0; i-- {
			servID := randomx.Intn(i)
			count := 0
			for k := 0; k < lenQserver; k++ {
				switch serverMarks[k] {
				case 0:
					if servID == count {
						output = append(output, k)
						serverMarks[k] = 1
						continue
					}
					count += 1
				case 1:
					continue
				}
			}
		}
		return output
	}
}

func PublishReply(topic string, mess string) string {
	return PublishReplyWithTimeout(topic, mess, TimeoutDuration)
}

func PublishReplyWithTimeout(topic string, mess string, timeout time.Duration) string {
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

	err := Publishx(topic, replyTopic, mess)
	if err != nil {
		return ""
	}
	getreply := <-reply
	switch getreply.(type) {
	case string:
		return getreply.(string)
	}
	return ""
}

func GetRandomTopic(replylen int) string {
	alphabet := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	output := ""
	for i := 0; i < replylen; i++ {
		output += string(alphabet[randomx.Intn(len(alphabet))])
	}
	return output
}

func Encrypt(data string) string {
	salt := GetRandomTopic(SaltSize)
	datax := salt + ":" + b64.StdEncoding.EncodeToString(mc.Encrypt([]byte(data), []byte(SecretText+salt)))
	return datax
}

func Decrypt(datax string) string {
	dataxSplit := strings.SplitN(datax, ":", 2)

	if len(dataxSplit) != 2 {
		return ""
	}

	if len(dataxSplit[0]) != SaltSize {
		return ""
	}
	dataDC, err := b64.StdEncoding.DecodeString(dataxSplit[1])
	if err != nil {
		return ""
	}
	return string(mc.Decrypt([]byte(dataDC), []byte(SecretText+dataxSplit[0])))
}
