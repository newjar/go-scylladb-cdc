package mqtt

import (
	"context"
	"encoding/json"
	"fmt"
	"go-scylladb-cdc/model"
	"math/rand"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/rs/zerolog"
)

type mqttService struct {
	ctx    context.Context
	wg     *sync.WaitGroup
	logger *zerolog.Logger

	mqttClient mqtt.Client
	mqttToken  mqtt.Token
	topic      string

	msgChan chan<- *model.Battery
}

type MQTTService interface {
	PublishMessage(msg *model.Battery)
	Stop()
}

func NewMQTTService(
	parentCtx context.Context,
	logger *zerolog.Logger,
	address, clientId, user, pass, topic string,
	msgChan chan *model.Battery) (MQTTService, error) {

	result := new(mqttService)

	options := mqtt.NewClientOptions()
	options.AddBroker(address)
	options.SetClientID(clientId)
	options.SetUsername(user)
	options.SetPassword(pass)
	options.SetAutoReconnect(true)

	client := mqtt.NewClient(options)
	var mqttToken mqtt.Token
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		return nil, token.Error()
	} else {
		mqttToken = token
	}

	*result = mqttService{
		ctx:    parentCtx,
		wg:     new(sync.WaitGroup),
		logger: logger,

		mqttClient: client,
		mqttToken:  mqttToken,
		topic:      topic,

		msgChan: msgChan,
	}

	result.start()

	go result.FakeMessage()

	return result, nil
}

func (m *mqttService) start() {
	maxWorkersNum := 5
	m.wg.Add(maxWorkersNum)

	for i := 0; i < maxWorkersNum; i++ {
		go pullMessages(m.mqttClient, m.topic, m.wg, m.onReceive())
	}
}

func pullMessages(
	mqttClient mqtt.Client,
	topic string,
	wg *sync.WaitGroup,
	receiver mqtt.MessageHandler) {
	defer wg.Done()

	mqttClient.Subscribe(topic, 0, receiver).Wait()
}

func (m *mqttService) onReceive() mqtt.MessageHandler {
	return func(client mqtt.Client, msg mqtt.Message) {
		msg.Ack()

		data := new(model.Battery)

		if err := json.Unmarshal(msg.Payload(), data); err != nil {
			m.logger.Error().Err(err).Msg("error while unmarshal message")
			return
		}

		m.msgChan <- data
	}
}

func (m *mqttService) FakeMessage() {
	for {
		message := new(model.Battery)
		message.ID = fmt.Sprintf("id-%d", rand.Intn(10))
		message.EntryTime = time.Now()
		message.Voltage = rand.Float32() * 100
		message.Current = rand.Float32() * 100
		message.Capacity = rand.Float32() * 100
		message.Power = rand.Intn(100)
		message.Temperature = rand.Float32() * 100
		message.SOC = rand.Intn(100)
		message.InternalResistance = rand.Float32() * 100

		m.PublishMessage(message)

		time.Sleep(500 * time.Millisecond)
	}
}

func (m *mqttService) PublishMessage(message *model.Battery) {
	msgByte, _ := json.Marshal(message)
	if ok := m.mqttClient.Publish(m.topic, 0, false, msgByte).Wait(); !ok {
		m.logger.Error().Msg("caught erorr while publish message")
		return
	}

}

func (m *mqttService) Stop() {
	m.mqttToken.Wait()
	m.mqttClient.Disconnect(10)
	m.wg.Wait()
}
