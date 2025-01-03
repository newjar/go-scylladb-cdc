package mqtt

import (
	"context"
	"encoding/json"
	"go-iot-cdc/model"
	"log/slog"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type mqttService struct {
	ctx    context.Context
	wg     *sync.WaitGroup
	logger *slog.Logger

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
	logger *slog.Logger,
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

	go result.PublishMessage(&model.Battery{
		ID:        "1",
		EntryTime: time.Now(),
	})

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
			m.logger.Error("error while unmarshal message", "error", err)
		}

		m.msgChan <- data

		m.logger.Info("Received message", "data", data)
	}
}

func (m *mqttService) PublishMessage(message *model.Battery) {
	for {
		msgByte, _ := json.Marshal(message)
		if ok := m.mqttClient.Publish(m.topic, 0, false, msgByte).Wait(); !ok {
			m.logger.Error("caught erorr while publish message")
		}
		time.Sleep(1 * time.Second)
	}
}

func (m *mqttService) Stop() {
	m.mqttToken.Wait()
	m.mqttClient.Disconnect(10)
	m.wg.Wait()
}
