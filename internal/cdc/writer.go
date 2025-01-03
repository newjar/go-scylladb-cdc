package cdc

import (
	"context"
	"go-iot-cdc/model"
)

func (cs *cdcService) Start() {
	cs.wg.Add(10)
	for i := 0; i < 10; i++ {
		go cs.msgProcessor()
	}
}

func (cs *cdcService) msgProcessor() {
	defer cs.wg.Done()

	for msg := range cs.msgChan {
		data := new(model.Battery)
		data.ID = msg.ID
		data.EntryTime = msg.EntryTime

		query := cs.batteryQuery.WithContext(context.Background())
		if err := query.Bind(data.ID, data.EntryTime).Exec(); err != nil {
			cs.logger.Error("caught error while insert data to scylla", "error", err)
		}
	}
}
