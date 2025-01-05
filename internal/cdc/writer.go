package cdc

import (
	"context"
	"go-scylladb-cdc/model"
)

func (cs *cdcService) Start() {
	cs.wg.Add(cs.maxNumWorkers + 1)

	for i := 0; i < 10; i++ {
		go cs.msgProcessor()
	}

	go cs.cdcReader.Run(cs.ctx)
}

func (cs *cdcService) msgProcessor() {
	defer cs.wg.Done()

	for msg := range cs.msgChan {
		data := new(model.Battery)
		data.ID = msg.ID
		data.EntryTime = msg.EntryTime
		data.Voltage = msg.Voltage
		data.Current = msg.Current
		data.Capacity = msg.Capacity
		data.Power = msg.Power
		data.Temperature = msg.Temperature
		data.SOC = msg.SOC
		data.InternalResistance = msg.InternalResistance

		query := cs.batteryQuery.WithContext(context.Background())
		if err := query.Bind(
			data.ID,
			data.EntryTime,
			data.Voltage,
			data.Current,
			data.Capacity,
			data.Power,
			data.Temperature,
			data.SOC,
			data.InternalResistance,
		).Exec(); err != nil {
			cs.logger.Error().Err(err).Msg("caught error while insert data to scylla")
		}
	}
}
