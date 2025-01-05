package cdc

import (
	"context"
	"go-scylladb-cdc/model"
	"time"

	scyllacdc "github.com/scylladb/scylla-cdc-go"
)

func consumer(replicateChan chan<- *model.Battery, batteryStatusChan chan<- *model.Battery) scyllacdc.ChangeConsumerFunc {
	return func(ctx context.Context, tableName string, c scyllacdc.Change) error {
		for _, changeRow := range c.Delta {
			data := new(model.Battery)

			if id, ok := changeRow.GetValue("id"); ok {
				data.ID = *id.(*string)
			}

			if entryTime, ok := changeRow.GetValue("entry_time"); ok {
				data.EntryTime = *entryTime.(*time.Time)
			}

			if voltage, ok := changeRow.GetValue("voltage"); ok {
				data.Voltage = *voltage.(*float32)
			}

			if current, ok := changeRow.GetValue("current"); ok {
				data.Current = *current.(*float32)
			}

			if capacity, ok := changeRow.GetValue("capacity"); ok {
				data.Capacity = *capacity.(*float32)
			}

			if power, ok := changeRow.GetValue("power"); ok {
				data.Power = *power.(*int)
			}

			if temperature, ok := changeRow.GetValue("temperature"); ok {
				data.Temperature = *temperature.(*float32)
			}

			if soc, ok := changeRow.GetValue("soc"); ok {
				data.SOC = *soc.(*int)
			}

			if internalResistance, ok := changeRow.GetValue("internal_resistance"); ok {
				data.InternalResistance = *internalResistance.(*float32)
			}

			replicateChan <- data
			batteryStatusChan <- data
		}
		return nil
	}
}

func (cs *cdcService) startReplicate() error {
	cs.wg.Add(1)
	go func() {
		defer cs.wg.Done()
		for data := range cs.replicateChan {
			if err := cs.dbService.Write(data); err != nil {
				cs.logger.Error().Err(err).Msg("failed to save replicate data")
			}
		}
	}()
	return nil
}

func (cs *cdcService) startBatteryChecker() {
	cs.wg.Add(1)
	go func() {
		defer cs.wg.Done()
		for data := range cs.batteryStatusChan {
			if data.Capacity < 10 {
				cs.logger.Warn().Msgf("current battery is low, id: [%s], capacity: [%.2f]", data.ID, data.Capacity)
			}
		}
	}()
}
