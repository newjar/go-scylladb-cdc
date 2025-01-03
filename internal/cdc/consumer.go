package cdc

import (
	"context"
	"fmt"
	"go-iot-cdc/model"
	"log"
	"time"

	scyllacdc "github.com/scylladb/scylla-cdc-go"
)

type consumer struct {
}

func (cs *cdcService) cdcListener() {
	cs.wg.Add(1)

	go func() {
		defer cs.wg.Done()

		lastTimestamp := time.Now().Add(5 * time.Minute)
		query := cs.batteryCDCQuery.Bind(lastTimestamp)

		iter := query.Iter()

		var (
			id        string
			entryTime time.Time
		)

		for iter.Scan(&id, &entryTime) {
			cs.logger.Info("Change detected")
			cs.logger.Info("Data changes", "id", id, "entryTime", entryTime)

			if entryTime.After(lastTimestamp) {
				lastTimestamp = entryTime
			}
		}

		if err := iter.Close(); err != nil {
			log.Printf("Error reading CDC log: %v", err)
		}
	}()
}

func (c *consumer) Consume(ctx context.Context, change scyllacdc.Change) error {
	for _, changeRow := range change.Delta {
		var (
			ID                 string
			entryTime          time.Time
			Voltage            float32
			Current            float32
			Capacity           float32
			Power              int
			Temperature        float32
			SOC                int
			InternalResistance float32
		)

		if v, ok := changeRow.GetValue("id"); !ok {
			continue
		} else {
			ID = v.(string)
		}

		if v, ok := changeRow.GetValue("entryTime"); !ok {
			continue
		} else {
			entryTime = v.(time.Time)
		}

		if v, ok := changeRow.GetValue("voltage"); !ok {
			continue
		} else {
			v = v.(float32)
		}

		if v, ok := changeRow.GetValue("current"); !ok {
			continue
		} else {
			Current = v.(float32)
		}

		if v, ok := changeRow.GetValue("capacity"); !ok {
			continue
		} else {
			Capacity = v.(float32)
		}

		if v, ok := changeRow.GetValue("power"); !ok {
			continue
		} else {
			Power = v.(int)
		}

		if v, ok := changeRow.GetValue("temperature"); !ok {
			continue
		} else {
			Temperature = v.(float32)
		}

		if v, ok := changeRow.GetValue("soc"); !ok {
			continue
		} else {
			SOC = v.(int)
		}

		if v, ok := changeRow.GetValue("internal_resistance"); !ok {
			continue
		} else {
			InternalResistance = v.(float32)
		}

		data := new(model.Battery)
		*data = model.Battery{
			ID:                 ID,
			EntryTime:          entryTime,
			Voltage:            Voltage,
			Current:            Current,
			Capacity:           Capacity,
			Power:              Power,
			Temperature:        Temperature,
			SOC:                SOC,
			InternalResistance: InternalResistance,
		}

		fmt.Println(data)

		// if err := c.cdc.dbService.Write(data); err != nil {
		// 	// log error
		// 	fmt.Println("Error: ", err)
		// }
	}
	return nil
}

func (c *consumer) End() error {
	return nil
}

func (cs *consumer) CreateChangeConsumer(ctx context.Context, input scyllacdc.CreateChangeConsumerInput) (scyllacdc.ChangeConsumer, error) {
	return &consumer{}, nil
}
