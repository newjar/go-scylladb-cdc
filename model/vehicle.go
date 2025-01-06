package model

import "time"

type Battery struct {
	ID                 string    `json:"id" rethinkdb:"vehicle_id"`
	EntryTime          time.Time `json:"entry_time" rethinkdb:"entry_time"`
	Voltage            float32   `json:"voltage" rethinkdb:"voltage"`
	Current            float32   `json:"current" rethinkdb:"current"`
	Capacity           float32   `json:"capacity" rethinkdb:"capacity"`
	Power              int       `json:"power" rethinkdb:"power"`
	Temperature        float32   `json:"temperature" rethinkdb:"temperature"`
	SOC                int       `json:"soc" rethinkdb:"soc"`
	InternalResistance float32   `json:"internal_resistance" rethinkdb:"internal_resistance"`
}
