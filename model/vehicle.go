package model

import "time"

type Battery struct {
	ID                 string    `json:"id"`
	EntryTime          time.Time `json:"entryTime"`
	Voltage            float32   `json:"voltage"`
	Current            float32   `json:"current"`
	Capacity           float32   `json:"capacity"`
	Power              int       `json:"power"`
	Temperature        float32   `json:"temperature"`
	SOC                int       `json:"soc"`
	InternalResistance float32   `json:"internal_resistance"`
}
