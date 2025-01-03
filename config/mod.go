package config

type Config struct {
	Port int `json:"port"`
	DB   struct {
		Scylla struct {
			Hosts           []string `json:"hosts"`
			KeySpace        string   `json:"keyspace"`
			BatteryTable    string   `json:"battery_table"`
			BatteryCDCTable string   `json:"battery_cdc_table"`
		} `json:"scylla"`
		Rethink struct {
			Addresses    []string `json:"addresses"`
			Database     string   `json:"database"`
			BatteryTable string   `json:"battery_table"`
		} `json:"rethink"`
	} `json:"db"`
	MQTT struct {
		Address  string `json:"address"`
		ClientID string `json:"client_id"`
		Topic    string `json:"topic"`
		Username string `json:"username"`
		Password string `json:"password"`
	} `json:"mqtt"`
}
