package db

import (
	"go-scylladb-cdc/model"

	"github.com/rs/zerolog"
	r "gopkg.in/rethinkdb/rethinkdb-go.v6"

	"context"
)

type dbService struct {
	ctx       context.Context
	session   *r.Session
	tableName string
	logger    *zerolog.Logger
}

type DBService interface {
	Write(data *model.Battery) error
	Close() error
}

func NewDBService(
	appCtx context.Context,
	logger *zerolog.Logger,
	addresses []string,
	dbName, tableName string,
) (DBService, error) {
	var session *r.Session
	if sess, err := r.Connect(r.ConnectOpts{
		Addresses:  addresses,
		InitialCap: 10,
		MaxOpen:    10,
		NumRetries: 2,
		Database:   dbName,
	}); err != nil {
		return nil, err
	} else {
		session = sess
	}

	result := new(dbService)
	*result = dbService{
		ctx:       appCtx,
		session:   session,
		tableName: tableName,
		logger:    logger,
	}

	go result.ReadChanges()

	return result, nil
}

func (d *dbService) Close() error {
	d.session.Close()
	return nil
}

func (d *dbService) Write(data *model.Battery) error {
	record := map[string]interface{}{
		"id":                  data.ID,
		"entry_time":          data.EntryTime,
		"voltage":             data.Voltage,
		"current":             data.Current,
		"capacity":            data.Capacity,
		"power":               data.Power,
		"temperature":         data.Temperature,
		"soc":                 data.SOC,
		"internal_resistance": data.InternalResistance,
	}

	if _, err := r.
		Table(d.tableName).
		Insert(record).
		RunWrite(d.session); err != nil {
		return err
	}
	return nil
}

func (d *dbService) ReadChanges() error {
	cursor, err := r.Table(d.tableName).Changes().Run(d.session)
	if err != nil {
		return err
	}

	var changeRow r.ChangeResponse
	for cursor.Next(&changeRow) {
		newValue := changeRow.NewValue.(map[string]interface{})
		d.logger.Info().Msgf("read changes, id [%s] capacity [%.2f]", newValue["ID"], newValue["Capacity"])
	}

	return nil
}
