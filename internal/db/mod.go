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

	return result, nil
}

func (d *dbService) Close() error {
	d.session.Close()
	return nil
}

func (d *dbService) Write(data *model.Battery) error {
	if _, err := r.
		Table(d.tableName).
		Insert(data).
		RunWrite(d.session); err != nil {
		return err
	} else {

	}
	return nil
}
