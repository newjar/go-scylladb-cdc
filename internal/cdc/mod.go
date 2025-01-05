package cdc

import (
	"context"
	"fmt"
	internal "go-scylladb-cdc/internal/db"
	"go-scylladb-cdc/model"
	"sync"
	"time"

	"github.com/gocql/gocql"
	"github.com/rs/zerolog"
	scyllacdc "github.com/scylladb/scylla-cdc-go"
)

func createConnection(
	ctx context.Context,
	logger *zerolog.Logger,
	keyspace, tableName string,
	hosts []string,
	changeConsumerFactory scyllacdc.ChangeConsumerFactory,
) (*gocql.Session, *gocql.Session, *scyllacdc.Reader, error) {
	var (
		cdcReader  *scyllacdc.Reader
		readerSess *gocql.Session
		writerSess *gocql.Session
	)

	wtCluster := gocql.NewCluster(hosts...)
	wtCluster.Timeout = 10 * time.Second
	wtCluster.Consistency = gocql.All
	wtCluster.Keyspace = keyspace

	if sess, err := wtCluster.CreateSession(); err != nil {
		logger.Error().Err(err).Msg("failed to create scylla writer session")
		return nil, nil, nil, err
	} else {
		writerSess = sess
	}

	rdCluster := gocql.NewCluster(hosts...)
	rdCluster.Timeout = 10 * time.Second
	rdCluster.Consistency = gocql.All
	rdCluster.Keyspace = keyspace
	rdCluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy())

	if sess, err := rdCluster.CreateSession(); err != nil {
		writerSess.Close()
		logger.Error().Err(err).Msg("failed to create scylla session cluster")
		return nil, nil, nil, err
	} else {
		readerSess = sess
	}

	// adjust cdc reader config
	adv := scyllacdc.AdvancedReaderConfig{
		ConfidenceWindowSize:   10 * time.Second,
		QueryTimeWindowSize:    10 * time.Second,
		PostEmptyQueryDelay:    5 * time.Second,
		PostNonEmptyQueryDelay: 3 * time.Second,
		PostFailedQueryDelay:   3 * time.Second,
		ChangeAgeLimit:         3 * time.Second,
	}

	if r, err := scyllacdc.NewReader(ctx, &scyllacdc.ReaderConfig{
		Session:               readerSess,
		TableNames:            []string{fmt.Sprintf("%s.%s", keyspace, tableName)},
		Consistency:           gocql.One,
		ChangeConsumerFactory: changeConsumerFactory,
		Logger:                logger,
		Advanced:              adv,
	}); err != nil {
		writerSess.Close()
		readerSess.Close()
		logger.Error().Err(err).Msg("failed to create scylla cdc-reader")
		return nil, nil, nil, err
	} else {
		cdcReader = r
	}
	return writerSess, readerSess, cdcReader, nil

}

type cdcService struct {
	ctx    context.Context
	logger *zerolog.Logger

	wg *sync.WaitGroup

	writerSess *gocql.Session
	readerSess *gocql.Session
	cdcReader  *scyllacdc.Reader

	batteryQuery *gocql.Query

	rowsRead *int64

	dbService internal.DBService

	msgChan           <-chan *model.Battery
	replicateChan     <-chan *model.Battery
	batteryStatusChan <-chan *model.Battery

	keyspace, tableName string

	changeConsumerFactory scyllacdc.ChangeConsumerFactory

	maxNumWorkers int
}

type CDCService interface {
	Stop()
	Start()
}

func NewCDCService(
	parentCtx context.Context,
	logger *zerolog.Logger,
	keyspace, tableName, cdcTableName string,
	hosts []string,
	db internal.DBService,
	msgChan chan *model.Battery,
) (CDCService, error) {
	var (
		writerSess *gocql.Session
		readerSess *gocql.Session
		cdcReader  *scyllacdc.Reader
	)

	replicateChan := make(chan *model.Battery)
	batteryStatusChan := make(chan *model.Battery)

	result := new(cdcService)
	changeConsumerFactory := scyllacdc.MakeChangeConsumerFactoryFromFunc(consumer(replicateChan, batteryStatusChan))

	if ws, rs, cdcr, err := createConnection(
		parentCtx,
		logger,
		keyspace,
		tableName,
		hosts,
		changeConsumerFactory,
	); err != nil {
		return nil, err
	} else {
		writerSess = ws
		readerSess = rs
		cdcReader = cdcr
	}

	batteryQuery := writerSess.Query(fmt.Sprintf(`INSERT INTO
	%s (id, entry_time, voltage, current, capacity, power, temperature, soc, internal_resistance)
	VALUES (?,?,?,?,?,?,?,?,?)`, tableName))

	*result = cdcService{
		ctx:    parentCtx,
		logger: logger,

		wg: new(sync.WaitGroup),

		writerSess: writerSess,
		readerSess: readerSess,
		rowsRead:   new(int64),
		cdcReader:  cdcReader,

		batteryQuery: batteryQuery,

		dbService: db,

		keyspace:  keyspace,
		tableName: tableName,

		msgChan:           msgChan,
		replicateChan:     replicateChan,
		batteryStatusChan: batteryStatusChan,

		changeConsumerFactory: changeConsumerFactory,

		maxNumWorkers: 10,
	}

	result.startBatteryChecker()
	result.startReplicate()

	return result, nil
}

func (cs *cdcService) Stop() {
	cs.cdcReader.Stop()
	cs.writerSess.Close()
	cs.readerSess.Close()
	cs.wg.Wait()
}
