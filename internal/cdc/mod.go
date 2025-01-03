package cdc

import (
	"context"
	"fmt"
	internal "go-iot-cdc/internal/db"
	"go-iot-cdc/model"
	"log/slog"
	"sync"
	"time"

	"github.com/gocql/gocql"
	scyllacdc "github.com/scylladb/scylla-cdc-go"
)

func createConnection(
	ctx context.Context,
	logger *slog.Logger,
	keyspace, tableName string,
	hosts []string) (*gocql.Session, *gocql.Session, *scyllacdc.Reader, error) {
	var (
		cdcReader  *scyllacdc.Reader
		readerSess *gocql.Session
		writerSess *gocql.Session
		// progressManager *scyllacdc.TableBackedProgressManager
	)

	wtCluster := gocql.NewCluster(hosts...)
	wtCluster.Timeout = 10 * time.Second
	wtCluster.Consistency = gocql.All
	wtCluster.Keyspace = keyspace

	if sess, err := wtCluster.CreateSession(); err != nil {
		logger.Error("failed to create scylla writer session", "error", err)
		return nil, nil, nil, err
	} else {
		writerSess = sess
	}

	// if pm, err := scyllacdc.NewTableBackedProgressManager(writerSess, tableName, "cdc-writer"); err != nil {
	// 	writerSess.Close()
	// 	logger.Error("failed to create scylla-cdc progress manager", "error", err)
	// 	return nil, nil, nil, err
	// } else {
	// 	progressManager = pm
	// }

	rdCluster := gocql.NewCluster(hosts...)
	rdCluster.Timeout = 10 * time.Second
	rdCluster.Consistency = gocql.All
	rdCluster.Keyspace = keyspace
	rdCluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy())

	if sess, err := rdCluster.CreateSession(); err != nil {
		writerSess.Close()
		logger.Error("failed to create scylla session cluster", "error", err)
		return nil, nil, nil, err
	} else {
		readerSess = sess
	}

	// adv := scyllacdc.AdvancedReaderConfig{
	// 	ConfidenceWindowSize:   5 * time.Minute,
	// 	QueryTimeWindowSize:    1 * time.Minute,
	// 	PostEmptyQueryDelay:    30 * time.Second,
	// 	PostNonEmptyQueryDelay: 10 * time.Second,
	// 	PostFailedQueryDelay:   1 * time.Second,
	// }

	// if r, err := scyllacdc.NewReader(ctx, &scyllacdc.ReaderConfig{
	// 	Session:               readerSess,
	// 	TableNames:            []string{tableName},
	// 	Consistency:           gocql.All,
	// 	ProgressManager:       progressManager,
	// 	ChangeConsumerFactory: &consumer{},
	// 	Advanced:              adv,
	// }); err != nil {
	// 	writerSess.Close()
	// 	readerSess.Close()
	// 	logger.Error("failed to create scylla cdc-reader", "error", err)
	// 	return nil, nil, nil, err
	// } else {
	// 	cdcReader = r
	// }
	return writerSess, readerSess, cdcReader, nil

}

type cdcService struct {
	ctx    context.Context
	logger *slog.Logger

	wg *sync.WaitGroup

	writerSess *gocql.Session
	readerSess *gocql.Session
	cdcReader  *scyllacdc.Reader

	batteryQuery    *gocql.Query
	batteryCDCQuery *gocql.Query

	rowsRead *int64

	dbService internal.DBService

	msgChan <-chan *model.Battery
}

type CDCService interface {
	Stop()
	Start()
}

func NewCDCService(
	parentCtx context.Context,
	logger *slog.Logger,
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

	result := new(cdcService)

	if ws, rs, cdcr, err := createConnection(
		parentCtx,
		logger,
		keyspace,
		tableName,
		hosts); err != nil {
		return nil, err
	} else {
		writerSess = ws
		readerSess = rs
		cdcReader = cdcr
	}

	// batteryQuery := writerSess.Query(fmt.Sprintf(`INSERT INTO
	// %s (id, entry_time, voltage, current, capacity, power, temperature, soc, internal_resistance)
	// VALUES (?,?,?,?,?,?,?,?,?)`, tableName))

	batteryQuery := writerSess.Query(fmt.Sprintf(`INSERT INTO
	%s (id, entry_time)
	VALUES (?, ?)`, tableName))

	batteryCDCQuery := readerSess.Query(fmt.Sprintf(`SELECT id, entry_time FROM %s
	WHERE entry_time > ? LIMIT 100`, cdcTableName))

	*result = cdcService{
		ctx:    parentCtx,
		logger: logger,

		wg: new(sync.WaitGroup),

		writerSess: writerSess,
		readerSess: readerSess,
		rowsRead:   new(int64),
		cdcReader:  cdcReader,

		batteryQuery:    batteryQuery,
		batteryCDCQuery: batteryCDCQuery,

		dbService: db,

		msgChan: msgChan,
	}

	result.cdcListener()

	return result, nil
}

func (cs *cdcService) Stop() {
	cs.wg.Wait()
	cs.cdcReader.Stop()
	cs.writerSess.Close()
	cs.readerSess.Close()
}
