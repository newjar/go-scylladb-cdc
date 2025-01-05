package main

import (
	"context"
	"encoding/json"
	"fmt"
	"go-scylladb-cdc/config"
	"go-scylladb-cdc/internal/cdc"
	"go-scylladb-cdc/internal/db"
	"go-scylladb-cdc/internal/mqtt"
	"go-scylladb-cdc/model"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
	_ "github.com/rs/zerolog/log"
	"github.com/urfave/cli/v2"
)

func buildLogger() *zerolog.Logger {
	logger := new(zerolog.Logger)
	zerolog.TimeFieldFormat = time.RFC3339
	*logger = zerolog.New(os.Stdout).With().Timestamp().Logger().Level(zerolog.InfoLevel)
	return logger
}

func buildApp(logger *zerolog.Logger) *cli.App {
	app := cli.NewApp()
	app.Usage = "Change Data Capture (CDC) for IoT"
	app.Version = Version
	app.Flags = []cli.Flag{
		&cli.PathFlag{
			Name:     "config",
			Aliases:  []string{"c"},
			Usage:    "Path to the configuration file (JSON format)",
			Required: true,
		},
	}
	app.Action = mainAction(logger)
	return app
}

func mainAction(logger *zerolog.Logger) cli.ActionFunc {
	return func(cliCtx *cli.Context) error {
		appCtx, cancel := context.WithCancel(context.Background())
		defer cancel()

		configPath := cliCtx.Path("config")
		config, err := loadConfigFromFile(configPath)
		if err != nil {
			return err
		}

		var dbService db.DBService
		if svc, err := db.NewDBService(
			appCtx,
			logger,
			config.DB.Rethink.Addresses,
			config.DB.Rethink.Database,
			config.DB.Rethink.BatteryTable); err != nil {
			logger.Error().Err(err).Msg("failed to create DB service")
			return err
		} else {
			logger.Info().Msg("connected into rethinkdb")
			dbService = svc
		}

		msgChan := make(chan *model.Battery, 1024)

		var mqttService mqtt.MQTTService
		if svc, err := mqtt.NewMQTTService(
			appCtx,
			logger,
			config.MQTT.Address,
			config.MQTT.ClientID,
			config.MQTT.Username,
			config.MQTT.Password,
			config.MQTT.Topic,
			msgChan); err != nil {
			logger.Error().Err(err).Msg("failed to create MQTT service")
			return err
		} else {
			logger.Info().Msg("connected into mqtt-server")
			mqttService = svc
		}

		var cdcService cdc.CDCService
		if svc, err := cdc.NewCDCService(
			appCtx,
			logger,
			config.DB.Scylla.KeySpace,
			config.DB.Scylla.BatteryTable,
			config.DB.Scylla.BatteryCDCTable,
			config.DB.Scylla.Hosts,
			dbService,
			msgChan); err != nil {
			logger.Error().Err(err).Msg("failed to create CDC service")
			return err
		} else {
			logger.Info().Msg("connected into scylladb")
			cdcService = svc
			cdcService.Start()
		}

		router := configureRouter()
		wgApp := new(sync.WaitGroup)
		wgApp.Add(1)

		go func(
			ctx context.Context,
			cancel context.CancelFunc,
			wg *sync.WaitGroup,
			router *fiber.App) {
			defer wgApp.Done()
			<-ctx.Done()
			cdcService.Stop()
			dbService.Close()
			mqttService.Stop()
			router.Shutdown()
			cancel()
		}(appCtx, cancel, wgApp, router)

		if err := router.Listen(fmt.Sprintf(":%d", config.Port)); err != nil {
			logger.Error().Err(err).Msg("failed to start the server")
		}

		wgApp.Wait()

		return nil
	}
}

func main() {
	logger := buildLogger()
	app := buildApp(logger)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	if err := app.RunContext(ctx, os.Args); err != nil {
		logger.Error().Err(err).Msg("failed to run the app")
		os.Exit(1)
	}
}

func loadConfigFromFile(filePath string) (*config.Config, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open config file: %w", err)
	}
	defer file.Close()

	c := new(config.Config)
	if err := json.NewDecoder(file).Decode(c); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	return c, nil
}
