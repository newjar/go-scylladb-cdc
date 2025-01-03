package main

import (
	"context"
	"encoding/json"
	"fmt"
	"go-iot-cdc/config"
	"go-iot-cdc/internal/cdc"
	"go-iot-cdc/internal/db"
	"go-iot-cdc/internal/mqtt"
	"go-iot-cdc/model"
	"log/slog"
	"os"
	"os/signal"
	"sync"

	"github.com/gofiber/fiber/v2"
	"github.com/urfave/cli/v2"
)

func buildLogger() *slog.Logger {
	logHandler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})
	return slog.New(logHandler)
}

func buildApp(logger *slog.Logger) *cli.App {
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

func mainAction(logger *slog.Logger) cli.ActionFunc {
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
			logger.Error("failed to create DB service", "error", err)
			return err
		} else {
			logger.Info("connected into rethinkdb")
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
			logger.Error("failed to create MQTT service", "error", err)
			return err
		} else {
			logger.Info("connected into mqtt-server")
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
			logger.Error("failed to create CDC service", "error", err)
			return err
		} else {
			logger.Info("connected into scylladb")
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
			logger.Error("failed to start the server", "error", err)
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
		logger.Error("failed to run the app", "error", err)
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
