package main

import (
	"github.com/Seascape-Foundation/mysql-seascape-extension/handler"
	"github.com/Seascape-Foundation/sds-service-lib/configuration"
	"github.com/Seascape-Foundation/sds-service-lib/extension"
	"github.com/Seascape-Foundation/sds-service-lib/log"
	"sync"
)

func main() {
	logger, err := log.New("main", true)
	if err != nil {
		logger.Fatal("log.New(`main`)", "error", err)
	}

	logger.Info("Load app configuration")
	appConfig, err := configuration.NewAppConfig(logger)
	if err != nil {
		logger.Fatal("configuration.NewAppConfig", "error", err)
	}
	logger.Info("App configuration loaded successfully")

	if len(appConfig.Services) == 0 {
		logger.Fatal("missing service configuration in seascape.yml")
	}

	////////////////////////////////////////////////////////////////////////
	//
	// Establish a connection to the mysql
	//
	////////////////////////////////////////////////////////////////////////
	// create a database connection
	// if security is enabled, then get the database credentials from vault
	// Set the database connection
	appConfig.SetDefaults(DatabaseConfigurations)
	databaseParameters, err := GetParameters(appConfig)
	if err != nil {
		logger.Fatal("GetParameters", "error", err)
	}

	if appConfig.Secure {
		logger.Info("Security enabled, therefore start pull controller that waits credentials from vault service")
		db = &Database{
			Connection:      nil,
			connectionMutex: sync.Mutex{},
			parameters:      *databaseParameters,
			logger:          logger,
		}
		// vault will push the credentials here
		db.runPuller()
	} else {
		logger.Info("Database is connected in an unsafe way. Connecting with default credentials")

		db, err = connectWithDefault(appConfig, logger, databaseParameters)
		if err != nil {
			logger.Fatal("database error", "message", err)
		}
		logger.Info("Database connected successfully!")
	}

	logger.Info("Run database controller")

	/////////////////////////////////////////////////////////////////////////
	//
	// Create the extension
	//
	/////////////////////////////////////////////////////////////////////////

	service, err := extension.New(appConfig.Services[0], logger)
	if err != nil {
		logger.Fatal("failed to initialize extension", "error", err)
	}

	dbController := service.GetFirstController()
	dbController.RegisterCommand(handler.EXIST, onExist)
	dbController.RegisterCommand(handler.SelectRow, onSelectRow)
	dbController.RegisterCommand(handler.SelectAll, onSelectAll)
	dbController.RegisterCommand(handler.DELETE, onDelete)
	dbController.RegisterCommand(handler.INSERT, onInsert)
	dbController.RegisterCommand(handler.UPDATE, onUpdate)

	service.Run()
}
