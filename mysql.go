package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/Seascape-Foundation/sds-service-lib/configuration"
	"github.com/Seascape-Foundation/sds-service-lib/log"
	"sync"
	"time"

	"github.com/Seascape-Foundation/sds-common-lib/data_type/key_value"
	_ "github.com/go-sql-driver/mysql"
)

// TimeoutCap Any configuration time can not be greater than this
const TimeoutCap = 3600

type DatabaseParameters struct {
	hostname string
	port     string
	name     string
	timeout  time.Duration
}

// DatabaseCredentials is a set of dynamic credentials retrieved from Vault
type DatabaseCredentials struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

// Database Global database structure that's initiated in the main().
// Then its passed to all controllers.
type Database struct {
	Connection      *sql.DB
	connectionMutex sync.Mutex
	parameters      DatabaseParameters
	logger          log.Logger
}

// DatabaseConfigurations The configuration parameters
// The values are the default values if it wasn't provided by the user
// Set the default value to nil, if the parameter is required from the user
var DatabaseConfigurations = configuration.DefaultConfig{
	Title: "Database",
	Parameters: key_value.New(map[string]interface{}{
		"SDS_DATABASE_HOST":     "localhost",
		"SDS_DATABASE_PORT":     "3306",
		"SDS_DATABASE_NAME":     "seascape_sds",
		"SDS_DATABASE_TIMEOUT":  uint64(10),
		"SDS_DATABASE_USERNAME": "root",
		"SDS_DATABASE_PASSWORD": "tiger",
	}),
}

// GetParameters returns the database parameters fetched from the environment variables.
//
// The `app_config` keeps the default variables if the parameters were not set.
func GetParameters(appConfig *configuration.Config) (*DatabaseParameters, error) {
	timeout := appConfig.GetUint64("SDS_DATABASE_TIMEOUT")
	if timeout > TimeoutCap {
		return nil, fmt.Errorf("'SDS_DATABASE_TIMEOUT' can not be greater than %d (seconds)", TimeoutCap)
	} else if timeout == 0 {
		return nil, errors.New("the 'SDS_DATABASE_TIMEOUT' can not be zero")
	}

	return &DatabaseParameters{
		hostname: appConfig.GetString("SDS_DATABASE_HOST"),
		port:     appConfig.GetString("SDS_DATABASE_PORT"),
		name:     appConfig.GetString("SDS_DATABASE_NAME"),
		timeout:  time.Duration(timeout) * time.Second,
	}, nil
}

// GetDefaultCredentials returns the default user/password that has access to the database
func GetDefaultCredentials(appConfig *configuration.Config) DatabaseCredentials {
	return DatabaseCredentials{
		Username: appConfig.GetString("SDS_DATABASE_USERNAME"),
		Password: appConfig.GetString("SDS_DATABASE_PASSWORD"),
	}
}

// Open establishes a database connection
func connectWithDefault(appConfig *configuration.Config, logger log.Logger, parameters *DatabaseParameters) (*Database, error) {
	database := &Database{
		Connection:      nil,
		connectionMutex: sync.Mutex{},
		parameters:      *parameters,
		logger:          logger,
	}

	// establish the first connection
	if err := database.Reconnect(GetDefaultCredentials(appConfig)); err != nil {
		return nil, fmt.Errorf("database.reconnect: %w", err)
	}

	return database, nil
}

func (database *Database) Timeout() time.Duration {
	return database.parameters.timeout
}

// Reconnect will be called periodically to refresh the database connection
// since the dynamic credentials expire after some time, it will:
//  1. construct a connection string using the given credentials
//  2. establish a database connection
//  3. close & replace the existing connection with the new one behind a mutex
func (database *Database) Reconnect(credentials DatabaseCredentials) error {
	ctx, cancelContextFunc := context.WithTimeout(context.Background(), database.parameters.timeout)
	defer cancelContextFunc()

	database.logger.Info(
		"connecting to `mysql` database",
		"protocol", "tcp",
		"database", database.parameters.name,
		"host", database.parameters.hostname,
		"port", database.parameters.port,
		"user", credentials.Username,
		"timeout", database.parameters.timeout,
	)

	dsn := fmt.Sprintf(
		"%s:%s@tcp(%s:%s)/%s?timeout=%s",
		credentials.Username,
		credentials.Password,
		database.parameters.hostname,
		database.parameters.port,
		database.parameters.name,
		database.parameters.timeout.String(),
	)

	connection, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("sql.open: %w", err)
	}

	// wait until the database is ready or timeout expires
	for {
		err = connection.Ping()
		if err == nil {
			break
		}
		select {
		case <-time.After(500 * time.Millisecond):
			continue
		case <-ctx.Done():
			return fmt.Errorf("database ping error: %v", err.Error())
		}
	}

	database.closeReplaceConnection(connection)

	database.logger.Info("connection success!", "database", database.parameters.name)

	return nil
}

func (database *Database) closeReplaceConnection(new *sql.DB) {
	/* */ database.connectionMutex.Lock()
	defer database.connectionMutex.Unlock()

	// close the existing connection, if exists
	if database.Connection != nil {
		_ = database.Connection.Close()
	}

	// replace with a new connection
	database.Connection = new
}

func (database *Database) Close() error {
	/* */ database.connectionMutex.Lock()
	defer database.connectionMutex.Unlock()

	if database.Connection != nil {
		err := database.Connection.Close()
		if err != nil {
			return fmt.Errorf("connection.Close: %w", err)
		}
	}

	return nil
}

// Query is the sample to test the connection
func (database *Database) Query(ctx context.Context, query string, arguments []interface{}) ([]interface{}, error) {
	database.connectionMutex.Lock()
	defer database.connectionMutex.Unlock()

	rows, err := database.Connection.QueryContext(ctx, query, arguments...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute '%q' query with arguments %v: %w", query, arguments, err)
	}
	defer func() {
		_ = rows.Close()
	}()

	var results []interface{}

	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err != nil {
			return nil, fmt.Errorf("failed to scan table row for %q query: %w", query, err)
		}
		results = append(results, p)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error after scanning %q query: %w", query, err)
	}

	return results, nil
}
