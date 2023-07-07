package main

import (
	"context"
	"fmt"
	"github.com/Seascape-Foundation/sds-service-lib/configuration"
	"github.com/Seascape-Foundation/sds-service-lib/log"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go/modules/mysql"
)

// We won't test the requests.
// The requests are tested in the controllers
// Define the suite, and absorb the built-in basic suite
// functionality from testify - including a T() method which
// returns the current testing context
type TestMysqlSuite struct {
	suite.Suite
	dbName    string
	container *mysql.MySQLContainer
	dbCon     *Database
	ctx       context.Context
}

func (suite *TestMysqlSuite) SetupTest() {
	suite.dbName = "test"
	_, filename, _, _ := runtime.Caller(0)
	storageAbiSql := "20230308171023_storage_abi.sql"
	storageAbiPath := filepath.Join(filepath.Dir(filename), "..", "_db", "migrations", storageAbiSql)

	// create_test_db := "create_test_db.sql"
	// create_test_db_path := filepath.Join(filepath.Dir(filename), "..", "_db", create_test_db)

	ctx := context.TODO()
	container, err := mysql.RunContainer(ctx,
		mysql.WithDatabase(suite.dbName),
		mysql.WithUsername("root"),
		mysql.WithPassword("tiger"),
		mysql.WithScripts(storageAbiPath),
	)

	suite.Require().NoError(err)
	suite.container = container
	suite.ctx = ctx

	logger, err := log.New("mysql-suite", false)
	suite.Require().NoError(err)
	appConfig, err := configuration.NewAppConfig(logger)
	suite.Require().NoError(err)

	// Getting default parameters should fail
	// since we don't have any data set yet
	credentials := GetDefaultCredentials(appConfig)
	suite.Require().Empty(credentials.Username)
	suite.Require().Empty(credentials.Password)

	// after settings the default parameters
	// we should have the username and password
	appConfig.SetDefaults(DatabaseConfigurations)
	credentials = GetDefaultCredentials(appConfig)
	suite.Require().Equal("root", credentials.Username)
	suite.Require().Equal("tiger", credentials.Password)

	// Overwrite the host
	host, err := container.Host(ctx)
	suite.Require().NoError(err)
	appConfig.SetDefault("SDS_DATABASE_HOST", host)

	// Overwrite the port
	ports, err := container.Ports(ctx)
	suite.Require().NoError(err)
	exposedPort := ""
	for _, port := range ports {
		if len(ports) > 0 {
			exposedPort = port[0].HostPort
			break
		}
	}
	suite.Require().NotEmpty(exposedPort)
	appConfig.SetDefault("SDS_DATABASE_PORT", exposedPort)

	// overwrite the database name
	appConfig.SetDefault("SDS_DATABASE_NAME", suite.dbName)
	parameters, err := GetParameters(appConfig)
	suite.Require().NoError(err)
	suite.Require().Equal(suite.dbName, parameters.name)

	// Connect to the database
	suite.T().Log("open database connection by", parameters.hostname, credentials)
	dbCon, err := Open(logger, parameters, credentials)
	suite.Require().NoError(err)
	suite.dbCon = dbCon

	suite.T().Cleanup(func() {
		if err := container.Terminate(ctx); err != nil {
			suite.T().Fatalf("failed to terminate container: %s", err)
		}
		if err := dbCon.Close(); err != nil {
			suite.T().Fatalf("failed to terminate database connection: %s", err)
		}
	})
}

func (suite *TestMysqlSuite) TestInsert() {
	// query
	query := `INSERT INTO storage_abi (abi_id, body) VALUES (?, ?)`
	arguments := []interface{}{"test_id", `[{}]`}

	_, err := suite.dbCon.Query(suite.ctx, query, arguments)
	suite.Require().NoError(err)

	// query
	query = `SELECT abi_id FROM storage_abi WHERE abi_id = ?`
	arguments = []interface{}{"test_id"}

	_, err = suite.dbCon.Query(suite.ctx, query, arguments)
	suite.Require().NoError(err)
}

func (suite *TestMysqlSuite) TestSelect() {
	// query
	query := `SELECT abi_id FROM storage_abi WHERE abi_id = ?`
	arguments := []interface{}{"test_id"}

	result, err := suite.dbCon.Query(suite.ctx, query, arguments)
	suite.Require().NoError(err)
	fmt.Println("the select result", result)
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestMysql(t *testing.T) {
	suite.Run(t, new(TestMysqlSuite))
}
