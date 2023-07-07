// The database package handles all the database operations.
// Note that for now it uses Mysql as a hardcoded data
//
// The database is creating a new service with the inproc reply controller.
// For any database operation interact with the service.
package main

import (
	"database/sql"
	"github.com/Seascape-Foundation/mysql-seascape-extension/handler"
	"github.com/Seascape-Foundation/sds-common-lib/data_type/database"
	"github.com/Seascape-Foundation/sds-common-lib/data_type/key_value"
	"github.com/Seascape-Foundation/sds-service-lib/communication/command"
	"github.com/Seascape-Foundation/sds-service-lib/communication/message"
	"github.com/Seascape-Foundation/sds-service-lib/controller"
	"github.com/Seascape-Foundation/sds-service-lib/log"
	"github.com/Seascape-Foundation/sds-service-lib/remote"
)

var db *Database

// run_puller creates a pull controller that gets the
// new database credentials to reconnect.
func (database *Database) runPuller() {
	database.logger.Info("Creating puller service to get credentials from vault service", "url", handler.PullerEndpoint())

	pull, err := controller.NewPull(database.logger)
	if err != nil {
		database.logger.Fatal("controller.NewPull", "error", err)
	}
	pull.RegisterCommand(handler.NewCredentials, onNewCredentials)

	database.logger.Info("Running pull controller")
	err = pull.Run()
	if err != nil {
		database.logger.Fatal("puller failed", "error", err)
	}
}

// puller received new credentials
var onNewCredentials = func(request message.Request, _ log.Logger, _ remote.Clients) message.Reply {
	var credentials DatabaseCredentials
	err := request.Parameters.Interface(&credentials)
	if err != nil {
		return message.Fail("the received database credentials are invalid")
	}

	if db.Connection == nil {
		return message.Fail("database.Connection is nil, please open the connection first")
	}

	// establish the first connection
	if err := db.Reconnect(credentials); err != nil {
		return message.Fail("database.reconnect:" + err.Error())
	}

	return message.Reply{
		Status:     message.OK,
		Message:    "",
		Parameters: key_value.Empty(),
	}
}

// selects all rows from the database
//
// intended to be used once during the app launch for caching.
//
// Minimize the database queries by using this
var onSelectAll = func(request message.Request, _ log.Logger, _ remote.Clients) message.Reply {
	if db == nil || db.Connection == nil {
		return message.Fail("database.Connection is nil, please open the connection first")
	}

	//parameters []interface{}, outputs []interface{}
	var queryParameters handler.DatabaseQueryRequest
	err := request.Parameters.Interface(&queryParameters)
	if err != nil {
		return message.Fail("parameter validation:" + err.Error())
	}

	query, err := queryParameters.BuildSelectQuery()
	if err != nil {
		return message.Fail("query_parameter.BuildSelectQuery: " + err.Error())
	}

	var rows *sql.Rows
	if len(queryParameters.Where) > 0 {
		rows, err = db.Connection.Query(query, queryParameters.Arguments...)
	} else {
		rows, err = db.Connection.Query(query)
	}
	if err != nil {
		return message.Fail("db.Connection.Query: " + err.Error())
	}
	fieldTypes, err := rows.ColumnTypes()
	if err != nil {
		return message.Fail("rows.ColumnTypes: " + err.Error())
	}

	replyObjects := make([]key_value.KeyValue, 0)

	for rows.Next() {
		scans := make([]interface{}, len(fieldTypes))
		row := key_value.Empty()

		for i := range scans {
			scans[i] = &scans[i]
		}
		err = rows.Scan(scans...)
		if err != nil {
			return message.Fail("failed to read database data into code: " + err.Error())
		}
		for i, v := range scans {
			err := database.SetValue(row, fieldTypes[i], v)
			if err != nil {
				return message.Fail("failed to set value for field " + fieldTypes[i].Name() + " of " + fieldTypes[i].DatabaseTypeName() + " type: " + err.Error())
			}
		}

		replyObjects = append(replyObjects, key_value.New(row))
	}

	reply := handler.SelectAllReply{
		Rows: replyObjects,
	}
	replyMessage, err := command.Reply(&reply)
	if err != nil {
		return message.Fail("command.Reply: " + err.Error())
	}

	return replyMessage
}

// checks whether there are any rows that matches to the query
var onExist = func(request message.Request, _ log.Logger, _ remote.Clients) message.Reply {
	if db == nil || db.Connection == nil {
		return message.Fail("database.Connection is nil, please open the connection first")
	}

	//parameters []interface{}, outputs []interface{}
	var queryParameters handler.DatabaseQueryRequest
	err := request.Parameters.Interface(&queryParameters)
	if err != nil {
		return message.Fail("parameter validation:" + err.Error())
	}

	query, err := queryParameters.BuildExistQuery()
	if err != nil {
		return message.Fail("query_parameter.BuildExistQuery: " + err.Error())
	}

	rows, err := db.Connection.Query(query, queryParameters.Arguments...)
	if err != nil {
		return message.Fail("db.Connection.Query: " + err.Error())
	}
	reply := handler.ExistReply{}

	if rows.Next() {
		reply.Exist = true
	} else {
		reply.Exist = false
	}

	replyMessage, err := command.Reply(&reply)
	if err != nil {
		return message.Fail("command.Reply: " + err.Error())
	}
	if err := rows.Close(); err != nil {
		message.Fail("database error. failed to close the connection: " + err.Error())
	}

	return replyMessage
}

// Read the row only once
// func on_read_one_row(db *sql.DB, query string, parameters []interface{}, outputs []interface{}) ([]interface{}, error) {
var onSelectRow = func(request message.Request, _ log.Logger, clients remote.Clients) message.Reply {
	if db == nil || db.Connection == nil {
		return message.Fail("database.Connection is nil, please open the connection first")
	}

	//parameters []interface{}, outputs []interface{}
	var queryParameters handler.DatabaseQueryRequest
	err := request.Parameters.Interface(&queryParameters)
	if err != nil {
		return message.Fail("parameter validation:" + err.Error())
	}

	query, err := queryParameters.BuildSelectRowQuery()
	if err != nil {
		return message.Fail("query_parameter.BuildSelectRowQuery: " + err.Error())
	}

	rows, err := db.Connection.Query(query, queryParameters.Arguments...)
	if err != nil {
		return message.Fail("db.Connection.Query: " + err.Error())
	}
	defer func() {
		err := rows.Close()
		if err != nil {
			db.logger.Warn("failed to close the database connection: ", "error", err)
		}
	}()

	fieldTypes, err := rows.ColumnTypes()
	if err != nil {
		return message.Fail("rows.ColumnTypes: " + err.Error())
	}

	row := key_value.Empty()
	noResult := true

	for rows.Next() {
		noResult = false
		scans := make([]interface{}, len(fieldTypes))

		for i := range scans {
			scans[i] = &scans[i]
		}
		err = rows.Scan(scans...)
		if err != nil {
			return message.Fail("failed to read database data into code: " + err.Error())
		}
		for i, v := range scans {
			err := database.SetValue(row, fieldTypes[i], v)
			if err != nil {
				return message.Fail("failed to set value for field " + fieldTypes[i].Name() + " of " + fieldTypes[i].DatabaseTypeName() + " type: " + err.Error())
			}
		}
	}

	if noResult {
		return message.Fail("not found")
	}

	reply := handler.SelectRowReply{
		Outputs: key_value.New(row),
	}
	replyMessage, err := command.Reply(&reply)
	if err != nil {
		return message.Fail("command.Reply: " + err.Error())
	}

	return replyMessage
}

// Execute the deletion
var onDelete = func(request message.Request, _ log.Logger, _ remote.Clients) message.Reply {
	if db == nil || db.Connection == nil {
		return message.Fail("database.Connection is nil, please open the connection first")
	}

	//parameters []interface{}, outputs []interface{}
	var queryParameters handler.DatabaseQueryRequest
	err := request.Parameters.Interface(&queryParameters)
	if err != nil {
		return message.Fail("parameter validation:" + err.Error())
	}

	query, err := queryParameters.BuildDeleteQuery()
	if err != nil {
		return message.Fail("query_parameter.BuildDeleteQuery: " + err.Error())
	}

	result, err := db.Connection.Exec(query, queryParameters.Arguments...)
	if err != nil {
		return message.Fail("db.Connection.Exec: " + err.Error())
	}

	affected, err := result.RowsAffected()
	if err != nil {
		return message.Fail("result.RowsAffected: " + err.Error())
	}

	if affected == 0 {
		return message.Fail("no rows were deleted")
	}
	reply := handler.DeleteReply{}
	replyMessage, err := command.Reply(&reply)
	if err != nil {
		return message.Fail("command.Reply: " + err.Error())
	}

	return replyMessage
}

// Execute the insert
var onInsert = func(request message.Request, _ log.Logger, _ remote.Clients) message.Reply {
	if db == nil || db.Connection == nil {
		return message.Fail("database.Connection is nil, please open the connection first")
	}

	//parameters []interface{}, outputs []interface{}
	var queryParameters handler.DatabaseQueryRequest
	err := request.Parameters.Interface(&queryParameters)
	if err != nil {
		return message.Fail("parameter validation:" + err.Error())
	}

	err = queryParameters.DeserializeBytes()
	if err != nil {
		return message.Fail("serialization failed: %w" + err.Error())
	}

	query, err := queryParameters.BuildInsertRowQuery()
	if err != nil {
		return message.Fail("query_parameter.BuildInsertRowQuery: " + err.Error())
	}

	result, err := db.Connection.Exec(query, queryParameters.Arguments...)
	if err != nil {
		return message.Fail("db.Connection.Exec: " + err.Error())
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return message.Fail("result.RowsAffected: " + err.Error())
	}

	if affected == 0 {
		return message.Fail("no rows were inserted or updated")
	}
	reply := handler.InsertReply{}
	replyMessage, err := command.Reply(&reply)
	if err != nil {
		return message.Fail("command.Reply: " + err.Error())
	}

	return replyMessage
}

// Execute the row update
var onUpdate = func(request message.Request, _ log.Logger, _ remote.Clients) message.Reply {
	if db == nil || db.Connection == nil {
		return message.Fail("database.Connection is nil, please open the connection first")
	}

	//parameters []interface{}, outputs []interface{}
	var queryParameters handler.DatabaseQueryRequest
	err := request.Parameters.Interface(&queryParameters)
	if err != nil {
		return message.Fail("parameter validation:" + err.Error())
	}

	err = queryParameters.DeserializeBytes()
	if err != nil {
		return message.Fail("serialization failed: %w" + err.Error())
	}

	query, err := queryParameters.BuildUpdateQuery()
	if err != nil {
		return message.Fail("query_parameter.BuildUpdateQuery: " + err.Error())
	}

	result, err := db.Connection.Exec(query, queryParameters.Arguments...)
	if err != nil {
		return message.Fail("db.Connection.Exec: " + err.Error())
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return message.Fail("result.RowsAffected: " + err.Error())
	}

	if affected == 0 {
		return message.Fail("no rows were inserted or updated")
	}
	reply := handler.UpdateReply{}
	replyMessage, err := command.Reply(&reply)
	if err != nil {
		return message.Fail("command.Reply: " + err.Error())
	}

	return replyMessage
}
