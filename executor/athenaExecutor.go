/*
 *  Copyright 2023 NURTURE AGTECH PVT LTD
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package executor

import (
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/athena"
	"github.com/nurture-farm/go-database-driver/driverConfig"
	"go.uber.org/zap"
	"strings"
	"sync"
	"time"
)

const (
	CONST_ATHENA_QUERY_SLEEP_DURATION = 2 // in seconds
	CONST_ATHENA_QUERY_SUCCESS        = "SUCCEEDED"
	CONST_ATHENA_QUERY_FAILED         = "FAILED"
	CONST_NIL                         = "NIL"
	CONST_DATABASE_REWARDS_GATEWAY    = "rewards_gateway"
)

type athenaExecutor struct {
}

var (
	AthenaExecutorInstance *athenaExecutor
	athenaClientLock       sync.Mutex
	AtheClient             *athena.Athena
)

func InitializeAthenaClient(config *driverConfig.AthenaConfig) error {

	if AtheClient == nil {
		athenaClientLock.Lock()
		defer athenaClientLock.Unlock()
		if AtheClient == nil {
			AthenaExecutorInstance = &athenaExecutor{}
			if len(config.AwsAccessKey) > 0 && len(config.AwsSecretKey) > 0 && len(config.AwsRegion) > 0 {
				awscfg := &aws.Config{
					Region:      &config.AwsRegion,
					Credentials: credentials.NewStaticCredentials(config.AwsAccessKey, config.AwsSecretKey, ""),
				}
				sess := session.Must(session.NewSession(awscfg))
				AtheClient = athena.New(sess, awscfg)
			}
		}
	}

	return nil
}

func (ae *athenaExecutor) Execute(query string) (map[string]string, [][]interface{}, []string, error) {
	logger.Info("Got request for executing query for Athena ", zap.Any("query", query))
	if AtheClient == nil {
		return nil, nil, nil, errors.New("AthenaClient is not initialized. Please use InitializeDrivers function for intializing client")
	}
	athenaResponse, err := RunAthenaQuery(query)

	if err != nil {
		logger.Error("Error in Running AthenaQuery", zap.Error(err))
		return nil, nil, nil, err
	}

	columnMap, rowData, colNames := MapAthenaResponse(athenaResponse)
	return columnMap, rowData, colNames, nil
}

func RunAthenaQuery(query string) (*athena.GetQueryResultsOutput, error) {
	database := CONST_DATABASE_REWARDS_GATEWAY
	logger.Info("Executing Athena query", zap.Any("query", query))
	var s athena.StartQueryExecutionInput
	s.QueryString = &query

	var q athena.QueryExecutionContext
	q.SetDatabase(database)
	s.SetQueryExecutionContext(&q)

	result, err := AtheClient.StartQueryExecution(&s)
	if err != nil {
		logger.Error("Error in StartQueryExecution for Athena query", zap.Error(err), zap.Any("query", query))
		return nil, err
	}

	var qri athena.GetQueryExecutionInput
	qri.SetQueryExecutionId(*result.QueryExecutionId)

	var qrop *athena.GetQueryExecutionOutput
	duration := time.Duration(CONST_ATHENA_QUERY_SLEEP_DURATION) * time.Second

	for {
		qrop, err = AtheClient.GetQueryExecution(&qri)
		if err != nil {
			logger.Error("Error in GetQueryExecution for Athena query", zap.Error(err), zap.Any("query", query))
			return nil, err
		}
		if *qrop.QueryExecution.Status.State == CONST_ATHENA_QUERY_SUCCESS {
			break
		}
		if *qrop.QueryExecution.Status.State == CONST_ATHENA_QUERY_FAILED {
			err = fmt.Errorf("ATHENA_QUERY_FAILED")
			logger.Error("Athena query execution failed", zap.Any("query", query))
			return nil, err
		}
		logger.Info("Waiting for Athena query execution to finish", zap.Any("query", query), zap.Any("state", *qrop.QueryExecution.Status.State))
		time.Sleep(duration)
	}

	logger.Info("Athena query execution completed", zap.Any("query", query))
	var ip athena.GetQueryResultsInput
	ip.SetQueryExecutionId(*result.QueryExecutionId)
	logger.Info("Getting Athena query result", zap.Any("query", query))
	op, err := AtheClient.GetQueryResults(&ip)
	if err != nil {
		logger.Error("Error in GetQueryResults for Athena query", zap.Error(err), zap.Any("query", query))
		return nil, err
	}
	logger.Info("Athena query result success", zap.Any("query", query))
	nextToken := op.NextToken
	for nextToken != nil {
		logger.Info("Proccessing Athena query result from next Token")
		queryResults, _ := AtheClient.GetQueryResults(&athena.GetQueryResultsInput{
			NextToken:        nextToken,
			QueryExecutionId: result.QueryExecutionId,
		})
		nextToken = queryResults.NextToken
		op.ResultSet.Rows = append(op.ResultSet.Rows, queryResults.ResultSet.Rows...)
		logger.Info("Total Number of rows received ", zap.Any("Rows", len(op.ResultSet.Rows)))
	}
	logger.Info("Total number of rows returned in query ", zap.Any("Rows", len(op.ResultSet.Rows)-1))
	logger.Info("Athena query execution successful!", zap.Any("query", query))
	return op, nil
}

func MapAthenaResponse(response *athena.GetQueryResultsOutput) (map[string]string, [][]interface{}, []string) {

	columnMap := make(map[string]string)
	rowsData := make([][]interface{}, 0)
	colNames := []string{}
	dataStartIndex := 0
	for _, columnInfo := range response.ResultSet.ResultSetMetadata.ColumnInfo {
		name := *columnInfo.Name
		if name != "" && strings.HasPrefix(name, "_") {
			dataStartIndex++
			continue
		}
		columnMap[name] = *columnInfo.Type
		colNames = append(colNames, name)
	}
	for _, row := range response.ResultSet.Rows {
		rowData := make([]interface{}, 0)
		for index, data := range row.Data {
			value := data.VarCharValue
			if index >= dataStartIndex {
				if value != nil {
					rowData = append(rowData, *data.VarCharValue)
				} else {
					rowData = append(rowData, CONST_NIL)
				}
			}
		}
		rowsData = append(rowsData, rowData)
	}
	return columnMap, rowsData, colNames
}
