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
	"database/sql"
	"errors"
	_ "github.com/go-sql-driver/mysql"
	"github.com/nurture-farm/go-database-driver/driverConfig"
	"go.uber.org/zap"
	"sync"
)

type mysqlExecutor struct {
}

var (
	MysqlExecutorInstance *mysqlExecutor
	mysqlClientLock       sync.Mutex
	MysqlClient           *sql.DB
)

const READ_REPLICA_DRIVER_INDEX = 1

func InitializeMysqlClient(config *driverConfig.MysqlConfig) error {

	if MysqlClient == nil {
		mysqlClientLock.Lock()
		defer mysqlClientLock.Unlock()
		if MysqlClient == nil {
			var err error
			MysqlExecutorInstance = &mysqlExecutor{}
			MysqlClient, err = sql.Open("presto", config.Dsn)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (pe *mysqlExecutor) Execute(query string) (map[string]string, [][]interface{}, []string, error) {
	logger.Info("Got request for executing query for Mysql ", zap.Any("query", query))
	if MysqlClient == nil {
		return nil, nil, nil, errors.New("MysqlClient is not initialized. Please use InitializeDrivers function for intializing client")
	}
	rows, err := MysqlClient.Query(query)
	if err != nil {
		logger.Error("Error in Running Query on Mysql", zap.Error(err))
		return nil, nil, nil, err
	}
	return GetQueryResponse(rows)
}
