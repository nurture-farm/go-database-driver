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

package go_database_driver

import (
	"errors"
	"github.com/nurture-farm/go-database-driver/driverConfig"
	"github.com/nurture-farm/go-database-driver/executor"
)

type DatabaseClient int

const (
	ATHENA DatabaseClient = iota
	MYSQL
	PRESTO
	TRINO
)

func InitializeDrivers(client DatabaseClient, config interface{}) error {

	switch client {
	case ATHENA:
		if athenaConfig, ok := config.(*driverConfig.AthenaConfig); ok {
			return executor.InitializeAthenaClient(athenaConfig)
		} else {
			return errors.New("Invalid config for Athena")
		}
	case TRINO:
		if trinoConfig, ok := config.(*driverConfig.TrinoConfig); ok {
			return executor.InitializeTrinoClient(trinoConfig)
		} else {
			return errors.New("Invalid config for Trino")
		}
	case PRESTO:
		if prestoConfig, ok := config.(*driverConfig.PrestoConfig); ok {
			return executor.InitializePrestoClient(prestoConfig)
		} else {
			return errors.New("Invalid config for Presto")
		}
	case MYSQL:
		if mysqlConfig, ok := config.(*driverConfig.MysqlConfig); ok {
			return executor.InitializeMysqlClient(mysqlConfig)
		} else {
			return errors.New("Invalid config for Mysql")
		}
	}

	return nil
}

func ExecuteQuery(query string, databaseClient DatabaseClient) (map[string]string, [][]interface{}, []string, error) {
	switch databaseClient {
	case ATHENA:
		return executor.AthenaExecutorInstance.Execute(query)
	case MYSQL:
		return executor.MysqlExecutorInstance.Execute(query)
	case PRESTO:
		return executor.PrestoExecutorInstance.Execute(query)
	case TRINO:
		return executor.TrinoExecutorInstance.Execute(query)
	}

	return nil, nil, nil, nil
}

//func main() {
//	fmt.Println("inside main function")
//}
