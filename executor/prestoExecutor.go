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
	"github.com/nurture-farm/go-database-driver/driverConfig"
	"github.com/prestodb/presto-go-client/presto"
	"go.uber.org/zap"
	"net"
	"net/http"
	"sync"
	"time"
)

type prestoExecutor struct {
}

var (
	PrestoExecutorInstance *prestoExecutor
	prestoClientLock       sync.Mutex
	PrestoClient           *sql.DB
)

func InitializePrestoClient(config *driverConfig.PrestoConfig) error {

	if PrestoClient == nil {
		prestoClientLock.Lock()
		defer prestoClientLock.Unlock()
		if PrestoClient == nil {
			PrestoExecutorInstance = &prestoExecutor{}
			ppsPrestoClient := &http.Client{
				Transport: &http.Transport{
					Proxy: http.ProxyFromEnvironment,
					DialContext: (&net.Dialer{
						Timeout:   300 * time.Second,
						KeepAlive: 30 * time.Second,
						DualStack: true,
					}).DialContext,
					MaxIdleConns:          10,
					IdleConnTimeout:       45 * time.Second,
					ExpectContinueTimeout: 0 * time.Second,
				},
			}
			err := presto.RegisterCustomClient("prestoClient", ppsPrestoClient)
			if err != nil {
				return err
			}
			PrestoClient, err = sql.Open("presto", config.Dsn)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (pe *prestoExecutor) Execute(query string) (map[string]string, [][]interface{}, []string, error) {
	logger.Info("Got request for executing query for Presto ", zap.Any("query", query))
	if PrestoClient == nil {
		return nil, nil, nil, errors.New("PrestoClient is not initialized. Please use InitializeDrivers function for intializing client")
	}
	rows, err := PrestoClient.Query(query)
	if err != nil {
		logger.Error("Error in Running Query on Presto", zap.Error(err))
		return nil, nil, nil, err
	}
	return GetQueryResponse(rows)
}
