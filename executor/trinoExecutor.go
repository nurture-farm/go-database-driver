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
	"crypto/tls"
	"database/sql"
	"errors"
	"github.com/nurture-farm/go-database-driver/driverConfig"
	"github.com/trinodb/trino-go-client/trino"
	"go.uber.org/zap"
	"net"
	"net/http"
	"sync"
	"time"
)

type trinoExecutor struct {
}

var (
	TrinoExecutorInstance *trinoExecutor
	trinoClientLock       sync.Mutex
	TrinoClient           *sql.DB
)

func InitializeTrinoClient(config *driverConfig.TrinoConfig) error {

	if TrinoClient == nil {
		trinoClientLock.Lock()
		defer trinoClientLock.Unlock()
		if TrinoClient == nil {
			TrinoExecutorInstance = &trinoExecutor{}
			ppsTrinoClient := &http.Client{
				Transport: &http.Transport{
					Proxy: http.ProxyFromEnvironment,
					DialContext: (&net.Dialer{
						Timeout:   30 * time.Second,
						KeepAlive: 30 * time.Second,
						DualStack: true,
					}).DialContext,
					MaxIdleConns:          100,
					IdleConnTimeout:       90 * time.Second,
					TLSHandshakeTimeout:   10 * time.Second,
					ExpectContinueTimeout: 1 * time.Second,
					TLSClientConfig:       &tls.Config{},
				},
			}
			err := trino.RegisterCustomClient("ppsTrino", ppsTrinoClient)
			if err != nil {
				return err
			}
			TrinoClient, err = sql.Open("trino", config.Dsn)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (te *trinoExecutor) Execute(query string) (map[string]string, [][]interface{}, []string, error) {
	logger.Info("Got request for executing query for Trino ", zap.Any("query", query))
	if TrinoClient == nil {
		return nil, nil, nil, errors.New("TrinoClient is not initialized. Please use InitializeDrivers function for intializing client")
	}
	rows, err := TrinoClient.Query(query)
	if err != nil {
		logger.Error("Error in Running Query on Trino", zap.Error(err))
		return nil, nil, nil, err
	}
	return GetQueryResponse(rows)
}
