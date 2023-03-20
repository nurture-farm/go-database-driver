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
	"go.uber.org/zap"
)

func GetQueryResponse(rows *sql.Rows) (map[string]string, [][]interface{}, []string, error) {
	columnMap := make(map[string]string)
	colsType, _ := rows.ColumnTypes()
	cols, _ := rows.Columns()
	rowsData := make([][]interface{}, 0)
	colNames := []string{}
	for i := range cols {
		columnMap[cols[i]] = colsType[i].DatabaseTypeName()
		colNames = append(colNames, cols[i])
	}

	for rows.Next() {
		columns := make([]interface{}, len(cols))
		columnPointers := make([]interface{}, len(cols))
		for i, _ := range columns {
			columnPointers[i] = &columns[i]
		}
		if err := rows.Scan(columnPointers...); err != nil {
			logger.Error("Error in Scanning Query Result", zap.Error(err))
			return nil, nil, nil, err
		}
		rowsData = append(rowsData, columnPointers)
	}

	return columnMap, rowsData, colNames, nil
}
