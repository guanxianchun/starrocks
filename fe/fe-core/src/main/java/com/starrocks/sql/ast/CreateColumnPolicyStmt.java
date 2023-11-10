// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.sql.ast;

import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.UserException;
import com.starrocks.policy.Policy;
import com.starrocks.policy.PolicyType;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.parser.NodePosition;

import java.util.Map;
import java.util.Optional;

/**
 * @ClassName CreateColumnPolicyStmt
 * @Author guanxianchun
 * @Description
 * @Date 2023/11/7 下午11:42
 */
public final class CreateColumnPolicyStmt extends CreatePolicyStmt {
    private final Map<String, FunctionCallExpr> columnMaskFunctionMap;

    public CreateColumnPolicyStmt(String policyName, boolean overwrite, TableName tableName, UserIdentity user,
                                  Map<String, FunctionCallExpr> columnMaskFunctionMap) {
        super(NodePosition.ZERO, policyName, PolicyType.COLUMN, overwrite, tableName, user);
        this.columnMaskFunctionMap = columnMaskFunctionMap;
    }

    @Override
    public Policy createPolicy() throws UserException {
        Database database = Optional.ofNullable(GlobalStateMgr.getCurrentState().getDb(tableName.getDb()))
                .orElseThrow(() -> new UserException(ErrorCode.ERR_BAD_DB_ERROR.formatErrorMsg(tableName.getDb())));
        Table table = Optional.ofNullable(database.getTable(tableName.getTbl()))
                .orElseThrow(() -> new ArithmeticException(ErrorCode.ERR_BAD_TABLE_ERROR.formatErrorMsg(tableName.getTbl())));
        user.analyze();
        return new Policy(database.getId(), table.getId(), PolicyType.COLUMN, policyName, user,
                columnMaskFunctionMap, origStmt.originStmt);
    }

    public Map<String, FunctionCallExpr> getColumnMaskFunctionMap() {
        return columnMaskFunctionMap;
    }
}
