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

package com.starrocks.policy;

import com.google.common.collect.Maps;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.common.io.Text;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.sql.ast.CreateColumnPolicyStmt;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.sql.parser.SqlParser;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

/**
 * @ClassName ColumnPolicy
 * @Author guanxianchun
 * @Description
 * @Date 2023/11/7 下午10:28
 */
public class ColumnPolicy extends Policy {
    private Map<String, FunctionCallExpr> columnMaskFunctionMap;

    public ColumnPolicy(Long dbId, Long tableId, PolicyType type, String policyName, UserIdentity user,
                        Map<String, FunctionCallExpr> columnMaskFunctionMap, String originStmt) {
        super(dbId, tableId, type, policyName, user, originStmt);
        this.columnMaskFunctionMap = columnMaskFunctionMap;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    @Override
    public void gsonPostProcess() throws IOException {
        if (columnMaskFunctionMap == null) {
            SessionVariable sessionVariable = new SessionVariable();
            sessionVariable.setSqlMode(SqlModeHelper.MODE_DEFAULT);
            CreateColumnPolicyStmt stmt = (CreateColumnPolicyStmt) SqlParser.parse(originStmt, sessionVariable).get(0);
            columnMaskFunctionMap = stmt.getColumnMaskFunctionMap();
        }
    }

    @Override
    protected ColumnPolicy clone() throws CloneNotSupportedException {
        // clone all column FunctionCallExpr
        Map<String, FunctionCallExpr> clone = Maps.transformEntries(columnMaskFunctionMap,
                new Maps.EntryTransformer<String, FunctionCallExpr, FunctionCallExpr>() {
                    @Override
                    public FunctionCallExpr transformEntry(String key, FunctionCallExpr value) {
                        return (FunctionCallExpr) value.clone();
                    }
                });
        return new ColumnPolicy(dbId, tableId, type, policyName,
                new UserIdentity(user.getUser(), user.getHost(), user.isDomain()), clone, originStmt);
    }

    public Map<String, FunctionCallExpr> getColumnMaskFunctionMap() {
        return columnMaskFunctionMap;
    }
}
