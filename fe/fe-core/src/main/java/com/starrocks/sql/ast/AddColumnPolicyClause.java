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
import com.starrocks.analysis.ParseNode;
import com.starrocks.sql.parser.NodePosition;

/**
 * @ClassName AddColumnPolicyStmt
 * @Author guanxianchun
 * @Description
 * @Date 2023/11/12 下午12:34
 */
public class AddColumnPolicyClause implements ParseNode {
    private String columnName;
    private FunctionCallExpr functionCallExpr;

    public AddColumnPolicyClause(String columnName, FunctionCallExpr functionCallExpr) {
        this.columnName = columnName;
        this.functionCallExpr = functionCallExpr;
    }


    @Override
    public String toSql() {
        return "ADD COLUMN " + columnName + " " +
                functionCallExpr.toSqlImpl();
    }

    @Override
    public NodePosition getPos() {
        return NodePosition.ZERO;
    }

    public String getColumnName() {
        return columnName;
    }

    public FunctionCallExpr getFunctionCallExpr() {
        return functionCallExpr;
    }
}
