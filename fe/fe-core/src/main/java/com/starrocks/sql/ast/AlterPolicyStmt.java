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
import com.starrocks.policy.PolicyOperator;
import com.starrocks.policy.PolicyType;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;
import java.util.Map;

/**
 * @ClassName AlterPolicyStmt
 * @Author guanxianchun
 * @Description
 * @Date 2023/11/8 下午11:18
 */
public abstract class AlterPolicyStmt extends DdlStmt {
    protected String policyName;
    protected PolicyType policyType;

    protected TableName tableName;
    protected PolicyOperator operator;
    protected Map<String, FunctionCallExpr> columnMaskFunctionMap;
    protected List<String> dropColumns;
    protected String newPolicyName;

    private AlterPolicyStmt(String policyName, PolicyType policyType, TableName tableName,
                            PolicyOperator operator) {
        super(NodePosition.ZERO);
        this.policyName = policyName;
        this.policyType = policyType;
        this.tableName = tableName;
        this.operator = operator;
    }

    public AlterPolicyStmt(String policyName, PolicyType policyType, TableName tableName,
                           PolicyOperator operator, Map<String, FunctionCallExpr> columnMaskFunctionMap) {
        this(policyName, policyType, tableName, operator);
        this.columnMaskFunctionMap = columnMaskFunctionMap;
    }

    public AlterPolicyStmt(String policyName, PolicyType policyType, TableName tableName,
                           PolicyOperator operator, List<String> dropColumns) {
        this(policyName, policyType, tableName, operator);
        this.dropColumns = dropColumns;
    }

    public AlterPolicyStmt(String policyName, PolicyType policyType, TableName tableName,
                           PolicyOperator operator, String newPolicyName) {
        this(policyName, policyType, tableName, operator);
        this.newPolicyName = newPolicyName;
    }

    public String getPolicyName() {
        return policyName;
    }

    public PolicyType getPolicyType() {
        return policyType;
    }

    public TableName getTableName() {
        return tableName;
    }

    public PolicyOperator getOperator() {
        return operator;
    }

    public Map<String, FunctionCallExpr> getColumnMaskFunctionMap() {
        return columnMaskFunctionMap;
    }

    public List<String> getDropColumns() {
        return dropColumns;
    }

    public String getNewPolicyName() {
        return newPolicyName;
    }

    @Override
    public String toSql() {
        return "";
    }
}
