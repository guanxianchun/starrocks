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

import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import com.starrocks.analysis.ParseNode;
import com.starrocks.sql.parser.NodePosition;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @ClassName AddColumnPoliciesStmt
 * @Author guanxianchun
 * @Description
 * @Date 2023/11/12 下午12:32
 */
public class AddColumnPoliciesClause implements ParseNode {
    List<AddColumnPolicyClause> policyClauses;
    Map<String, AddColumnPolicyClause> columnPolicyClauseMap;

    public AddColumnPoliciesClause(List<AddColumnPolicyClause> policyClauses) {
        this.policyClauses = policyClauses;
        if (policyClauses != null) {
            columnPolicyClauseMap = Maps.newConcurrentMap();
            policyClauses.forEach(v -> columnPolicyClauseMap.put(v.getColumnName(), v));
        }
    }

    @Override
    public String toSql() {
        if (policyClauses == null) {
            return "";
        }
        List<String> columnPolicies = new ArrayList<>();
        policyClauses.forEach(v -> columnPolicies.add(v.toSql()));
        return Joiner.on(", ").join(columnPolicies);
    }

    @Override
    public NodePosition getPos() {
        return NodePosition.ZERO;
    }

    public List<AddColumnPolicyClause> getPolicyClauses() {
        return policyClauses;
    }

    public Map<String, AddColumnPolicyClause> getColumnPolicyClauseMap() {
        return columnPolicyClauseMap;
    }

    public AddColumnPolicyClause getColumnPolicyClause(String columnName) {
        return columnPolicyClauseMap == null ? null : columnPolicyClauseMap.get(columnName);
    }
}
