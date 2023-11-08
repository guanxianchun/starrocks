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

package com.starrocks.sql.analyzer;

import com.starrocks.analysis.TableName;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AlterPolicyStmt;
import com.starrocks.sql.ast.CreatePolicyStmt;
import com.starrocks.sql.ast.DropPolicyStmt;
import org.apache.commons.lang3.StringUtils;

/**
 * @ClassName PolicyStmtAnalyzer
 * @Author guanxianchun
 * @Description
 * @Date 2023/11/8 下午11:04
 */
public class PolicyStmtAnalyzer {

    public static void analyze(CreatePolicyStmt statement, ConnectContext context) {
        setTableName(statement.getTableName(), context);
    }

    public static void analyze(DropPolicyStmt statement, ConnectContext context) {
        setTableName(statement.getTableName(), context);
    }

    public static void analyze(AlterPolicyStmt statement, ConnectContext context) {
        setTableName(statement.getTableName(), context);
    }

    private static void setTableName(TableName tableName, ConnectContext context) {
        if (StringUtils.isBlank(tableName.getCatalogAndDb())) {
            tableName.setCatalog(context.getCurrentCatalog());
            tableName.setDb(context.getDatabase());
        }
    }
}
