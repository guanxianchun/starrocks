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
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.Pair;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AlterPolicyStmt;
import com.starrocks.sql.ast.CreatePolicyStmt;
import com.starrocks.sql.ast.DropPolicyStmt;
import org.apache.commons.lang3.StringUtils;

import java.util.Optional;

/**
 * @ClassName PolicyStmtAnalyzer
 * @Author guanxianchun
 * @Description
 * @Date 2023/11/8 下午11:04
 */
public class PolicyStmtAnalyzer {

    public static void analyze(CreatePolicyStmt statement, ConnectContext context) {
        TableName tableName = statement.getTableName();
        setTableName(tableName, context);
        Pair<Database, Table> dbTable = checkAndGetDatabaseTable(tableName, context);
        // 检查策略是否存在
        if (!statement.isOverwrite() && GlobalStateMgr.getCurrentState().getPolicyManager().existPolicy(dbTable.first.getId(),
                dbTable.second.getId(), statement.getPolicyType(), statement.getPolicyName(), statement.getUser())) {
            throw new SemanticException(ErrorCode.ERR_POLICY_EXISTS.formatErrorMsg(statement.getPolicyName()));
        }

    }

    public static void analyze(DropPolicyStmt statement, ConnectContext context) {
        setTableName(statement.getTableName(), context);
    }

    public static void analyze(AlterPolicyStmt statement, ConnectContext context) {
        TableName tableName = statement.getTableName();
        setTableName(tableName, context);
        Pair<Database, Table> dbTable = checkAndGetDatabaseTable(tableName, context);
        if (!GlobalStateMgr.getCurrentState().getPolicyManager().existPolicy(dbTable.first.getId(),
                dbTable.second.getId(), statement.getPolicyType(), statement.getPolicyName(), null)) {
            throw new SemanticException(ErrorCode.ERR_POLICY_NOT_EXISTS.formatErrorMsg(statement.getPolicyName()));
        }
    }

    /**
     * 检查数据库和表是否存在，存在则返回数据库和表
     *
     * @param tableName
     * @param context
     * @return
     */
    private static Pair<Database, Table> checkAndGetDatabaseTable(TableName tableName, ConnectContext context) {

        // 检查数据库/表是否存在
        Database database = Optional.ofNullable(GlobalStateMgr.getCurrentState().getDb(tableName.getDb()))
                .orElseThrow(() -> new SemanticException(ErrorCode.ERR_BAD_DB_ERROR.formatErrorMsg(tableName.getDb())));
        Table table = Optional.ofNullable(database.getTable(tableName.getTbl()))
                .orElseThrow(() -> new SemanticException(ErrorCode.ERR_BAD_TABLE_ERROR.formatErrorMsg(tableName.getTbl())));
        return new Pair<>(database, table);
    }

    private static void setTableName(TableName tableName, ConnectContext context) {
        if (StringUtils.isBlank(tableName.getCatalogAndDb())) {
            tableName.setCatalog(context.getCurrentCatalog());
            tableName.setDb(context.getDatabase());
        }
    }
}
