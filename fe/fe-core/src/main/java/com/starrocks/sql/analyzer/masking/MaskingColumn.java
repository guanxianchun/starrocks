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

package com.starrocks.sql.analyzer.masking;

import java.util.Objects;

/**
 * @ClassName PhysicalMaskingColumn
 * @Author guanxianchun
 * @Description
 * @Date 2023/11/7 下午10:10
 */
public class MaskingColumn {
    private String database;
    private String tableName;
    private String name;

    public MaskingColumn(String database, String tableName, String name) {
        this.database = database;
        this.tableName = tableName;
        this.name = name;
    }

    public String getDatabase() {
        return database;
    }

    public MaskingColumn setDatabase(String database) {
        this.database = database;
        return this;
    }

    public String getTableName() {
        return tableName;
    }

    public MaskingColumn setTableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    public String getName() {
        return name;
    }

    public MaskingColumn setName(String name) {
        this.name = name;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MaskingColumn that = (MaskingColumn) o;
        return Objects.equals(database, that.database)
                && Objects.equals(tableName, that.tableName)
                && Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(database, tableName, name);
    }
}
