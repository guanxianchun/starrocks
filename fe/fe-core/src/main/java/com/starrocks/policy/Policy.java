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

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.UserException;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.sql.ast.CreatePolicyStmt;
import com.starrocks.sql.ast.UserIdentity;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.IOException;
import java.util.Objects;

/**
 * @ClassName Policy
 * @Author guanxianchun
 * @Description
 * @Date 2023/11/7 下午10:26
 */
public abstract class Policy implements Writable, GsonPostProcessable {
    private static final Logger LOG = LogManager.getLogger(Policy.class);

    @SerializedName(value = "dbId")
    protected Long dbId;
    @SerializedName(value = "tableId")
    protected Long tableId;
    @SerializedName(value = "type")
    protected PolicyType type = PolicyType.COLUMN;
    @SerializedName(value = "policyName")
    protected String policyName = null;
    @SerializedName(value = "user")
    protected UserIdentity user;
    @SerializedName(value = "enabled")
    protected boolean enabled = true;
    protected String originStmt;

    /**
     * Base class for Policy.
     *
     * @param dbId
     * @param tableId
     * @param type
     * @param policyName
     * @param user
     */
    public Policy(Long dbId, Long tableId, PolicyType type, String policyName, UserIdentity user, String originStmt) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.type = type;
        this.policyName = policyName;
        this.user = user;
        this.originStmt = originStmt;
    }

    public static Policy fromCreateStmt(CreatePolicyStmt stmt) throws UserException {
        return stmt.createPolicy();
    }

    public Long getDbId() {
        return dbId;
    }

    public Long getTableId() {
        return tableId;
    }

    public PolicyType getType() {
        return type;
    }

    public String getPolicyName() {
        return policyName;
    }

    public UserIdentity getUser() {
        return user;
    }

    public boolean isEnabled() {
        return enabled;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Policy policy = (Policy) o;
        return Objects.equals(dbId, policy.dbId) && Objects.equals(tableId, policy.tableId)
                && type == policy.type && Objects.equals(policyName, policy.policyName)
                && Objects.equals(user.getUser(), policy.user.getUser());
    }

    @Override
    public int hashCode() {
        return Objects.hash(dbId, tableId, type, policyName, user.getUser());
    }

    public static Policy read(DataInput in, PolicyType policyType) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, policyType == PolicyType.COLUMN ? ColumnPolicy.class : Policy.class);
    }
}
