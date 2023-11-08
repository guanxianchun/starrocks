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
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.sql.ast.CreatePolicyStmt;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @ClassName Policy
 * @Author guanxianchun
 * @Description
 * @Date 2023/11/7 下午10:26
 */
public abstract class Policy implements Writable, GsonPostProcessable {
    private static final Logger LOG = LogManager.getLogger(Policy.class);

    @SerializedName(value = "id")
    protected long id = -1;

    @SerializedName(value = "type")
    protected PolicyType type = null;

    @SerializedName(value = "policyName")
    protected String policyName = null;

    @SerializedName(value = "version")
    protected long version = -1;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

    public void writeLock() {
        lock.writeLock().lock();
    }

    public void writeUnlock() {
        lock.writeLock().unlock();
    }

    public void readLock() {
        lock.readLock().lock();
    }

    public void readUnlock() {
        lock.readLock().unlock();
    }

    // just for subclass lombok @Data
    public Policy() {
    }

    public Policy(PolicyType type) {
        this.type = type;
    }

    /**
     * Base class for Policy.
     *
     * @param type       policy type
     * @param policyName policy name
     */
    public Policy(long id, final PolicyType type, final String policyName) {
        this.id = id;
        this.type = type;
        this.policyName = policyName;
        this.version = 0;
    }

    public static Policy fromCreateStmt(CreatePolicyStmt stmt) {

        return null;
    }
}
