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

import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.sql.ast.UserIdentity;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;
import java.util.Set;

/**
 * @ClassName DataBasePolicy
 * @Author guanxianchun
 * @Description
 * @Date 2023/11/10 下午8:28
 */
public class DataBasePolicy implements Writable {
    private static final Logger LOG = LogManager.getLogger(DataBasePolicy.class);
    @SerializedName("dbId")
    private Long dbId;
    @SerializedName("policies")
    private Set<Policy> policies = Sets.newConcurrentHashSet();

    public DataBasePolicy(Long dbId) {
        this.dbId = dbId;
    }

    public static DataBasePolicy load(SRMetaBlockReader reader) throws SRMetaBlockEOFException,
            IOException, SRMetaBlockException {
        int numbers = reader.readInt();
        DataBasePolicy dataBasePolicy = null;
        for (int i = 0; i < numbers; i++) {
            Policy policy = reader.readJson(Policy.class);
            if (dataBasePolicy == null) {
                dataBasePolicy = new DataBasePolicy(policy.getDbId());
            }
            dataBasePolicy.getPolicies().add(policy);
        }
        return dataBasePolicy;
    }

    public static DataBasePolicy loadPolicies(DataInputStream dis) throws IOException {
        return read(dis);
    }

    /**
     * @param out <code>DataOutput</code> to serialize this object into.
     * @throws IOException
     */
    @Override
    public synchronized void write(DataOutput out) throws IOException {
        int numPolicy = policies.size();
        out.writeInt(numPolicy);
        for (Policy policy : policies) {
            policy.write(out);
        }
        LOG.info("save db policy success, db: {}, policies: {}", dbId, numPolicy);
    }

    /**
     * @param in
     * @return
     * @throws Exception
     */
    public static DataBasePolicy read(DataInput in) throws IOException {
        int numPolicy = in.readInt();
        DataBasePolicy dataBasePolicy = null;
        for (int i = 0; i < numPolicy; i++) {
            Policy policy = Policy.read(in);
            if (dataBasePolicy == null) {
                dataBasePolicy = new DataBasePolicy(policy.getDbId());
            }
            dataBasePolicy.getPolicies().add(policy);
        }
        return dataBasePolicy;
    }

    public Long getDbId() {
        return dbId;
    }

    public Set<Policy> getPolicies() {
        return policies;
    }

    public void addPolicy(Policy policy) {
        policies.remove(policy);
        policies.add(policy);
    }

    public boolean existPolicy(long tableId, PolicyType policyType, String policyName, UserIdentity user) {
        return CollectionUtils.isNotEmpty(policies) && policies.stream().anyMatch(
                policy -> matchPolicy(policy, tableId, policyType, policyName, user));
    }

    private boolean matchPolicy(Policy policy, long tableId, PolicyType policyType, String policyName, UserIdentity user) {
        return Objects.equals(policy.getTableId(), tableId)
                && policy.getPolicyType().equals(policyType)
                && StringUtils.equals(policy.getPolicyName(), policyName)
                && (user == null || StringUtils.equals(policy.getUser().getUser(), user.getUser()));
    }

    public void removeAll(Set<Policy> policies) {
        this.policies.removeAll(policies);
    }
}
