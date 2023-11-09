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

import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.DdlException;
import com.starrocks.common.UserException;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.CreatePolicyStmt;
import com.starrocks.sql.ast.UserIdentity;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * @ClassName PolicyManager
 * @Author guanxianchun
 * @Description
 * @Date 2023/11/7 下午10:26
 */
public class PolicyManager implements Writable {
    private static final Logger LOG = LogManager.getLogger(PolicyManager.class);

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

    @SerializedName("dbToPolicyMap")
    private final Map<Long, Set<Policy>> dbToPolicyMap = Maps.newConcurrentMap();
    /**
     * cache policy for match
     * key: tableId-type
     */
    private final Map<String, Set<Policy>> tablePolicyMap = Maps.newConcurrentMap();
    /**
     * cache policy for search
     * key: user-type-tableId
     */
    private final Map<String, Policy> userTableToPolicyMap = Maps.newConcurrentMap();
    /**
     * key: user-type
     */
    private final Set<String> userPolicySet = Sets.newConcurrentHashSet();

    private void writeLock() {
        lock.writeLock().lock();
    }

    private void writeUnlock() {
        lock.writeLock().unlock();
    }

    private void readLock() {
        lock.readLock().lock();
    }

    private void readUnlock() {
        lock.readLock().unlock();
    }

    public void createPolicy(CreatePolicyStmt stmt) throws UserException {
        Policy policy = Policy.fromCreateStmt(stmt);
        writeLock();
        try {
            if (existPolicy(policy.getDbId(), policy.getTableId(), policy.getType(),
                    policy.getPolicyName(), policy.getUser())) {
                if (!stmt.isOverwrite()) {
                    throw new DdlException("the policy " + policy.getPolicyName() + " already create");
                }
            }
            unprotectedAdd(policy);
            GlobalStateMgr.getCurrentState().getEditLog().logCreatePolicy(policy);
        } finally {
            writeUnlock();
        }

    }

    private boolean existPolicy(long dbId, long tableId, PolicyType type, String policyName, UserIdentity user) {
        Set<Policy> policies = getDbPolicies(dbId);
        return policies.stream().anyMatch(policy -> matchPolicy(policy, tableId, type, policyName, user));
    }

    private boolean matchPolicy(Policy policy, long tableId, PolicyType type, String policyName, UserIdentity user) {
        return Objects.equals(policy.getTableId(), tableId)
                && policy.getType().equals(type)
                && StringUtils.equals(policy.getPolicyName(), policyName)
                && StringUtils.equals(policy.getUser().getUser(), user.getUser());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    /**
     * Read policyMgr from file.
     **/
    public static PolicyManager read(DataInput in) throws IOException {
        String json = Text.readString(in);
        PolicyManager policyManager = GsonUtils.GSON.fromJson(json, PolicyManager.class);
        // update merge policy cache and userPolicySet
        policyManager.updateMergeTablePolicyMap();
        return policyManager;
    }

    private void updateMergeTablePolicyMap() {
        dbToPolicyMap.forEach((dbId, policies) -> updateMergePolicyMap(dbId));
    }

    public void updateMergePolicyMap(Long dbId) {
        Set<Policy> policies = dbToPolicyMap.get(dbId);
        if (CollectionUtils.isEmpty(policies)) {
            return;
        }
        policies.forEach(policy -> {
            updateTablePolicyCache(policy);
            updateUserTablePolicyCache(policy);
            updateUserPolicyCache(policy);
        });
    }

    public void updateUserTablePolicyCache(Policy policy) {
        String key = Joiner.on("-").join(policy.getUser().getUser(), policy.getType(), policy.getTableId());
        userTableToPolicyMap.put(key, policy);
    }

    /**
     * @param policy
     */
    public void updateTablePolicyCache(Policy policy) {
        if (Objects.isNull(policy)) {
            return;
        }
        String key = Joiner.on("-").join(policy.getTableId(), policy.getType());
        Set<Policy> policies = Optional.ofNullable(tablePolicyMap.get(key)).orElse(new HashSet<>());
        // 处理已经存在的情况，后添加的不能更新
        policies.remove(policy);
        policies.add(policy);
        if (!tablePolicyMap.containsKey(key)) {
            tablePolicyMap.put(key, policies);
        }
    }

    /**
     * @param policy
     */
    private void updateUserPolicyCache(Policy policy) {
        userPolicySet.add(Joiner.on("-").join(policy.getUser().getUser(), policy.getType()));
    }

    public void replayCreate(Policy policy) {
        unprotectedAdd(policy);
        LOG.info("replay create policy: {}", policy);
    }

    private void unprotectedAdd(Policy policy) {
        if (policy == null) {
            return;
        }
        long dbId = policy.getDbId();
        Set<Policy> dbPolicies = getDbPolicies(dbId);
        // 更新数据
        dbPolicies.remove(policy);
        dbPolicies.add(policy);
        // 更新缓存
        updateTablePolicyCache(policy);
    }

    public Set<Policy> getDbPolicies(long dbId) {
        return dbToPolicyMap.get(dbId);
    }

    /**
     * 获取表下某种类型的所有策略
     *
     * @param tableId
     * @param policyType
     * @return
     */
    public Set<Policy> getTablePolicy(Long tableId, PolicyType policyType) {
        String key = Joiner.on("-").join(tableId, policyType);
        return tablePolicyMap.get(key);
    }

    /**
     * 获取策略
     *
     * @param user
     * @param policyType
     * @param tableId
     * @return
     */
    public Policy getPolicy(String user, PolicyType policyType, Long tableId) {
        return userTableToPolicyMap.get(Joiner.on("-").join(user, policyType, tableId));
    }

    /**
     * 判断策略是否存在
     *
     * @param user
     * @param policyType
     * @return
     */
    public boolean hasPolicy(String user, PolicyType policyType) {
        return userPolicySet.contains(Joiner.on("-").join(user, policyType));
    }

    /**
     * 删除数据库时，清空策略和缓存
     *
     * @param dbId
     */
    public void dropPolicy(Long dbId) {
        Set<Policy> policies = dbToPolicyMap.get(dbId);
        if (CollectionUtils.isEmpty(policies)) {
            return;
        }

        dbToPolicyMap.remove(dbId);

        policies.forEach(policy -> {
            removeFromTablePolicyCache(policy);
            removeFromUserTablePolicyCache(policy);
            removeFromUserPolicyCache(policy);
        });
    }

    /**
     * 删除表时，清空策略和缓存
     *
     * @param dbId
     * @param tableId
     */
    public void dropPolicy(Long dbId, Long tableId) {
        // 获取数据库所有的策略
        Set<Policy> policies = dbToPolicyMap.get(dbId);
        if (CollectionUtils.isEmpty(policies)) {
            return;
        }
        // 获取表所有的策略
        Set<Policy> deletePolicies = policies.stream()
                .filter(policy -> Objects.equals(tableId, policy.getTableId())).collect(Collectors.toSet());
        // 将表的策略从该数据库策略中删除
        policies.removeAll(deletePolicies);
        // 删除缓存
        deletePolicies.forEach(policy -> {
            removeFromTablePolicyCache(policy);
            removeFromUserTablePolicyCache(policy);
            removeFromUserPolicyCache(policy);
        });
    }

    /**
     * 将策略从tablePolicyMap缓存中移除
     *
     * @param policy
     */
    private void removeFromTablePolicyCache(Policy policy) {
        tablePolicyMap.remove(Joiner.on("-").join(policy.getTableId(), policy.getType()));
    }

    /**
     * 将策略从userTableToPolicyMap缓存中移除
     *
     * @param policy
     */
    private void removeFromUserTablePolicyCache(Policy policy) {
        userTableToPolicyMap.remove(Joiner.on("-").join(policy.getUser().getUser(),
                policy.getType(), policy.getTableId()));
    }

    /**
     * 将策略从userPolicySet缓存中移除
     *
     * @param policy
     */
    private void removeFromUserPolicyCache(Policy policy) {
        userPolicySet.remove(Joiner.on("-").join(policy.getUser(), policy.getType()));
    }
}
