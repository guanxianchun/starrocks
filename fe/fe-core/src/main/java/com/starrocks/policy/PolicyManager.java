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
import com.starrocks.common.io.Writable;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.CreatePolicyStmt;
import com.starrocks.sql.ast.UserIdentity;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
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
    private final Map<Long, DataBasePolicy> dataBaseToPolicyMap = Maps.newConcurrentMap();
    /**
     * cache policy for match
     * key: tableId-type
     */
    private final Map<String, Set<Policy>> tablePolicyCache = Maps.newConcurrentMap();
    /**
     * cache policy for search
     * key: user-type-tableId
     */
    private final Map<String, Policy> userTablePolicyCache = Maps.newConcurrentMap();
    /**
     * key: user-type
     */
    private final Set<String> userPolicyCache = Sets.newConcurrentHashSet();

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
            if (existPolicy(policy.getDbId(), policy.getTableId(), policy.getPolicyType(),
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

    public boolean existPolicy(long dbId, long tableId, PolicyType policyType, String policyName, UserIdentity user) {
        DataBasePolicy dataBasePolicy = getDbPolicies(dbId);
        return dataBasePolicy != null && dataBasePolicy.existPolicy(tableId, policyType, policyName, user);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        int numbers = dataBaseToPolicyMap.size();
        out.writeInt(numbers);
        for (Map.Entry<Long, DataBasePolicy> entry : dataBaseToPolicyMap.entrySet()) {
            entry.getValue().write(out);
        }
        LOG.info("write all policy success.");
    }

    /**
     * Read policyMgr from file.
     **/
    public static PolicyManager read(DataInput in) throws IOException {
        int numbers = in.readInt();
        PolicyManager policyManager = new PolicyManager();
        try {
            for (int i = 0; i < numbers; i++) {
                DataBasePolicy dataBasePolicy = DataBasePolicy.read(in);
                policyManager.dataBaseToPolicyMap.put(dataBasePolicy.getDbId(), dataBasePolicy);
            }
            // update merge policy cache and userPolicySet
            policyManager.updatePolicyCache();
            LOG.info("load all policy success.");
        } catch (IOException e) {
            LOG.warn("load all policy failed.", e);
        }
        return policyManager;
    }

    private void updatePolicyCache() {
        dataBaseToPolicyMap.forEach((dbId, policies) -> updatePolicyCache(dbId));
    }

    public void updatePolicyCache(Long dbId) {
        readLock();
        try {
            DataBasePolicy dataBasePolicy = dataBaseToPolicyMap.get(dbId);
            if (Objects.isNull(dataBasePolicy)) {
                return;
            }
            dataBasePolicy.getPolicies().forEach(policy -> {
                updateTablePolicyCache(policy);
                updateUserTablePolicyCache(policy);
                updateUserPolicyCache(policy);
            });
        } finally {
            readUnlock();
        }
    }

    public void updateUserTablePolicyCache(Policy policy) {
        String key = Joiner.on("-").join(policy.getUser().getUser(), policy.getPolicyType(), policy.getTableId());
        userTablePolicyCache.put(key, policy);
    }

    /**
     * @param policy
     */
    public void updateTablePolicyCache(Policy policy) {
        if (Objects.isNull(policy)) {
            return;
        }
        String key = Joiner.on("-").join(policy.getTableId(), policy.getPolicyType());
        Set<Policy> policies = Optional.ofNullable(tablePolicyCache.get(key)).orElse(new HashSet<>());
        // 处理已经存在的情况，后添加的不能更新
        policies.remove(policy);
        policies.add(policy);
        if (!tablePolicyCache.containsKey(key)) {
            tablePolicyCache.put(key, policies);
        }
    }

    /**
     * @param policy
     */
    private void updateUserPolicyCache(Policy policy) {
        userPolicyCache.add(Joiner.on("-").join(policy.getUser().getUser(), policy.getPolicyType()));
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
        DataBasePolicy dataBasePolicy = getDbPolicies(dbId);

        if (dataBasePolicy == null) {
            dataBasePolicy = new DataBasePolicy(policy.getDbId());
            dataBaseToPolicyMap.put(policy.getDbId(), dataBasePolicy);
        }
        // 更新数据
        dataBasePolicy.addPolicy(policy);
        // 更新缓存
        updateTablePolicyCache(policy);
    }

    public DataBasePolicy getDbPolicies(long dbId) {
        return dataBaseToPolicyMap.get(dbId);
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
        return tablePolicyCache.get(key);
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
        return userTablePolicyCache.get(Joiner.on("-").join(user, policyType, tableId));
    }

    /**
     * 判断策略是否存在
     *
     * @param user
     * @param policyType
     * @return
     */
    public boolean hasPolicy(String user, PolicyType policyType) {
        return userPolicyCache.contains(Joiner.on("-").join(user, policyType));
    }

    /**
     * 删除数据库时，清空策略和缓存
     *
     * @param dbId
     */
    public synchronized void dropPolicy(Long dbId) {
        DataBasePolicy dataBasePolicy = dataBaseToPolicyMap.get(dbId);
        if (dataBasePolicy == null) {
            return;
        }
        dataBaseToPolicyMap.remove(dbId);
        dataBasePolicy.getPolicies().forEach(policy -> {
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
    public synchronized void dropPolicy(Long dbId, Long tableId) {
        // 获取数据库所有的策略
        DataBasePolicy dataBasePolicy = dataBaseToPolicyMap.get(dbId);
        if (dataBasePolicy == null) {
            return;
        }
        // 获取表所有的策略
        Set<Policy> deletePolicies = dataBasePolicy.getPolicies().stream()
                .filter(policy -> Objects.equals(tableId, policy.getTableId())).collect(Collectors.toSet());
        // 将表的策略从该数据库策略中删除
        dataBasePolicy.removeAll(deletePolicies);
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
        tablePolicyCache.remove(Joiner.on("-").join(policy.getTableId(), policy.getPolicyType()));
    }

    /**
     * 将策略从userTableToPolicyMap缓存中移除
     *
     * @param policy
     */
    private void removeFromUserTablePolicyCache(Policy policy) {
        userTablePolicyCache.remove(Joiner.on("-").join(policy.getUser().getUser(),
                policy.getPolicyType(), policy.getTableId()));
    }

    /**
     * 将策略从userPolicySet缓存中移除
     *
     * @param policy
     */
    private void removeFromUserPolicyCache(Policy policy) {
        userPolicyCache.remove(Joiner.on("-").join(policy.getUser(), policy.getPolicyType()));
    }

    public synchronized long savePolicies(DataOutputStream dos, long checksum) throws IOException {
        int numbers = dataBaseToPolicyMap.size();
        checksum ^= numbers;
        write(dos);
        return checksum;
    }

    public void save(DataOutputStream dos) throws IOException {
        write(dos);
        LOG.info("save PolicyManager success to image.");
    }

    public void load(SRMetaBlockReader reader) throws SRMetaBlockEOFException, IOException, SRMetaBlockException {
        try {
            int numbers = reader.readInt();
            for (int i = 0; i < numbers; i++) {
                DataBasePolicy dataBasePolicy = DataBasePolicy.load(reader);
                this.dataBaseToPolicyMap.put(dataBasePolicy.getDbId(), dataBasePolicy);
                LOG.info("load db policy success, db: {}, policies: {}", dataBasePolicy.getDbId(),
                        dataBasePolicy.getPolicies().size());
            }
            LOG.info("load PolicyManager success from image.");
        } catch (Exception e) {
            LOG.warn("load PolicyManager failed from image", e);
        }
    }

    public long loadPolicies(DataInputStream dis, long checksum) throws IOException {
        try {
            int numbers = dis.readInt();
            checksum ^= numbers;
            for (int i = 0; i < numbers; i++) {
                DataBasePolicy dataBasePolicy = DataBasePolicy.loadPolicies(dis);
                this.dataBaseToPolicyMap.put(dataBasePolicy.getDbId(), dataBasePolicy);
                LOG.info("load db policy success, db: {}, policies: {}", dataBasePolicy.getDbId(),
                        dataBasePolicy.getPolicies().size());
            }
            LOG.info("load PolicyManager success from image.");
        } catch (IOException e) {
            LOG.warn("load PolicyManager failed from image", e);
        }
        return checksum;
    }
}
