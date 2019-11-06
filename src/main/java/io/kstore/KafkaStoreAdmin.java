/*
 * Copyright 2015 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.kstore;

import io.kcache.Cache;
import io.kstore.schema.KafkaSchemaKey;
import io.kstore.schema.KafkaSchemaValue;
import io.kstore.schema.KafkaSchemaValue.Action;
import io.kstore.schema.TableDef;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CacheEvictionStats;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.RegionMetrics;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.CompactType;
import org.apache.hadoop.hbase.client.CompactionState;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.SnapshotDescription;
import org.apache.hadoop.hbase.client.SnapshotType;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.client.replication.TableCFs;
import org.apache.hadoop.hbase.client.security.SecurityCapability;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.quotas.QuotaFilter;
import org.apache.hadoop.hbase.quotas.QuotaRetriever;
import org.apache.hadoop.hbase.quotas.QuotaSettings;
import org.apache.hadoop.hbase.quotas.SpaceQuotaSnapshotView;
import org.apache.hadoop.hbase.regionserver.wal.FailedLogCloseException;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationPeerDescription;
import org.apache.hadoop.hbase.security.access.GetUserPermissionsRequest;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.security.access.UserPermission;
import org.apache.hadoop.hbase.snapshot.HBaseSnapshotException;
import org.apache.hadoop.hbase.snapshot.RestoreSnapshotException;
import org.apache.hadoop.hbase.snapshot.SnapshotCreationException;
import org.apache.hadoop.hbase.snapshot.UnknownSnapshotException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.regex.Pattern;

public class KafkaStoreAdmin implements Admin {

    protected final Logger LOG = LoggerFactory.getLogger(KafkaStoreAdmin.class);

    private final KafkaStoreConnection conn;

    /**
     * Kafka Store doesn't require disabling tables before deletes or schema changes. Some clients do
     * call disable first, and then check for disable before deletes or schema changes. We're keeping
     * track of that state in memory on so that those clients can proceed with the delete/schema
     * change
     */
    private final Set<TableName> disabledTables;

    public KafkaStoreAdmin(KafkaStoreConnection conn) throws IOException {
        this.conn = conn;
        this.disabledTables = conn.getDisabledTables();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Connection getConnection() {
        return conn;
    }

    public Cache<KafkaSchemaKey, KafkaSchemaValue> getSchemas() {
        return conn.getSchemas();
    }

    public Map<TableName, KafkaStoreTable> getTables() {
        return conn.getTables();
    }

    public KafkaSchemaValue getLatestSchemaValue(TableName tableName) {
        return conn.getLatestSchemaValue(tableName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean tableExists(TableName tableName) throws IOException {
        for (TableName existingTableName : listTableNames(tableName.getNameAsString())) {
            if (existingTableName.equals(tableName)) {
                return true;
            }
        }
        return false;
    }

    // Used by the Hbase shell but not defined by Admin. Will be removed once the
    // shell is switch to use the methods defined in the interface.

    /**
     * tableExists.
     *
     * @param tableName a {@link java.lang.String} object.
     * @return a boolean.
     * @throws java.io.IOException if any.
     */
    @Deprecated
    public boolean tableExists(String tableName) throws IOException {
        return tableExists(TableName.valueOf(tableName));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public HTableDescriptor[] listTables() throws IOException {
        // NOTE: We don't have systables
        return getTableDescriptors(listTableNames());
    }

    private HTableDescriptor[] getTableDescriptors(TableName[] tableNames) throws IOException {
        HTableDescriptor[] response = new HTableDescriptor[tableNames.length];
        for (int i = 0; i < tableNames.length; i++) {
            response[i] = getTableDescriptor(tableNames[i]);
        }
        return response;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<TableDescriptor> listTableDescriptors() throws IOException {
        return Arrays.asList(listTables());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<TableDescriptor> listTableDescriptors(Pattern pattern) throws IOException {
        return Arrays.asList(listTables(pattern));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<TableDescriptor> listTableDescriptors(List<TableName> tableNames) throws IOException {
        List<TableDescriptor> response = new ArrayList<>();
        for (TableName tableName : tableNames) {
            TableDescriptor desc = getTableDescriptor(tableName);
            if (desc != null) {
                response.add(desc);
            }
        }
        return response;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<TableDescriptor> listTableDescriptors(Pattern pattern, boolean includeSysTables)
        throws IOException {
        return Arrays.asList(listTables(pattern, includeSysTables));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public HTableDescriptor[] listTables(Pattern pattern) throws IOException {
        // NOTE: We don't have systables
        return getTableDescriptors(listTableNames(pattern));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public HTableDescriptor[] listTables(final Pattern pattern, final boolean includeSysTables)
        throws IOException {
        return listTables(pattern);
    }

    // Used by the Hbase shell but not defined by Admin. Will be removed once the
    // shell is switch to use the methods defined in the interface.

    /**
     * {@inheritDoc}
     */
    @Override
    @Deprecated
    public TableName[] listTableNames(String patternStr) throws IOException {
        return listTableNames(Pattern.compile(patternStr));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TableName[] listTableNames(Pattern pattern) throws IOException {
        List<TableName> result = new ArrayList<>();

        for (TableName tableName : listTableNames()) {
            if (pattern.matcher(tableName.getNameAsString()).matches()) {
                result.add(tableName);
            }
        }

        return result.toArray(new TableName[result.size()]);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TableName[] listTableNames(Pattern pattern, boolean includeSysTables) throws IOException {
        return listTableNames(pattern);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TableName[] listTableNames(String regex, boolean includeSysTables) throws IOException {
        return listTableNames(regex);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public HTableDescriptor[] listTables(String regex) throws IOException {
        return listTables(Pattern.compile(regex));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public HTableDescriptor[] listTables(String regex, boolean includeSysTables) throws IOException {
        return listTables(regex);
    }

    /**
     * Lists all table names for the cluster provided in the configuration.
     */
    @Override
    public TableName[] listTableNames() throws IOException {
        return getTables().keySet().toArray(new TableName[0]);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public HTableDescriptor getTableDescriptor(TableName tableName) throws IOException {
        if (tableName == null) {
            return null;
        }

        return new HTableDescriptor(getLatestSchemaValue(tableName).getSchema().toDesc());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TableDescriptor getDescriptor(TableName tableName) throws IOException {
        return getTableDescriptor(tableName);
    }

    // Used by the Hbase shell but not defined by Admin. Will be removed once the
    // shell is switch to use the methods defined in the interface.

    /**
     * getTableNames.
     *
     * @param regex a {@link String} object.
     * @return an array of {@link String} objects.
     * @throws IOException if any.
     */
    @Deprecated
    public String[] getTableNames(String regex) throws IOException {
        TableName[] tableNames = listTableNames(regex);
        String[] tableIds = new String[tableNames.length];
        for (int i = 0; i < tableNames.length; i++) {
            tableIds[i] = tableNames[i].getNameAsString();
        }
        return tableIds;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void createTable(TableDescriptor desc) throws IOException {
        createTable(desc, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void createTable(TableDescriptor desc, byte[] startKey, byte[] endKey, int numRegions)
        throws IOException {
        createTable(desc, createSplitKeys(startKey, endKey, numRegions));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void createTable(TableDescriptor desc, byte[][] splitKeys) throws IOException {
        Cache<KafkaSchemaKey, KafkaSchemaValue> schemas = getSchemas();
        TableName tableName = desc.getTableName();
        KafkaSchemaValue latest = getLatestSchemaValue(tableName);
        // Verify the previous version is null or deleted
        if (latest != null && latest.getAction() != Action.DROP) {
            throw new IllegalStateException("Table " + tableName + " already exists");
        }
        int version = latest != null ? latest.getVersion() + 1 : 1;
        schemas.put(new KafkaSchemaKey(tableName.getNameAsString(), version),
            new KafkaSchemaValue(tableName.getNameAsString(), version, new TableDef(desc), Action.CREATE, version));
        schemas.flush();
        // Initialize the table
        KafkaStoreTable table = getTables().get(tableName);
        table.getCache().init();
    }

    public static byte[][] createSplitKeys(byte[] startKey, byte[] endKey, int numRegions) {
        if (numRegions < 3) {
            throw new IllegalArgumentException("Must create at least three regions");
        } else if (Bytes.compareTo(startKey, endKey) >= 0) {
            throw new IllegalArgumentException("Start key must be smaller than end key");
        }
        byte[][] splitKeys;
        if (numRegions == 3) {
            splitKeys = new byte[][]{startKey, endKey};
        } else {
            splitKeys = Bytes.split(startKey, endKey, numRegions - 3);
            if (splitKeys == null || splitKeys.length != numRegions - 1) {
                throw new IllegalArgumentException("Unable to split key range into enough regions");
            }
        }
        return splitKeys;
    }

    @Override
    public Future<Void> createTableAsync(TableDescriptor tableDescriptor) throws IOException {
        throw new UnsupportedOperationException("createTableAsync");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Future<Void> createTableAsync(TableDescriptor desc, byte[][] splitKeys)
        throws IOException {
        throw new UnsupportedOperationException("createTableAsync");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteTable(TableName tableName) throws IOException {
        Cache<KafkaSchemaKey, KafkaSchemaValue> schemas = getSchemas();
        KafkaSchemaValue latest = getLatestSchemaValue(tableName);
        if (latest == null || latest.getAction() == Action.DROP) {
            return;
        }
        int version = latest.getVersion();
        schemas.put(new KafkaSchemaKey(tableName.getNameAsString(), version + 1),
            new KafkaSchemaValue(tableName.getNameAsString(), version + 1, null, Action.DROP, latest.getEpoch()));
        schemas.flush();
        disabledTables.remove(tableName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public HTableDescriptor[] deleteTables(String regex) throws IOException {
        return deleteTables(Pattern.compile(regex));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public HTableDescriptor[] deleteTables(Pattern pattern) throws IOException {
        List<HTableDescriptor> failed = new LinkedList<>();
        for (HTableDescriptor table : listTables(pattern)) {
            try {
                deleteTable(table.getTableName());
            } catch (IOException ex) {
                LOG.info("Failed to delete table " + table.getTableName(), ex);
                failed.add(table);
            }
        }
        return failed.toArray(new HTableDescriptor[failed.size()]);
    }

    @Override
    public Future<Void> deleteTableAsync(TableName tableName) throws IOException {
        throw new UnsupportedOperationException("deleteTableAsync");
    }

    @Override
    public void modifyTable(TableDescriptor tableDescriptor) throws IOException {
        modifyTable(tableDescriptor.getTableName(), tableDescriptor);
    }

    @Override
    public void modifyTable(TableName tableName, TableDescriptor tableDesc) throws IOException {
        Cache<KafkaSchemaKey, KafkaSchemaValue> schemas = getSchemas();
        KafkaSchemaValue latest = getLatestSchemaValue(tableName);
        if (latest == null || latest.getAction() == Action.DROP) {
            throw new IllegalStateException("Table " + tableName + " does not exist");
        }
        int version = latest.getVersion();
        schemas.put(new KafkaSchemaKey(tableName.getNameAsString(), version + 1),
            new KafkaSchemaValue(tableName.getNameAsString(), version + 1, new TableDef(tableDesc), Action.ALTER, latest.getEpoch()));
        schemas.flush();
    }

    @Override
    public Future<Void> modifyTableAsync(TableDescriptor tableDescriptor) throws IOException {
        return modifyTableAsync(tableDescriptor.getTableName(), tableDescriptor);
    }

    @Override
    public Future<Void> modifyTableAsync(TableName tableName, TableDescriptor newDescriptor) {
        throw new UnsupportedOperationException("modifyTableAsync");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void enableTable(TableName tableName) throws IOException {
        TableName.isLegalFullyQualifiedTableName(tableName.getName());
        if (!tableExists(tableName)) {
            throw new TableNotFoundException(tableName);
        }
        disabledTables.remove(tableName);
        LOG.warn("Table " + tableName + " was enabled in memory only.");
    }

    // Used by the Hbase shell but not defined by Admin. Will be removed once the
    // shell is switch to use the methods defined in the interface.

    /**
     * {@inheritDoc}
     */
    @Override
    public HTableDescriptor[] enableTables(String regex) throws IOException {
        HTableDescriptor[] tableDescriptors = listTables(regex);
        for (HTableDescriptor descriptor : tableDescriptors) {
            enableTable(descriptor.getTableName());
        }
        return tableDescriptors;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public HTableDescriptor[] enableTables(Pattern pattern) throws IOException {
        HTableDescriptor[] tableDescriptors = listTables(pattern);
        for (HTableDescriptor descriptor : tableDescriptors) {
            enableTable(descriptor.getTableName());
        }
        return tableDescriptors;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Future<Void> enableTableAsync(TableName tableName) throws IOException {
        throw new UnsupportedOperationException("enableTableAsync");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void disableTable(TableName tableName) throws IOException {
        TableName.isLegalFullyQualifiedTableName(tableName.getName());
        if (!tableExists(tableName)) {
            throw new TableNotFoundException(tableName);
        }
        if (isTableDisabled(tableName)) {
            throw new TableNotEnabledException(tableName);
        }
        disabledTables.add(tableName);
        LOG.warn("Table " + tableName + " was disabled in memory only.");
    }

    // Used by the Hbase shell but not defined by Admin. Will be removed once the
    // shell is switch to use the methods defined in the interface.

    /**
     * disableTable.
     *
     * @param tableName a {@link java.lang.String} object.
     * @throws java.io.IOException if any.
     */
    @Deprecated
    public void disableTable(String tableName) throws IOException {
        disableTable(TableName.valueOf(tableName));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public HTableDescriptor[] disableTables(String regex) throws IOException {
        HTableDescriptor[] tableDescriptors = listTables(regex);
        for (HTableDescriptor descriptor : tableDescriptors) {
            disableTable(descriptor.getTableName());
        }
        return tableDescriptors;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public HTableDescriptor[] disableTables(Pattern pattern) throws IOException {
        HTableDescriptor[] tableDescriptors = listTables(pattern);
        for (HTableDescriptor descriptor : tableDescriptors) {
            disableTable(descriptor.getTableName());
        }
        return tableDescriptors;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Future<Void> deleteNamespaceAsync(String name) throws IOException {
        deleteNamespace(name);
        return CompletableFuture.runAsync(() -> {
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Future<Void> disableTableAsync(TableName tableName) throws IOException {
        throw new UnsupportedOperationException("disableTableAsync");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isTableEnabled(TableName tableName) throws IOException {
        return !isTableDisabled(tableName);
    }

    // Used by the Hbase shell but not defined by Admin. Will be removed once the
    // shell is switch to use the methods defined in the interface.

    /**
     * isTableEnabled.
     *
     * @param tableName a {@link java.lang.String} object.
     * @return a boolean.
     * @throws java.io.IOException if any.
     */
    @Deprecated
    public boolean isTableEnabled(String tableName) throws IOException {
        return isTableEnabled(TableName.valueOf(tableName));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isTableDisabled(TableName tableName) throws IOException {
        return disabledTables.contains(tableName);
    }

    // Used by the Hbase shell but not defined by Admin. Will be removed once the
    // shell is switch to use the methods defined in the interface.

    /**
     * isTableDisabled.
     *
     * @param tableName a {@link java.lang.String} object.
     * @return a boolean.
     * @throws java.io.IOException if any.
     */
    @Deprecated
    public boolean isTableDisabled(String tableName) throws IOException {
        return isTableDisabled(TableName.valueOf(tableName));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isTableAvailable(TableName tableName) throws IOException {
        return tableExists(tableName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteColumn(TableName tableName, byte[] columnName) throws IOException {
        TableDescriptor tableDesc = getTableDescriptor(tableName);
        TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableDesc);
        builder.removeColumnFamily(columnName);
        modifyTable(tableName, builder.build());
    }

    // Used by the Hbase shell but not defined by Admin. Will be removed once the
    // shell is switch to use the methods defined in the interface.
    @Deprecated
    public void addColumn(String tableName, HColumnDescriptor column) throws IOException {
        addColumn(TableName.valueOf(tableName), column);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addColumnFamily(TableName tableName, ColumnFamilyDescriptor columnFamilyDesc)
        throws IOException {
        TableDescriptor tableDesc = getTableDescriptor(tableName);
        TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableDesc);
        builder.setColumnFamily(columnFamilyDesc);
        modifyTable(tableName, builder.build());
    }

    @Override
    public Future<Void> addColumnFamilyAsync(
        TableName tableName, ColumnFamilyDescriptor columnFamily) {
        throw new UnsupportedOperationException("addColumnFamilyAsync");
    }

    /**
     * Modify an existing column family on a table. NOTE: this is needed for backwards compatibility
     * for the hbase shell.
     *
     * @param tableName  a {@link TableName} object.
     * @param descriptor a {@link HColumnDescriptor} object.
     * @throws java.io.IOException if any.
     */
    public void modifyColumns(final String tableName, HColumnDescriptor descriptor)
        throws IOException {
        modifyColumn(TableName.valueOf(tableName), descriptor);
    }

    // Used by the Hbase shell but not defined by Admin. Will be removed once the
    // shell is switch to use the methods defined in the interface.

    /**
     * deleteColumn.
     *
     * @param tableName  a {@link java.lang.String} object.
     * @param columnName an array of byte.
     * @throws java.io.IOException if any.
     */
    @Deprecated
    public void deleteColumn(String tableName, byte[] columnName) throws IOException {
        deleteColumn(TableName.valueOf(tableName), columnName);
    }

    // Used by the Hbase shell but not defined by Admin. Will be removed once the
    // shell is switch to use the methods defined in the interface.

    /**
     * deleteColumn.
     *
     * @param tableName  a {@link java.lang.String} object.
     * @param columnName a {@link java.lang.String} object.
     * @throws java.io.IOException if any.
     */
    @Deprecated
    public void deleteColumn(final String tableName, final String columnName) throws IOException {
        deleteColumn(TableName.valueOf(tableName), Bytes.toBytes(columnName));
    }

    @Override
    public void deleteColumnFamily(TableName tableName, byte[] columnName) throws IOException {
        deleteColumn(tableName, columnName);
    }

    @Override
    public Future<Void> deleteColumnFamilyAsync(TableName tableName, byte[] columnName) {
        throw new UnsupportedOperationException("deleteColumnFamilyAsync");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void modifyColumnFamily(TableName tableName, ColumnFamilyDescriptor columnFamilyDesc)
        throws IOException {
        TableDescriptor tableDesc = getTableDescriptor(tableName);
        TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableDesc);
        builder.modifyColumnFamily(columnFamilyDesc);
        modifyTable(tableName, builder.build());
    }

    @Override
    public Future<Void> modifyColumnFamilyAsync(
        TableName tableName, ColumnFamilyDescriptor columnFamily) throws IOException {
        throw new UnsupportedOperationException("modifyColumnFamilyAsync");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ClusterStatus getClusterStatus() throws IOException {
        return new ClusterStatus(null) {
            @Override
            public Collection<ServerName> getServers() {
                return Collections.emptyList();
            }
        };
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Configuration getConfiguration() {
        return conn.getConfiguration();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<HRegionInfo> getTableRegions(TableName tableName) throws IOException {
        return Collections.emptyList();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws IOException {
        // no-op
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public HTableDescriptor[] getTableDescriptorsByTableName(List<TableName> tableNames)
        throws IOException {
        TableName[] tableNameArray = tableNames.toArray(new TableName[tableNames.size()]);
        return getTableDescriptors(tableNameArray);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public HTableDescriptor[] getTableDescriptors(List<String> names) throws IOException {
        TableName[] tableNameArray = new TableName[names.size()];
        for (int i = 0; i < names.size(); i++) {
            tableNameArray[i] = TableName.valueOf(names.get(i));
        }
        return getTableDescriptors(tableNameArray);
    }

    /* Unsupported operations */

    /**
     * {@inheritDoc}
     */
    @Override
    public int getOperationTimeout() {
        throw new UnsupportedOperationException("getOperationTimeout");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void abort(String why, Throwable e) {
        throw new UnsupportedOperationException("abort");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isAborted() {
        throw new UnsupportedOperationException("isAborted");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void truncateTable(TableName tableName, boolean preserveSplits) throws IOException {
        throw new UnsupportedOperationException("truncateTable");
    }

    @Override
    public Future<Void> truncateTableAsync(TableName tableName, boolean preserveSplits)
        throws IOException {
        throw new UnsupportedOperationException("truncateTableAsync");
    }

    @Override
    public boolean abortProcedure(long arg0, boolean arg1) throws IOException {
        throw new UnsupportedOperationException("abortProcedure");
    }

    @Override
    public Future<Boolean> abortProcedureAsync(long arg0, boolean arg1) throws IOException {
        throw new UnsupportedOperationException("abortProcedureAsync");
    }

    @Override
    public boolean balance() throws IOException {
        throw new UnsupportedOperationException("balance");
    }

    @Override
    public boolean balance(boolean arg0) throws IOException {
        throw new UnsupportedOperationException("balance");
    }

    @Override
    public boolean balancerSwitch(boolean arg0, boolean arg1) throws IOException {
        throw new UnsupportedOperationException("balancerSwitch");
    }

    @Override
    public boolean catalogJanitorSwitch(boolean arg0) throws IOException {
        throw new UnsupportedOperationException("catalogJanitorSwitch");
    }

    @Override
    public boolean cleanerChoreSwitch(boolean arg0) throws IOException {
        throw new UnsupportedOperationException("cleanerChoreSwitch");
    }

    @Override
    public void clearCompactionQueues(ServerName arg0, Set<String> arg1)
        throws IOException, InterruptedException {
        throw new UnsupportedOperationException("clearCompactionQueues");
    }

    @Override
    public List<ServerName> clearDeadServers(List<ServerName> arg0) throws IOException {
        throw new UnsupportedOperationException("clearDeadServers");
    }

    @Override
    public void cloneSnapshot(String arg0, TableName arg1, boolean arg2)
        throws IOException, TableExistsException, RestoreSnapshotException {
        throw new UnsupportedOperationException("cloneSnapshot");
    }

    @Override
    public Future<Void> cloneSnapshotAsync(String arg0, TableName arg1)
        throws IOException, TableExistsException {
        throw new UnsupportedOperationException("cloneSnapshotAsync");
    }

    @Override
    public void cloneTableSchema(TableName tableName, TableName tableName1, boolean b) {
        throw new UnsupportedOperationException("cloneTableSchema");
    }

    @Override
    public boolean switchRpcThrottle(boolean enable) throws IOException {
        throw new UnsupportedOperationException("switchRpcThrottle");
    }

    @Override
    public boolean isRpcThrottleEnabled() throws IOException {
        throw new UnsupportedOperationException("isRpcThrottleEnabled");
    }

    @Override
    public boolean exceedThrottleQuotaSwitch(boolean b) throws IOException {
        throw new UnsupportedOperationException("exceedThrottleQuotaSwitch");
    }

    @Override
    public Map<TableName, Long> getSpaceQuotaTableSizes() throws IOException {
        throw new UnsupportedOperationException("getSpaceQuotaTableSizes");
    }

    @Override
    public Map<TableName, ? extends SpaceQuotaSnapshotView> getRegionServerSpaceQuotaSnapshots(
        ServerName serverName) throws IOException {
        throw new UnsupportedOperationException("getRegionServerSpaceQuotaSnapshots");
    }

    @Override
    public SpaceQuotaSnapshotView getCurrentSpaceQuotaSnapshot(String namespace) throws IOException {
        throw new UnsupportedOperationException("getCurrentSpaceQuotaSnapshot");
    }

    @Override
    public SpaceQuotaSnapshotView getCurrentSpaceQuotaSnapshot(TableName tableName)
        throws IOException {
        throw new UnsupportedOperationException("getCurrentSpaceQuotaSnapshot");
    }

    @Override
    public void grant(UserPermission userPermission, boolean mergeExistingPermissions)
        throws IOException {
        throw new UnsupportedOperationException("grant");
    }

    @Override
    public void revoke(UserPermission userPermission) throws IOException {
        throw new UnsupportedOperationException("revoke");
    }

    @Override
    public List<UserPermission> getUserPermissions(
        GetUserPermissionsRequest getUserPermissionsRequest) throws IOException {
        throw new UnsupportedOperationException("getUserPermissions");
    }

    @Override
    public List<Boolean> hasUserPermissions(String userName, List<Permission> permissions)
        throws IOException {
        throw new UnsupportedOperationException("hasUserPermissions");
    }

    @Override
    public void compact(TableName arg0, CompactType arg1) throws IOException, InterruptedException {
        throw new UnsupportedOperationException("compact");
    }

    @Override
    public void compact(TableName arg0, byte[] arg1, CompactType arg2)
        throws IOException, InterruptedException {
        throw new UnsupportedOperationException("compact");
    }

    @Override
    public Future<Void> createNamespaceAsync(NamespaceDescriptor arg0) throws IOException {
        throw new UnsupportedOperationException("createNamespaceAsync");
    }

    @Override
    public void decommissionRegionServers(List<ServerName> arg0, boolean arg1) throws IOException {
        throw new UnsupportedOperationException("decommissionRegionServers");
    }

    @Override
    public void disableTableReplication(TableName arg0) throws IOException {
        throw new UnsupportedOperationException("disableTableReplication");
    }

    @Override
    public void enableTableReplication(TableName arg0) throws IOException {
        throw new UnsupportedOperationException("enableTableReplication");
    }

    @Override
    public Future<Void> enableReplicationPeerAsync(String s) {
        throw new UnsupportedOperationException("enableTableReplication");
    }

    @Override
    public Future<Void> disableReplicationPeerAsync(String s) {
        throw new UnsupportedOperationException("disableReplicationPeerAsync");
    }

    @Override
    public byte[] execProcedureWithReturn(String arg0, String arg1, Map<String, String> arg2)
        throws IOException {
        throw new UnsupportedOperationException("execProcedureWithReturn");
    }

    @Override
    public CompactionState getCompactionState(TableName arg0) throws IOException {
        throw new UnsupportedOperationException("getCompactionState");
    }

    @Override
    public CompactionState getCompactionState(TableName arg0, CompactType arg1) throws IOException {
        throw new UnsupportedOperationException("getCompactionState");
    }

    @Override
    public CompactionState getCompactionStateForRegion(byte[] arg0) throws IOException {
        throw new UnsupportedOperationException("getCompactionStateForRegion");
    }

    @Override
    public long getLastMajorCompactionTimestamp(TableName arg0) throws IOException {
        throw new UnsupportedOperationException("getLastMajorCompactionTimestamp");
    }

    @Override
    public long getLastMajorCompactionTimestampForRegion(byte[] arg0) throws IOException {
        throw new UnsupportedOperationException("getLastMajorCompactionTimestamp");
    }

    @Override
    public String getLocks() throws IOException {
        throw new UnsupportedOperationException("getLocks");
    }

    @Override
    public String getProcedures() throws IOException {
        throw new UnsupportedOperationException("getProcedures");
    }

    @Override
    public QuotaRetriever getQuotaRetriever(QuotaFilter arg0) throws IOException {
        throw new UnsupportedOperationException("getQuotaRetriever");
    }

    @Override
    public List<RegionInfo> getRegions(ServerName arg0) throws IOException {
        throw new UnsupportedOperationException("getRegions");
    }

    @Override
    public void flushRegionServer(ServerName serverName) throws IOException {
        throw new UnsupportedOperationException("flushRegionServer");
    }

    @Override
    public List<RegionInfo> getRegions(TableName tableName) throws IOException {
        List<RegionInfo> regionInfo = new ArrayList<>();
        for (HRegionInfo hRegionInfo : getTableRegions(tableName)) {
            regionInfo.add(hRegionInfo);
        }
        return regionInfo;
    }

    @Override
    public List<SecurityCapability> getSecurityCapabilities() throws IOException {
        throw new UnsupportedOperationException("getSecurityCapabilities");
    }

    @Override
    public boolean isBalancerEnabled() throws IOException {
        throw new UnsupportedOperationException("isBalancerEnabled");
    }

    @Override
    public boolean isCleanerChoreEnabled() throws IOException {
        throw new UnsupportedOperationException("isCleanerChoreEnabled");
    }

    @Override
    public boolean isMasterInMaintenanceMode() throws IOException {
        throw new UnsupportedOperationException("isMasterInMaintenanceMode");
    }

    @Override
    public boolean isNormalizerEnabled() throws IOException {
        throw new UnsupportedOperationException("isNormalizerEnabled");
    }

    @Override
    public boolean isSnapshotFinished(SnapshotDescription arg0)
        throws IOException, HBaseSnapshotException, UnknownSnapshotException {
        throw new UnsupportedOperationException("isSnapshotFinished");
    }

    @Override
    public List<ServerName> listDeadServers() throws IOException {
        throw new UnsupportedOperationException("listDeadServers");
    }

    @Override
    public List<ServerName> listDecommissionedRegionServers() throws IOException {
        throw new UnsupportedOperationException("listDecommissionedRegionServers");
    }

    @Override
    public List<TableCFs> listReplicatedTableCFs() throws IOException {
        throw new UnsupportedOperationException("listReplicatedTableCFs");
    }

    @Override
    public void majorCompact(TableName arg0, CompactType arg1)
        throws IOException, InterruptedException {
        throw new UnsupportedOperationException("majorCompact");
    }

    @Override
    public void majorCompact(TableName arg0, byte[] arg1, CompactType arg2)
        throws IOException, InterruptedException {
        throw new UnsupportedOperationException("majorCompact");
    }

    @Override
    public Map<ServerName, Boolean> compactionSwitch(
        boolean switchState, List<String> serverNamesList) throws IOException {
        throw new UnsupportedOperationException("compactionSwitch");
    }

    @Override
    public Future<Void> mergeRegionsAsync(byte[][] arg0, boolean arg1) throws IOException {
        throw new UnsupportedOperationException("mergeRegionsAsync");
    }

    @Override
    public Future<Void> splitRegionAsync(byte[] regionName) throws IOException {
        throw new UnsupportedOperationException("splitRegionAsync");
    }

    @Override
    public Future<Void> mergeRegionsAsync(byte[] arg0, byte[] arg1, boolean arg2) throws IOException {
        throw new UnsupportedOperationException("mergeRegionsAsync");
    }

    @Override
    public Future<Void> modifyNamespaceAsync(NamespaceDescriptor arg0) throws IOException {
        throw new UnsupportedOperationException("modifyNamespaceAsync");
    }

    @Override
    public boolean normalize() throws IOException {
        throw new UnsupportedOperationException("normalize");
    }

    @Override
    public boolean normalizerSwitch(boolean arg0) throws IOException {
        throw new UnsupportedOperationException("normalizerSwitch");
    }

    @Override
    public void recommissionRegionServer(ServerName arg0, List<byte[]> arg1) throws IOException {
        throw new UnsupportedOperationException("recommissionRegionServer");
    }

    @Override
    public void restoreSnapshot(String arg0, boolean arg1, boolean arg2)
        throws IOException, RestoreSnapshotException {
        throw new UnsupportedOperationException("restoreSnapshot");
    }

    @Override
    public Future<Void> restoreSnapshotAsync(String arg0)
        throws IOException, RestoreSnapshotException {
        throw new UnsupportedOperationException("restoreSnapshotAsync");
    }

    @Override
    public int runCatalogJanitor() throws IOException {
        throw new UnsupportedOperationException("runCatalogJanitor");
    }

    @Override
    public boolean runCleanerChore() throws IOException {
        throw new UnsupportedOperationException("runCleanerChore");
    }

    @Override
    public void setQuota(QuotaSettings arg0) throws IOException {
        throw new UnsupportedOperationException("setQuota");
    }

    @Override
    public Future<Void> splitRegionAsync(byte[] arg0, byte[] arg1) throws IOException {
        throw new UnsupportedOperationException("splitRegionAsync");
    }

    @Override
    public void addReplicationPeer(String arg0, ReplicationPeerConfig arg1, boolean arg2)
        throws IOException {
        throw new UnsupportedOperationException("addReplicationPeer");
    }

    @Override
    public Future<Void> addReplicationPeerAsync(String peerId, ReplicationPeerConfig peerConfig) {
        throw new UnsupportedOperationException("addReplicationPeerAsync");
    }

    @Override
    public Future<Void> addReplicationPeerAsync(
        String s, ReplicationPeerConfig replicationPeerConfig, boolean b) {
        throw new UnsupportedOperationException("addReplicationPeerAsync");
    }

    @Override
    public void appendReplicationPeerTableCFs(String arg0, Map<TableName, List<String>> arg1)
        throws ReplicationException, IOException {
        throw new UnsupportedOperationException("appendReplicationPeerTableCFs");
    }

    @Override
    public CacheEvictionStats clearBlockCache(TableName arg0) throws IOException {
        throw new UnsupportedOperationException("clearBlockCache");
    }

    @Override
    public void compactRegionServer(ServerName arg0) throws IOException {
        throw new UnsupportedOperationException("splitRegionAsync");
    }

    @Override
    public void disableReplicationPeer(String arg0) throws IOException {
        throw new UnsupportedOperationException("disableReplicationPeer");
    }

    @Override
    public void enableReplicationPeer(String arg0) throws IOException {
        throw new UnsupportedOperationException("enableReplicationPeer");
    }

    @Override
    public ClusterMetrics getClusterMetrics(EnumSet<ClusterMetrics.Option> arg0) throws IOException {
        return getClusterStatus();
    }

    @Override
    public List<QuotaSettings> getQuota(QuotaFilter arg0) throws IOException {
        throw new UnsupportedOperationException("getQuota");
    }

    @Override
    public List<RegionMetrics> getRegionMetrics(ServerName arg0, TableName arg1) throws IOException {
        throw new UnsupportedOperationException("getRegionMetrics");
    }

    @Override
    public ReplicationPeerConfig getReplicationPeerConfig(String arg0) throws IOException {
        throw new UnsupportedOperationException("getReplicationPeerConfig");
    }

    @Override
    public boolean isMergeEnabled() throws IOException {
        throw new UnsupportedOperationException("isMergeEnabled");
    }

    @Override
    public boolean isSplitEnabled() throws IOException {
        throw new UnsupportedOperationException("isSplitEnabled");
    }

    @Override
    public List<ReplicationPeerDescription> listReplicationPeers() throws IOException {
        throw new UnsupportedOperationException("listReplicationPeers");
    }

    @Override
    public List<ReplicationPeerDescription> listReplicationPeers(Pattern arg0) throws IOException {
        throw new UnsupportedOperationException("listReplicationPeers");
    }

    @Override
    public void majorCompactRegionServer(ServerName arg0) throws IOException {
        throw new UnsupportedOperationException("majorCompactRegionServer");
    }

    @Override
    public void move(byte[] encodedRegionName) throws IOException {
        throw new UnsupportedOperationException("move");
    }

    @Override
    public void move(byte[] encodedRegionName, ServerName destServerName) throws IOException {
        throw new UnsupportedOperationException("move");
    }

    @Override
    public boolean mergeSwitch(boolean arg0, boolean arg1) throws IOException {
        throw new UnsupportedOperationException("mergeSwitch");
    }

    @Override
    public void removeReplicationPeer(String arg0) throws IOException {
        throw new UnsupportedOperationException("removeReplicationPeer");
    }

    @Override
    public void removeReplicationPeerTableCFs(String arg0, Map<TableName, List<String>> arg1)
        throws ReplicationException, IOException {
        throw new UnsupportedOperationException("removeReplicationPeerTableCFs");
    }

    @Override
    public Future<Void> removeReplicationPeerAsync(String s) {
        throw new UnsupportedOperationException("removeReplicationPeerAsync");
    }

    @Override
    public boolean splitSwitch(boolean arg0, boolean arg1) throws IOException {
        throw new UnsupportedOperationException("splitSwitch");
    }

    @Override
    public void updateReplicationPeerConfig(String arg0, ReplicationPeerConfig arg1)
        throws IOException {
        throw new UnsupportedOperationException("updateReplicationPeerConfig");
    }

    @Override
    public Future<Void> updateReplicationPeerConfigAsync(
        String s, ReplicationPeerConfig replicationPeerConfig) {
        throw new UnsupportedOperationException("updateReplicationPeerConfigAsync");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isTableAvailable(TableName tableName, byte[][] splitKeys) throws IOException {
        return tableExists(tableName);
    }

    /**
     * {@inheritDoc}
     *
     * <p>HBase column operations are not synchronous, since they're not as fast as Bigtable. Bigtable
     * does not have async operations, so always return (0, 0). This is needed for some shell
     * operations.
     */
    @Override
    public Pair<Integer, Integer> getAlterStatus(TableName tableName) throws IOException {
        return new Pair<>(0, 0);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Pair<Integer, Integer> getAlterStatus(byte[] tableName) throws IOException {
        return getAlterStatus(TableName.valueOf(tableName));
    }

    // ------------ SNAPSHOT methods begin

    /**
     * Creates a snapshot from an existing table. NOTE: Cloud Bigtable has a cleanup policy
     *
     * @param snapshotName a {@link String} object.
     * @param tableName    a {@link TableName} object.
     * @throws IOException if any.
     */
    @Override
    public void snapshot(String snapshotName, TableName tableName)
        throws IOException, SnapshotCreationException, IllegalArgumentException {
        throw new UnsupportedOperationException("snapshot");
    }

    /**
     * This is needed for the hbase shell.
     *
     * @param snapshotName a byte array object.
     * @param tableName    a byte array object.
     * @throws IOException if any.
     */
    public void snapshot(byte[] snapshotName, byte[] tableName)
        throws IOException, IllegalArgumentException {
        snapshot(snapshotName, TableName.valueOf(tableName));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void snapshot(byte[] snapshotName, TableName tableName)
        throws IOException, IllegalArgumentException {
        snapshot(Bytes.toString(snapshotName), tableName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void snapshot(SnapshotDescription snapshot)
        throws IOException, SnapshotCreationException, IllegalArgumentException {
        snapshot(snapshot.getName(), snapshot.getTableName());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void snapshot(String snapshotName, TableName tableName, SnapshotType arg2)
        throws IOException, SnapshotCreationException, IllegalArgumentException {
        snapshot(snapshotName, tableName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void snapshotAsync(SnapshotDescription snapshot)
        throws IOException, SnapshotCreationException {
        throw new UnsupportedOperationException("snapshotAsync");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<SnapshotDescription> listSnapshots(String regex) throws IOException {
        return listSnapshots(Pattern.compile(regex));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<SnapshotDescription> listSnapshots(Pattern pattern) throws IOException {
        List<SnapshotDescription> response = new ArrayList<>();
        for (SnapshotDescription description : listSnapshots()) {
            if (pattern.matcher(description.getName()).matches()) {
                response.add(description);
            }
        }
        return response;
    }

    @Override
    public List<SnapshotDescription> listSnapshots() throws IOException {
        throw new UnsupportedOperationException("listSnapshots");
    }

    @Override
    public List<SnapshotDescription> listTableSnapshots(String tableName, String snapshotName)
        throws IOException {
        return listTableSnapshots(Pattern.compile(tableName), Pattern.compile(snapshotName));
    }

    @Override
    public List<SnapshotDescription> listTableSnapshots(Pattern tableName, Pattern snapshotName)
        throws IOException {
        List<SnapshotDescription> response = new ArrayList<>();
        for (SnapshotDescription snapshot : listSnapshots(snapshotName)) {
            if (tableName.matcher(snapshot.getTableNameAsString()).matches()) {
                response.add(snapshot);
            }
        }
        return response;
    }

    /**
     * This is needed for the hbase shell.
     *
     * @param snapshotName a byte array object.
     * @param tableName    a byte array object.
     * @throws IOException if any.
     */
    public void cloneSnapshot(byte[] snapshotName, byte[] tableName) throws IOException {
        cloneSnapshot(snapshotName, TableName.valueOf(tableName));
    }

    /**
     * @param snapshotName a {@link String} object.
     * @param tableName    a {@link TableName} object.
     * @throws IOException if any.
     */
    @Override
    public void cloneSnapshot(byte[] snapshotName, TableName tableName)
        throws IOException, TableExistsException, RestoreSnapshotException {
        cloneSnapshot(Bytes.toString(snapshotName), tableName);
    }

    /**
     * @param snapshotName a {@link String} object.
     * @param tableName    a {@link TableName} object.
     * @throws IOException if any.
     */
    @Override
    public void cloneSnapshot(String snapshotName, TableName tableName)
        throws IOException, TableExistsException, RestoreSnapshotException {
        throw new UnsupportedOperationException("cloneSnapshot");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteSnapshot(byte[] snapshotName) throws IOException {
        deleteSnapshot(Bytes.toString(snapshotName));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteSnapshot(String snapshotName) throws IOException {
        throw new UnsupportedOperationException("deleteSnapshot");
    }

    /**
     * {@inheritDoc}
     *
     * <p>The snapshots will be deleted serially and the first failure will prevent the deletion of
     * the remaining snapshots.
     */
    @Override
    public void deleteSnapshots(String regex) throws IOException {
        deleteSnapshots(Pattern.compile(regex));
    }

    /**
     * {@inheritDoc}
     *
     * <p>The snapshots will be deleted serially and the first failure will prevent the deletion of
     * the remaining snapshots.
     */
    @Override
    public void deleteSnapshots(Pattern pattern) throws IOException {
        throw new UnsupportedOperationException("deleteSnapshots");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteTableSnapshots(String tableNameRegex, String snapshotNameRegex)
        throws IOException {
        deleteTableSnapshots(Pattern.compile(tableNameRegex), Pattern.compile(snapshotNameRegex));
    }

    /**
     * {@inheritDoc}
     *
     * <p>The snapshots will be deleted serially and the first failure will prevent the deletion of
     * the remaining snapshots.
     */
    @Override
    public void deleteTableSnapshots(Pattern tableNamePattern, Pattern snapshotNamePattern)
        throws IOException {
        throw new UnsupportedOperationException("deleteTableSnapshots");
    }

    // ------------- Unsupported snapshot methods.

    /**
     * {@inheritDoc}
     */
    @Override
    public void restoreSnapshot(byte[] snapshotName) throws IOException, RestoreSnapshotException {
        throw new UnsupportedOperationException("restoreSnapshot");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void restoreSnapshot(String snapshotName) throws IOException, RestoreSnapshotException {
        throw new UnsupportedOperationException("restoreSnapshot");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void restoreSnapshot(byte[] snapshotName, boolean takeFailSafeSnapshot)
        throws IOException, RestoreSnapshotException {
        throw new UnsupportedOperationException("restoreSnapshot");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void restoreSnapshot(String snapshotName, boolean takeFailSafeSnapshot)
        throws IOException, RestoreSnapshotException {
        throw new UnsupportedOperationException("restoreSnapshot");
    }

    // ------------- Snapshot method end

    /**
     * {@inheritDoc}
     */
    @Override
    public void closeRegion(String regionname, String serverName) throws IOException {
        throw new UnsupportedOperationException("closeRegion");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void closeRegion(byte[] regionname, String serverName) throws IOException {
        throw new UnsupportedOperationException("closeRegion");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean closeRegionWithEncodedRegionName(String encodedRegionName, String serverName)
        throws IOException {
        throw new UnsupportedOperationException("closeRegionWithEncodedRegionName");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void closeRegion(ServerName sn, HRegionInfo hri) throws IOException {
        throw new UnsupportedOperationException("closeRegion");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<HRegionInfo> getOnlineRegions(ServerName sn) throws IOException {
        throw new UnsupportedOperationException("getOnlineRegions");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void flush(TableName tableName) throws IOException {
        throw new UnsupportedOperationException("flush");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void flushRegion(byte[] bytes) throws IOException {
        LOG.info("flushRegion is a no-op");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void compact(TableName tableName) throws IOException {
        LOG.info("compact is a no-op");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void compactRegion(byte[] bytes) throws IOException {
        LOG.info("compactRegion is a no-op");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void compact(TableName tableName, byte[] bytes) throws IOException {
        LOG.info("compact is a no-op");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void compactRegion(byte[] bytes, byte[] bytes2) throws IOException {
        LOG.info("compactRegion is a no-op");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void majorCompact(TableName tableName) throws IOException {
        LOG.info("majorCompact is a no-op");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void majorCompactRegion(byte[] bytes) throws IOException {
        LOG.info("majorCompactRegion is a no-op");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void majorCompact(TableName tableName, byte[] bytes) throws IOException {
        LOG.info("majorCompact is a no-op");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void majorCompactRegion(byte[] bytes, byte[] bytes2) throws IOException {
        LOG.info("majorCompactRegion is a no-op");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void compactRegionServer(ServerName serverName, boolean b) throws IOException {
        LOG.info("compactRegionServer is a no-op");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void move(byte[] encodedRegionName, byte[] destServerName)
        throws HBaseIOException, MasterNotRunningException, ZooKeeperConnectionException {
        LOG.info("move is a no-op");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void assign(byte[] regionName)
        throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
        LOG.info("assign is a no-op");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void unassign(byte[] regionName, boolean force)
        throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
        LOG.info("unassign is a no-op");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void offline(byte[] regionName) throws IOException {
        throw new UnsupportedOperationException("offline");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean setBalancerRunning(boolean on, boolean synchronous)
        throws MasterNotRunningException, ZooKeeperConnectionException {
        throw new UnsupportedOperationException("setBalancerRunning");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean balancer() throws MasterNotRunningException, ZooKeeperConnectionException {
        throw new UnsupportedOperationException("balancer");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean enableCatalogJanitor(boolean enable) throws MasterNotRunningException {
        throw new UnsupportedOperationException("enableCatalogJanitor");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int runCatalogScan() throws MasterNotRunningException {
        throw new UnsupportedOperationException("runCatalogScan");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isCatalogJanitorEnabled() throws MasterNotRunningException {
        throw new UnsupportedOperationException("isCatalogJanitorEnabled");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void mergeRegions(
        byte[] encodedNameOfRegionA, byte[] encodedNameOfRegionB, boolean forcible)
        throws IOException {
        LOG.info("mergeRegions is a no-op");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void split(TableName tableName) throws IOException {
        LOG.info("split is a no-op");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void splitRegion(byte[] bytes) throws IOException {
        LOG.info("splitRegion is a no-op");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void split(TableName tableName, byte[] bytes) throws IOException {
        LOG.info("split is a no-op");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void splitRegion(byte[] bytes, byte[] bytes2) throws IOException {
        LOG.info("split is a no-op");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void shutdown() throws IOException {
        throw new UnsupportedOperationException("shutdown");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void stopMaster() throws IOException {
        throw new UnsupportedOperationException("stopMaster");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void stopRegionServer(String hostnamePort) throws IOException {
        throw new UnsupportedOperationException("stopRegionServer");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void createNamespace(NamespaceDescriptor descriptor) throws IOException {
        if (provideWarningsForNamespaces()) {
            LOG.warn("createNamespace is a no-op");
        } else {
            throw new UnsupportedOperationException("createNamespace");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void modifyNamespace(NamespaceDescriptor descriptor) throws IOException {
        if (provideWarningsForNamespaces()) {
            LOG.warn("modifyNamespace is a no-op");
        } else {
            throw new UnsupportedOperationException("modifyNamespace");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteNamespace(String name) throws IOException {
        if (provideWarningsForNamespaces()) {
            LOG.warn("deleteNamespace is a no-op");
        } else {
            throw new UnsupportedOperationException("deleteNamespace");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NamespaceDescriptor getNamespaceDescriptor(String name) throws IOException {
        if (provideWarningsForNamespaces()) {
            LOG.warn("getNamespaceDescriptor is a no-op");
            return null;
        } else {
            throw new UnsupportedOperationException("getNamespaceDescriptor");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NamespaceDescriptor[] listNamespaceDescriptors() throws IOException {
        if (provideWarningsForNamespaces()) {
            LOG.warn("listNamespaceDescriptors is a no-op");
            return new NamespaceDescriptor[0];
        } else {
            throw new UnsupportedOperationException("listNamespaceDescriptors");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public HTableDescriptor[] listTableDescriptorsByNamespace(String name) throws IOException {
        if (provideWarningsForNamespaces()) {
            LOG.warn("listTableDescriptorsByNamespace is a no-op");
            return new HTableDescriptor[0];
        } else {
            throw new UnsupportedOperationException("listTableDescriptorsByNamespace");
        }
    }

    @Override
    public List<TableDescriptor> listTableDescriptorsByNamespace(byte[] namespace)
        throws IOException {
        final String namespaceStr = Bytes.toString(namespace);
        return Arrays.asList(listTableDescriptorsByNamespace(namespaceStr));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TableName[] listTableNamesByNamespace(String name) throws IOException {
        if (provideWarningsForNamespaces()) {
            LOG.warn("listTableNamesByNamespace is a no-op");
            return new TableName[0];
        } else {
            throw new UnsupportedOperationException("listTableNamesByNamespace");
        }
    }

    private boolean provideWarningsForNamespaces() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String[] getMasterCoprocessors() {
        throw new UnsupportedOperationException("getMasterCoprocessors");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void execProcedure(String signature, String instance, Map<String, String> props)
        throws IOException {
        throw new UnsupportedOperationException("execProcedure");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public byte[] execProcedureWithRet(String signature, String instance, Map<String, String> props)
        throws IOException {
        throw new UnsupportedOperationException("execProcedureWithRet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isProcedureFinished(String signature, String instance, Map<String, String> props)
        throws IOException {
        throw new UnsupportedOperationException("isProcedureFinished");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CoprocessorRpcChannel coprocessorService() {
        throw new UnsupportedOperationException("coprocessorService");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CoprocessorRpcChannel coprocessorService(ServerName serverName) {
        throw new UnsupportedOperationException("coprocessorService");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateConfiguration(ServerName serverName) throws IOException {
        throw new UnsupportedOperationException("updateConfiguration");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateConfiguration() throws IOException {
        throw new UnsupportedOperationException("updateConfiguration");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getMasterInfoPort() throws IOException {
        throw new UnsupportedOperationException("getMasterInfoPort");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void rollWALWriter(ServerName serverName) throws IOException, FailedLogCloseException {
        throw new UnsupportedOperationException("rollWALWriter");
    }
}
