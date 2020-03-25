/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.kstore;

import io.kcache.Cache;
import io.kcache.CacheUpdateHandler;
import io.kcache.KafkaCache;
import io.kcache.KafkaCacheConfig;
import io.kcache.utils.Caches;
import io.kcache.utils.InMemoryCache;
import io.kcache.utils.Streams;
import io.kstore.schema.KafkaSchemaKey;
import io.kstore.schema.KafkaSchemaValue;
import io.kstore.serialization.KafkaSchemaKeySerde;
import io.kstore.serialization.KafkaSchemaValueSerde;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableBuilder;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.security.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

import static io.kstore.Constants.DEFAULT_FAMILY_BYTES;
import static io.kstore.Constants.KAFKASTORE_TOPIC_DEFAULT;

public class KafkaStoreConnection implements Connection {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaStoreConnection.class);

    private static final int MIN_VERSION = 1;
    private static final int MAX_VERSION = Integer.MAX_VALUE;

    private final Configuration config;
    private Cache<KafkaSchemaKey, KafkaSchemaValue> schemas;
    private final Map<TableName, KafkaStoreTable> tables = new ConcurrentHashMap<>();
    private final Set<TableName> disabledTables = new HashSet<>();

    public KafkaStoreConnection(Configuration config) {
        this(config, null, null);
    }

    public KafkaStoreConnection(Configuration config, ExecutorService pool, User user) {
        this.config = config;
        Map<String, String> configs = new HashMap<>(config.getValByRegex("kafkacache.*"));
        String topic = config.get(Constants.KAFKASTORE_TOPIC_CONFIG, KAFKASTORE_TOPIC_DEFAULT);
        String groupId = config.get(KafkaCacheConfig.KAFKACACHE_GROUP_ID_CONFIG, "kstore-1");
        String clientId = config.get(KafkaCacheConfig.KAFKACACHE_CLIENT_ID_CONFIG, groupId + "-" + topic);
        configs.put(KafkaCacheConfig.KAFKACACHE_TOPIC_CONFIG, topic);
        configs.put(KafkaCacheConfig.KAFKACACHE_GROUP_ID_CONFIG, groupId);
        configs.put(KafkaCacheConfig.KAFKACACHE_CLIENT_ID_CONFIG, clientId);
        // Always reload all tables on startup as they need to be initialized properly
        Cache<KafkaSchemaKey, KafkaSchemaValue> schemaMap = new KafkaCache<>(
            new KafkaCacheConfig(configs), new KafkaSchemaKeySerde(), new KafkaSchemaValueSerde(),
            new TableUpdateHandler(), new InMemoryCache<>());
        this.schemas = Caches.concurrentCache(schemaMap);
        this.schemas.init();
        // Initialize tables in parallel
        CompletableFuture
            .allOf(tables.values().stream()
                .map(t -> CompletableFuture.runAsync(() -> t.getCache().init()))
                .toArray(CompletableFuture[]::new))
            .join();
    }

    public Cache<KafkaSchemaKey, KafkaSchemaValue> getSchemas() {
        return schemas;
    }

    public Map<TableName, KafkaStoreTable> getTables() {
        return tables;
    }

    public KafkaSchemaValue getLatestSchemaValue(TableName tableName) {
        KafkaSchemaKey key1 = new KafkaSchemaKey(tableName.getNameAsString(), MIN_VERSION);
        KafkaSchemaKey key2 = new KafkaSchemaKey(tableName.getNameAsString(), MAX_VERSION);
        return Streams.streamOf(schemas.range(key1, true, key2, false))
            .reduce((e1, e2) -> e2)
            .map(kv -> kv.value)
            .orElse(null);
    }

    /**
     * @return Configuration instance being used by this Connection instance.
     */
    @Override
    public Configuration getConfiguration() {
        return config;
    }

    /**
     * Retrieve a Table implementation for accessing a table.
     * The returned Table is not thread safe, a new instance should be created for each using thread.
     * This is a lightweight operation, pooling or caching of the returned Table
     * is neither required nor desired.
     * <p>
     * The caller is responsible for calling {@link Table#close()} on the returned
     * table instance.
     * <p>
     * Since 0.98.1 this method no longer checks table existence. An exception
     * will be thrown if the table does not exist only when the first operation is
     * attempted.
     *
     * @param tableName the name of the table
     * @return a Table to use for interactions with this table
     */
    @Override
    public Table getTable(TableName tableName) throws IOException {
        return getTable(tableName, null);
    }

    /**
     * Retrieve a Table implementation for accessing a table.
     * The returned Table is not thread safe, a new instance should be created for each using thread.
     * This is a lightweight operation, pooling or caching of the returned Table
     * is neither required nor desired.
     * <p>
     * The caller is responsible for calling {@link Table#close()} on the returned
     * table instance.
     * <p>
     * Since 0.98.1 this method no longer checks table existence. An exception
     * will be thrown if the table does not exist only when the first operation is
     * attempted.
     *
     * @param tableName the name of the table
     * @param pool      The thread pool to use for batch operations, null to use a default pool.
     * @return a Table to use for interactions with this table
     */
    @Override
    public Table getTable(TableName tableName, ExecutorService pool) throws IOException {
        KafkaStoreTable table = tables.get(tableName);
        if (table == null) {
            getAdmin().createTable(TableDescriptorBuilder.newBuilder(tableName)
                .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(DEFAULT_FAMILY_BYTES).build())
                .build());
            table = tables.get(tableName);
        }
        return table;
    }

    /**
     * <p>
     * Retrieve a {@link BufferedMutator} for performing client-side buffering of writes. The
     * {@link BufferedMutator} returned by this method is thread-safe. This BufferedMutator will
     * use the Connection's ExecutorService. This object can be used for long lived operations.
     * </p>
     * <p>
     * The caller is responsible for calling {@link BufferedMutator#close()} on
     * the returned {@link BufferedMutator} instance.
     * </p>
     * <p>
     * This accessor will use the connection's ExecutorService and will throw an
     * exception in the main thread when an asynchronous exception occurs.
     *
     * @param tableName the name of the table
     * @return a {@link BufferedMutator} for the supplied tableName.
     */
    @Override
    public BufferedMutator getBufferedMutator(TableName tableName) throws IOException {
        return new KafkaStoreBufferedMutator(this, tableName);
    }

    /**
     * Retrieve a {@link BufferedMutator} for performing client-side buffering of writes. The
     * {@link BufferedMutator} returned by this method is thread-safe. This object can be used for
     * long lived table operations. The caller is responsible for calling
     * {@link BufferedMutator#close()} on the returned {@link BufferedMutator} instance.
     *
     * @param params details on how to instantiate the {@code BufferedMutator}.
     * @return a {@link BufferedMutator} for the supplied tableName.
     */
    @Override
    public BufferedMutator getBufferedMutator(BufferedMutatorParams params) throws IOException {
        return new KafkaStoreBufferedMutator(this, params.getTableName());
    }

    /**
     * Retrieve a RegionLocator implementation to inspect region information on a table. The returned
     * RegionLocator is not thread-safe, so a new instance should be created for each using thread.
     * <p>
     * This is a lightweight operation.  Pooling or caching of the returned RegionLocator is neither
     * required nor desired.
     * <br>
     * The caller is responsible for calling {@link RegionLocator#close()} on the returned
     * RegionLocator instance.
     * <p>
     * RegionLocator needs to be unmanaged
     *
     * @param tableName Name of the table who's region is to be examined
     * @return A RegionLocator instance
     */
    @Override
    public RegionLocator getRegionLocator(TableName tableName) throws IOException {
        return new KafkaStoreRegionLocator(tableName);
    }

    /**
     * Clear all the entries in the region location cache, for all the tables.
     * <p>
     * If you only want to clear the cache for a specific table, use
     * {@link RegionLocator#clearRegionLocationCache()}.
     * <p>
     * This may cause performance issue so use it with caution.
     */
    @Override
    public void clearRegionLocationCache() {
        throw new UnsupportedOperationException();
    }

    /**
     * Retrieve an Admin implementation to administer an HBase cluster.
     * The returned Admin is not guaranteed to be thread-safe.  A new instance should be created for
     * each using thread.  This is a lightweight operation.  Pooling or caching of the returned
     * Admin is not recommended.
     * <br>
     * The caller is responsible for calling {@link Admin#close()} on the returned
     * Admin instance.
     *
     * @return an Admin instance for cluster administration
     */
    @Override
    public Admin getAdmin() throws IOException {
        return new KafkaStoreAdmin(this);
    }

    /**
     * Abort the server or client.
     *
     * @param why Why we're aborting.
     * @param e   Throwable that caused abort. Can be null.
     */
    @Override
    public void abort(String why, Throwable e) {
    }

    /**
     * Check if the server or client was aborted.
     *
     * @return true if the server or client was aborted, false otherwise
     */
    @Override
    public boolean isAborted() {
        return false;
    }

    @Override
    public void close() throws IOException {
        for (KafkaStoreTable table : tables.values()) {
            table.getCache().close();
        }
        tables.clear();
        disabledTables.clear();
        if (schemas != null) {
            schemas.close();
            schemas = null;
        }
    }

    /**
     * Returns whether the connection is closed or not.
     *
     * @return true if this connection is closed
     */
    @Override
    public boolean isClosed() {
        return false;
    }

    /**
     * Returns an {@link TableBuilder} for creating {@link Table}.
     *
     * @param tableName the name of the table
     * @param pool      the thread pool to use for requests like batch and scan
     */
    @Override
    public TableBuilder getTableBuilder(TableName tableName, ExecutorService pool) {
        return new TableBuilder() {

            @Override
            public TableBuilder setWriteRpcTimeout(int arg0) {
                return this;
            }

            @Override
            public TableBuilder setRpcTimeout(int arg0) {
                return this;
            }

            @Override
            public TableBuilder setReadRpcTimeout(int arg0) {
                return this;
            }

            @Override
            public TableBuilder setOperationTimeout(int arg0) {
                return this;
            }

            @Override
            public Table build() {
                try {
                    return getTable(tableName, pool);
                } catch (IOException e) {
                    throw new RuntimeException("Could not create the table", e);
                }
            }
        };
    }

    public Set<TableName> getDisabledTables() {
        return disabledTables;
    }

    private class TableUpdateHandler implements CacheUpdateHandler<KafkaSchemaKey, KafkaSchemaValue> {
        @Override
        public void handleUpdate(KafkaSchemaKey schemaKey,
                                 KafkaSchemaValue schemaValue,
                                 KafkaSchemaValue oldSchemaValue) {
            TableName tableName = TableName.valueOf(schemaKey.getTableName());
            KafkaStoreTable table;
            switch (schemaValue.getAction()) {
                case CREATE:
                    table = new KafkaStoreTxTable(config, KafkaStoreConnection.this, schemaValue);
                    tables.put(tableName, table);
                    LOG.info("Created table: {}", tableName);
                    break;
                case ALTER:
                    table = tables.get(tableName);
                    table.getCache().setSchemaValue(schemaValue);
                    LOG.info("Modified table: {}", tableName);
                    break;
                case DROP:
                    table = tables.get(tableName);
                    if (table != null) {
                        try {
                            table.getCache().close();
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                        tables.remove(tableName);
                        LOG.info("Dropped table: {}", tableName);
                    }
                    break;
            }
        }
    }
}
