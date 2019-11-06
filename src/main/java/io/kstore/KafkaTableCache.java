/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.kstore;

import io.kcache.Cache;
import io.kcache.KafkaCache;
import io.kcache.KafkaCacheConfig;
import io.kcache.utils.InMemoryCache;
import io.kcache.utils.rocksdb.RocksDBCache;
import io.kstore.schema.KafkaSchemaValue;
import io.kstore.serialization.KryoSerde;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.common.serialization.Serdes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import static io.kstore.Constants.ROCKS_DB_ENABLE_CONFIG;
import static io.kstore.Constants.ROCKS_DB_ROOT_DIR_CONFIG;
import static io.kstore.Constants.ROCKS_DB_ROOT_DIR_DEFAULT;

public class KafkaTableCache implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaTableCache.class);

    private final Configuration config;
    private KafkaSchemaValue schemaValue;
    private Cache<byte[], NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>>> rows;

    public KafkaTableCache(Configuration config, KafkaSchemaValue schemaValue) {
        this.config = config;
        this.schemaValue = schemaValue;
        TableName tableName = TableName.valueOf(schemaValue.getTableName());
        int epoch = schemaValue.getEpoch();
        Map<String, String> configs = new HashMap<>(config.getValByRegex("kafkacache.*"));
        String topic = tableName.getNamespaceAsString() + "_" + tableName.getQualifierAsString() + "_" + epoch;
        String groupId = config.get(KafkaCacheConfig.KAFKACACHE_GROUP_ID_CONFIG, "kstore-1");
        String clientId = config.get(KafkaCacheConfig.KAFKACACHE_CLIENT_ID_CONFIG, groupId + "-" + topic);
        configs.put(KafkaCacheConfig.KAFKACACHE_TOPIC_CONFIG, topic);
        configs.put(KafkaCacheConfig.KAFKACACHE_GROUP_ID_CONFIG, groupId);
        configs.put(KafkaCacheConfig.KAFKACACHE_CLIENT_ID_CONFIG, clientId);
        configs.put(KafkaCacheConfig.KAFKACACHE_ENABLE_OFFSET_COMMIT_CONFIG, "true");
        String enableRocksDbStr = config.get(ROCKS_DB_ENABLE_CONFIG, "true");
        boolean enableRocksDb = Boolean.parseBoolean(enableRocksDbStr);
        String rootDir = config.get(ROCKS_DB_ROOT_DIR_CONFIG, ROCKS_DB_ROOT_DIR_DEFAULT);
        Comparator<byte[]> cmp = Bytes.BYTES_COMPARATOR;
        Cache<byte[], NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>>> cache = enableRocksDb
            ? new RocksDBCache<>(topic, "rocksdb", rootDir, Serdes.ByteArray(), new KryoSerde<>(), cmp)
            : new InMemoryCache<>(new ConcurrentSkipListMap<>(cmp));
        Cache<byte[], NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>>> rowMap = new KafkaCache<>(
            new KafkaCacheConfig(configs), Serdes.ByteArray(), new KryoSerde<>(), null, cache);
        this.rows = rowMap;
    }

    public void init() {
        rows.init();
        LOG.info("Initialized table: {}, version: {}", schemaValue.getTableName(), schemaValue.getVersion());
    }

    public KafkaSchemaValue getSchemaValue() {
        return schemaValue;
    }

    public void setSchemaValue(KafkaSchemaValue schemaValue) {
        this.schemaValue = schemaValue;
    }

    public Cache<byte[], NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>>> getRows() {
        return rows;
    }

    public void close() throws IOException {
        if (rows != null) {
            rows.close();
            rows = null;
        }
    }
}
