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

import io.kstore.utils.ClusterTestHarness;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Random;
import java.util.UUID;

public abstract class AbstractTest extends ClusterTestHarness {

    public static final int MAX_VERSIONS = 6;
    public static final byte[] COLUMN_FAMILY = Bytes.toBytes("test_family");
    public static final byte[] COLUMN_FAMILY2 = Bytes.toBytes("test_family2");

    protected static DataGenerationHelper dataHelper = new DataGenerationHelper();

    private Configuration config;
    private Connection conn;
    private TableName defaultTableName;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        config = new Configuration();
        config.set("kafkacache.bootstrap.servers", bootstrapServers);
        config.set("rocksdb.enable", "false");
        config.set("rocksdb.root.dir", "/tmp/rocksdb_" + UUID.randomUUID());
        conn = new KafkaStoreConnection(config);
        defaultTableName = newTestTableName();
    }

    protected TableName newTestTableName() {
        String suffix =
            String.format("%016x-%016x", System.currentTimeMillis(), new Random().nextLong());
        return TableName.valueOf("test_table2-" + suffix);
    }

    @After
    public void tearDown() throws Exception {
        conn.close();
        super.tearDown();
    }

    // This is for when we need to look at the results outside of the current connection
    protected Connection createNewConnection() throws IOException {
        return new KafkaStoreConnection(config);
    }

    protected Connection getConnection() {
        return conn;
    }

    protected void createTable(TableName tableName) throws IOException {
        conn.getTable(tableName);
    }

    protected Table getDefaultTable() throws IOException {
        return conn.getTable(defaultTableName);
    }

    protected static class QualifierValue implements Comparable<QualifierValue> {

        protected final byte[] qualifier;
        protected final byte[] value;

        public QualifierValue(byte[] qualifier, byte[] value) {
            this.qualifier = qualifier;
            this.value = value;
        }

        @Override
        public int compareTo(QualifierValue qualifierValue) {
            return Bytes.compareTo(this.qualifier, qualifierValue.qualifier);
        }
    }
}
