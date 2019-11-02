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

package io.kstore.schema;

import org.apache.hadoop.hbase.client.TableDescriptor;

import java.util.Objects;

public class KafkaSchemaValue {

    private final String tableName;
    private final Integer version;
    private final TableDescriptor schema;
    private final Action action;
    private final Integer epoch;

    public KafkaSchemaValue(String tableName,
                            Integer version,
                            TableDescriptor schema,
                            Action action,
                            Integer epoch) {
        this.tableName = tableName;
        this.version = version;
        this.schema = schema;
        this.action = action;
        this.epoch = epoch;
    }

    public String getTableName() {
        return tableName;
    }

    public Integer getVersion() {
        return this.version;
    }

    public TableDescriptor getSchema() {
        return this.schema;
    }

    public Action getAction() {
        return action;
    }

    public Integer getEpoch() {
        return this.epoch;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KafkaSchemaValue that = (KafkaSchemaValue) o;
        return tableName.equals(that.tableName) &&
            version.equals(that.version) &&
            Objects.equals(schema, that.schema) &&
            action == that.action &&
            epoch.equals(that.epoch);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableName, version, schema, action, epoch);
    }

    @Override
    public String toString() {
        return "KafkaSchemaValue{" +
            "tableName='" + tableName + '\'' +
            ", version=" + version +
            ", schema='" + schema + '\'' +
            ", action=" + action +
            ", epoch=" + epoch +
            '}';
    }

    public enum Action {
        CREATE,
        ALTER,
        DROP
    }
}
