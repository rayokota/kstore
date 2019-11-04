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

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.annotate.JsonCreator;

public class ColumnFamilyDef {
    private final String name;

    public ColumnFamilyDef(ColumnFamilyDescriptor columnDesc) {
        this(columnDesc.getNameAsString());
    }

    @JsonCreator
    public ColumnFamilyDef(@JsonProperty("name") String name) {
        this.name = name;
    }

    @JsonProperty("name")
    public String getName() {
        return name;
    }

    public ColumnFamilyDescriptor toDesc() {
        return ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(name)).build();
    }
}
