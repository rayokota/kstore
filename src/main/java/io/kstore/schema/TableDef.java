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
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.codehaus.jackson.annotate.JsonCreator;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class TableDef {
    private final String name;
    private final List<ColumnFamilyDef> columnFamilies;

    public TableDef(TableDescriptor tableDesc) {
        this(tableDesc.getTableName().getNameAsString(), toColumnFamilyDefs(tableDesc.getColumnFamilies()));
    }

    private static List<ColumnFamilyDef> toColumnFamilyDefs(ColumnFamilyDescriptor[] columnFamilyDescs) {
        return Arrays.stream(columnFamilyDescs)
            .map(ColumnFamilyDef::new)
            .collect(Collectors.toList());
    }

    @JsonCreator
    public TableDef(@JsonProperty("name") String name,
                    @JsonProperty("columnFamilies") List<ColumnFamilyDef> columnFamilies) {
        this.name = name;
        this.columnFamilies = columnFamilies;
    }

    @JsonProperty("name")
    public String getName() {
        return name;
    }

    @JsonProperty("columnFamilies")
    public List<ColumnFamilyDef> getColumnFamilies() {
        return columnFamilies;
    }

    public TableDescriptor toDesc() {
        TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(TableName.valueOf(name));
        for (ColumnFamilyDef columnFamilyDef : getColumnFamilies()) {
            builder.setColumnFamily(columnFamilyDef.toDesc());
        }
        return builder.build();
    }
}
