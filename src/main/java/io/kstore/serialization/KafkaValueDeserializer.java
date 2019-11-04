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
package io.kstore.serialization;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.msgpack.jackson.dataformat.MessagePackFactory;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class KafkaValueDeserializer implements Deserializer<TreeMap<String, TreeMap<String, TreeMap<Long, byte[]>>>> {

    private ObjectMapper objectMapper = new ObjectMapper(new MessagePackFactory());
    private TypeReference<TreeMap<String, TreeMap<String, TreeMap<Long, byte[]>>>> typeReference =
        new TypeReference<TreeMap<String, TreeMap<String, TreeMap<Long, byte[]>>>>() {};

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public TreeMap<String, TreeMap<String, TreeMap<Long, byte[]>>> deserialize(String topic, byte[] value) throws SerializationException {
        try {
            return objectMapper.readValue(value, typeReference);
        } catch (IOException e) {
            throw new SerializationException("Error while deserializing value", e);
        }
    }
}
