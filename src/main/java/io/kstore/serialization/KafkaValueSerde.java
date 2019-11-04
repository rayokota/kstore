/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;
import java.util.TreeMap;

public class KafkaValueSerde implements Serde<TreeMap<String, TreeMap<String, TreeMap<Long, byte[]>>>> {

    private final Serde<TreeMap<String, TreeMap<String, TreeMap<Long, byte[]>>>> inner;

    public KafkaValueSerde() {
        inner = Serdes.serdeFrom(new KafkaValueSerializer(), new KafkaValueDeserializer());
    }

    @Override
    public Serializer<TreeMap<String, TreeMap<String, TreeMap<Long, byte[]>>>> serializer() {
        return inner.serializer();
    }

    @Override
    public Deserializer<TreeMap<String, TreeMap<String, TreeMap<Long, byte[]>>>> deserializer() {
        return inner.deserializer();
    }

    @Override
    public void configure(final Map<String, ?> serdeConfig, final boolean isSerdeForRecordKeys) {
        inner.serializer().configure(serdeConfig, isSerdeForRecordKeys);
        inner.deserializer().configure(serdeConfig, isSerdeForRecordKeys);
    }

    @Override
    public void close() {
        inner.serializer().close();
        inner.deserializer().close();
    }
}