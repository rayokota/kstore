package io.kstore;

import org.apache.hadoop.hbase.util.Bytes;

public class Constants {

    /**
     * Configuration keys
     */
    public static final String KAFKASTORE_TOPIC_CONFIG = "kafkastore.topic";
    public static final String KAFKASTORE_TOPIC_DEFAULT = "_tables";

    /**
     * Default column family
     */
    public static final String DEFAULT_FAMILY = "f";
    static final byte[] DEFAULT_FAMILY_BYTES = Bytes.toBytes(DEFAULT_FAMILY);
}
