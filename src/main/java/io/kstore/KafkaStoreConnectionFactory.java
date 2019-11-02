package io.kstore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;

public class KafkaStoreConnectionFactory {

    private static Connection connection = null;

    protected KafkaStoreConnectionFactory() {
    }

    public static synchronized Connection createConnection(Configuration config) {
        if (connection == null) {
            connection = new KafkaStoreConnection(config);
        }
        return connection;
    }
}
