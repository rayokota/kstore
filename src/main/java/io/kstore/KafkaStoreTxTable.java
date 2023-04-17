/*
 * This file is licensed to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package io.kstore;

import com.google.common.util.concurrent.Striped;
import io.kstore.schema.KafkaSchemaValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

public class KafkaStoreTxTable extends KafkaStoreTable {

    private final transient Striped<ReadWriteLock> striped;

    public KafkaStoreTxTable(Configuration config, KafkaStoreConnection conn, KafkaSchemaValue schemaValue) {
        super(config, conn, schemaValue);
        this.striped = Striped.readWriteLock(128);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Result mutateRow(RowMutations rm) throws IOException {
        byte[] row = rm.getRow();
        Lock lock = striped.get(new Bytes(row)).writeLock();
        lock.lock();
        try {
            return super.mutateRow(rm);
        } finally {
            lock.unlock();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Result append(Append append) throws IOException {
        byte[] row = append.getRow();
        Lock lock = striped.get(new Bytes(row)).writeLock();
        lock.lock();
        try {
            return super.append(append);
        } finally {
            lock.unlock();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean exists(Get get) throws IOException {
        byte[] row = get.getRow();
        Lock lock = striped.get(new Bytes(row)).readLock();
        lock.lock();
        try {
            return super.exists(get);
        } finally {
            lock.unlock();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Result get(Get get) throws IOException {
        byte[] row = get.getRow();
        Lock lock = striped.get(new Bytes(row)).readLock();
        lock.lock();
        try {
            return super.get(get);
        } finally {
            lock.unlock();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void put(Put put) throws IOException {
        byte[] row = put.getRow();
        Lock lock = striped.get(new Bytes(row)).writeLock();
        lock.lock();
        try {
            super.put(put);
        } finally {
            lock.unlock();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier, CompareOperator compareOp, byte[] value, Put put) throws IOException {
        Lock lock = striped.get(new Bytes(row)).writeLock();
        lock.lock();
        try {
            return super.checkAndPut(row, family, qualifier, compareOp, value, put);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void delete(Delete delete) throws IOException {
        byte[] row = delete.getRow();
        Lock lock = striped.get(new Bytes(row)).writeLock();
        lock.lock();
        try {
            super.delete(delete);
        } finally {
            lock.unlock();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier, CompareOperator compareOp, byte[] value, Delete delete) throws IOException {
        Lock lock = striped.get(new Bytes(row)).writeLock();
        lock.lock();
        try {
            return super.checkAndDelete(row, family, qualifier, compareOp, value, delete);
        } finally {
            lock.unlock();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean checkAndMutate(byte[] row, byte[] family, byte[] qualifier, CompareOperator compareOp, byte[] value, RowMutations rm) throws IOException {
        Lock lock = striped.get(new Bytes(row)).writeLock();
        lock.lock();
        try {
            return super.checkAndMutate(row, family, qualifier, compareOp, value, rm);
        } finally {
            lock.unlock();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Result increment(Increment increment) throws IOException {
        byte[] row = increment.getRow();
        Lock lock = striped.get(new Bytes(row)).writeLock();
        lock.lock();
        try {
            return super.increment(increment);
        } finally {
            lock.unlock();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount) throws IOException {
        Lock lock = striped.get(new Bytes(row)).writeLock();
        lock.lock();
        try {
            return super.incrementColumnValue(row, family, qualifier, amount);
        } finally {
            lock.unlock();
        }
    }
}
