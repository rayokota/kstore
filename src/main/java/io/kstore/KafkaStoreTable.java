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

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.Service;
import com.google.protobuf.ServiceException;
import io.kcache.Cache;
import io.kcache.KeyValueIterator;
import io.kstore.schema.KafkaSchemaValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.TreeMap;

public abstract class KafkaStoreTable implements Table {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaStoreTable.class);

    private final KafkaStoreConnection conn;
    private final TableName tableName;
    private final KafkaTableCache cache;
    private final Cache<byte[], NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>>> data;

    public KafkaStoreTable(Configuration config, KafkaStoreConnection conn, KafkaSchemaValue schemaValue) {
        this.conn = conn;
        this.tableName = TableName.valueOf(schemaValue.getTableName());
        this.cache = new KafkaTableCache(config, schemaValue);
        this.data = cache.getRows();
    }

    public KafkaTableCache getCache() {
        return cache;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TableName getName() {
        return tableName;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Configuration getConfiguration() {
        return conn.getConfiguration();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TableDescriptor getDescriptor() throws IOException {
        return conn.getLatestSchemaValue(tableName).getSchema().toDesc();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void mutateRow(RowMutations rm) throws IOException {
        for (Mutation mutation : rm.getMutations()) {
            if (mutation instanceof Put) {
                put((Put) mutation);
            } else if (mutation instanceof Delete) {
                delete((Delete) mutation);
            } else if (mutation instanceof Increment) {
                increment((Increment) mutation);
            } else if (mutation instanceof Append) {
                append((Append) mutation);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Result append(Append append) throws IOException {
        throw new UnsupportedOperationException("append");
    }

    private static List<Cell> toKeyValue(byte[] row,
                                         NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> rowdata,
                                         TimeRange timeRange,
                                         Map<byte[], TimeRange> familyTimeRanges,
                                         int maxVersions) {
        List<Cell> ret = new ArrayList<>();
        for (Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> entry : rowdata.entrySet()) {
            byte[] family = entry.getKey();
            TimeRange familyTimeRange = familyTimeRanges.get(family);
            if (familyTimeRange == null) familyTimeRange = timeRange;
            for (Map.Entry<byte[], NavigableMap<Long, byte[]>> innerEntry : entry.getValue().entrySet()) {
                byte[] qualifier = innerEntry.getKey();
                int versionsAdded = 0;
                for (Map.Entry<Long, byte[]> tsToVal : innerEntry.getValue().descendingMap().entrySet()) {
                    if (versionsAdded == maxVersions)
                        break;
                    Long timestamp = tsToVal.getKey();
                    if (!familyTimeRange.withinTimeRange(timestamp))
                        continue;
                    byte[] value = tsToVal.getValue();
                    ret.add(new KeyValue(row, family, qualifier, timestamp, value));
                    versionsAdded++;
                }
            }
        }
        return ret;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean exists(Get get) throws IOException {
        Result result = get(get);
        return result != null && !result.isEmpty();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean[] exists(List<Get> gets) throws IOException {
        boolean[] result = new boolean[gets.size()];
        for (int i = 0; i < gets.size(); i++) {
            result[i] = exists(gets.get(i));
        }
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void batch(List<? extends Row> actions, Object[] results) throws IOException, InterruptedException {
        Object[] rows = batch(actions);
        if (results != null) {
            System.arraycopy(rows, 0, results, 0, rows.length);
        }
    }

    /**
     * {@inheritDoc}
     */
    public Object[] batch(List<? extends Row> actions) throws IOException, InterruptedException {
        Object[] results = new Object[actions.size()]; // same size.
        for (int i = 0; i < actions.size(); i++) {
            Row r = actions.get(i);
            if (r instanceof Delete) {
                delete((Delete) r);
                results[i] = new Result();
            }
            if (r instanceof Put) {
                put((Put) r);
                results[i] = new Result();
            }
            if (r instanceof Get) {
                Result result = get((Get) r);
                results[i] = result;
            }
            if (r instanceof Increment) {
                Result result = increment((Increment) r);
                results[i] = result;
            }
            if (r instanceof Append) {
                Result result = append((Append) r);
                results[i] = result;
            }
        }
        return results;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <R> void batchCallback(
        final List<? extends Row> actions, final Object[] results, final Batch.Callback<R> callback)
        throws IOException, InterruptedException {
        throw new UnsupportedOperationException("batchCallback");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Result get(Get get) throws IOException {
        byte[] row = get.getRow();
        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> rowData = data.get(row);
        if (rowData == null) {
            return new Result();
        }
        List<Cell> kvs = new ArrayList<>();
        Filter filter = get.getFilter();
        int maxResults = get.getMaxResultsPerColumnFamily();

        if (!get.hasFamilies()) {
            kvs = toKeyValue(row, rowData, get.getTimeRange(), get.getColumnFamilyTimeRange(), get.getMaxVersions());
            if (filter != null) {
                kvs = filter(filter, kvs);
            }
            if (maxResults >= 0 && kvs.size() > maxResults) {
                kvs = kvs.subList(0, maxResults);
            }
        } else {
            for (Map.Entry<byte[], NavigableSet<byte[]>> entry : get.getFamilyMap().entrySet()) {
                byte[] family = entry.getKey();
                NavigableMap<byte[], NavigableMap<Long, byte[]>> familyData = rowData.get(family);
                if (familyData == null)
                    continue;
                NavigableSet<byte[]> qualifiers = entry.getValue();
                if (qualifiers == null || qualifiers.isEmpty())
                    qualifiers = familyData.navigableKeySet();
                List<Cell> familyKvs = new ArrayList<>();
                TimeRange familyTimeRange = get.getColumnFamilyTimeRange().get(family);
                if (familyTimeRange == null) familyTimeRange = get.getTimeRange();
                for (byte[] qualifier : qualifiers) {
                    if (familyData.get(qualifier) == null)
                        continue;
                    List<KeyValue> tsKvs = new ArrayList<>();
                    for (Map.Entry<Long, byte[]> innerMostEntry : familyData.get(qualifier).descendingMap().entrySet()) {
                        Long timestamp = innerMostEntry.getKey();
                        if (!familyTimeRange.withinTimeRange(timestamp))
                            continue;
                        byte[] value = innerMostEntry.getValue();
                        tsKvs.add(new KeyValue(row, family, qualifier, timestamp, value));
                        if (tsKvs.size() == get.getMaxVersions()) {
                            break;
                        }
                    }
                    familyKvs.addAll(tsKvs);
                }
                if (filter != null) {
                    familyKvs = filter(filter, familyKvs);
                }
                if (maxResults >= 0 && familyKvs.size() > maxResults) {
                    familyKvs = familyKvs.subList(0, maxResults);
                }
                kvs.addAll(familyKvs);
            }
        }
        return Result.create(kvs);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Result[] get(List<Get> gets) throws IOException {
        List<Result> results = new ArrayList<>();
        for (Get g : gets) {
            results.add(get(g));
        }
        return results.toArray(new Result[0]);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultScanner getScanner(Scan scan) throws IOException {
        final List<Result> ret = new ArrayList<>();
        byte[] st = scan.getStartRow();
        if (st.length == 0) st = null;
        byte[] sp = scan.getStopRow();
        if (sp.length == 0) sp = null;
        Filter filter = scan.getFilter();
        int maxResults = scan.getMaxResultsPerColumnFamily();

        Cache<byte[], NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>>> subData =
            scan.isReversed() ? data.descendingCache() : data;

        if (st != null || sp != null) {
            boolean includeStopRow = scan.includeStopRow();
            if (st != null && sp != null && Arrays.equals(st, sp)) {
                includeStopRow = true;
            }
            subData = subData.subCache(st, scan.includeStartRow(), sp, includeStopRow);
        }

        KeyValueIterator<byte[], NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>>> iter = subData.all();
        while (iter.hasNext()) {
            io.kcache.KeyValue<byte[], NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>>> entry = iter.next();
            byte[] row = entry.key;
            NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> rowData = entry.value;
            List<Cell> kvs;
            if (!scan.hasFamilies()) {
                kvs = toKeyValue(row, rowData, scan.getTimeRange(), scan.getColumnFamilyTimeRange(), scan.getMaxVersions());
                if (filter != null) {
                    kvs = filter(filter, kvs);
                }
                if (maxResults >= 0 && kvs.size() > maxResults) {
                    kvs = kvs.subList(0, maxResults);
                }
            } else {
                kvs = new ArrayList<>();
                for (Map.Entry<byte[], NavigableSet<byte[]>> innerEntry : scan.getFamilyMap().entrySet()) {
                    byte[] family = innerEntry.getKey();
                    NavigableMap<byte[], NavigableMap<Long, byte[]>> familyData = rowData.get(family);
                    if (familyData == null)
                        continue;
                    NavigableSet<byte[]> qualifiers = innerEntry.getValue();
                    if (qualifiers == null || qualifiers.isEmpty())
                        qualifiers = familyData.navigableKeySet();
                    List<Cell> familyKvs = new ArrayList<>();
                    TimeRange familyTimeRange = scan.getColumnFamilyTimeRange().get(family);
                    if (familyTimeRange == null) familyTimeRange = scan.getTimeRange();
                    for (byte[] qualifier : qualifiers) {
                        if (familyData.get(qualifier) == null)
                            continue;
                        List<KeyValue> tsKvs = new ArrayList<>();
                        for (Map.Entry<Long, byte[]> innerMostEntry : familyData.get(qualifier).descendingMap().entrySet()) {
                            Long timestamp = innerMostEntry.getKey();
                            if (!familyTimeRange.withinTimeRange(timestamp))
                                continue;
                            byte[] value = innerMostEntry.getValue();
                            tsKvs.add(new KeyValue(row, family, qualifier, timestamp, value));
                            if (tsKvs.size() == scan.getMaxVersions()) {
                                break;
                            }
                        }
                        familyKvs.addAll(tsKvs);
                    }
                    if (filter != null) {
                        familyKvs = filter(filter, familyKvs);
                    }
                    if (maxResults >= 0 && familyKvs.size() > maxResults) {
                        familyKvs = familyKvs.subList(0, maxResults);
                    }
                    kvs.addAll(familyKvs);
                }
            }
            if (!kvs.isEmpty()) {
                ret.add(Result.create(kvs));
            }
            // Check for early out optimization
            if (filter != null && filter.filterAllRemaining()) {
                break;
            }
        }

        return new ResultScanner() {
            private final Iterator<Result> iterator = ret.iterator();

            public Iterator<Result> iterator() {
                return iterator;
            }

            public Result[] next(int nbRows) throws IOException {
                ArrayList<Result> resultSets = new ArrayList<>(nbRows);
                for (int i = 0; i < nbRows; i++) {
                    Result next = next();
                    if (next != null) {
                        resultSets.add(next);
                    } else {
                        break;
                    }
                }
                return resultSets.toArray(new Result[0]);
            }

            public Result next() throws IOException {
                try {
                    return iterator().next();
                } catch (NoSuchElementException e) {
                    return null;
                }
            }

            public void close() {
            }

            public ScanMetrics getScanMetrics() {
                return null;
            }

            public boolean renewLease() {
                return false;
            }
        };
    }

    /**
     * Follows the logical flow through the filter methods for a single row.
     *
     * @param filter HBase filter.
     * @param kvs    List of a row's KeyValues
     * @return List of KeyValues that were not filtered.
     */
    private List<Cell> filter(Filter filter, List<Cell> kvs) throws IOException {
        filter.reset();

        List<Cell> tmp = new ArrayList<>(kvs.size());
        tmp.addAll(kvs);

        /*
         * Note. Filter flow for a single row. Adapted from
         * "HBase: The Definitive Guide" (p. 163) by Lars George, 2011.
         * See Figure 4-2 on p. 163.
         */
        boolean filteredOnRowKey = false;
        List<Cell> nkvs = new ArrayList<>(tmp.size());
        for (Cell kv : tmp) {
            if (filter.filterRowKey(kv)) {
                filteredOnRowKey = true;
                break;
            }
            Filter.ReturnCode filterResult = filter.filterCell(kv);
            if (filterResult == Filter.ReturnCode.INCLUDE || filterResult == Filter.ReturnCode.INCLUDE_AND_NEXT_COL) {
                nkvs.add(filter.transformCell(kv));
            } else if (filterResult == Filter.ReturnCode.NEXT_ROW) {
                break;
            } else if (filterResult == Filter.ReturnCode.NEXT_COL || filterResult == Filter.ReturnCode.SKIP) {
                //noinspection UnnecessaryContinue
                continue;
            }
            /*
             * Ignoring next key hint which is a optimization to reduce file
             * system IO
             */
        }
        if (filter.hasFilterRow() && !filteredOnRowKey) {
            filter.filterRowCells(nkvs);
        }
        if (filter.filterRow() || filteredOnRowKey) {
            nkvs.clear();
        }
        tmp = nkvs;
        return tmp;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultScanner getScanner(byte[] family) throws IOException {
        Scan scan = new Scan();
        scan.addFamily(family);
        return getScanner(scan);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultScanner getScanner(byte[] family, byte[] qualifier) throws IOException {
        Scan scan = new Scan();
        scan.addColumn(family, qualifier);
        return getScanner(scan);
    }

    private <K, V> V forceFind(Map<K, V> map, K key, V newObject) {
        V data = map.putIfAbsent(key, newObject);
        if (data == null) {
            data = newObject;
        }
        return data;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void put(Put put) throws IOException {
        byte[] row = put.getRow();
        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> rowData = forceFind(data, row, new TreeMap<>(Bytes.BYTES_COMPARATOR));
        for (Map.Entry<byte[], List<Cell>> entry : put.getFamilyCellMap().entrySet()) {
            byte[] family = entry.getKey();
            NavigableMap<byte[], NavigableMap<Long, byte[]>> familyData = forceFind(rowData, family, new TreeMap<>(Bytes.BYTES_COMPARATOR));
            for (Cell kv : entry.getValue()) {
                long ts = kv.getTimestamp();
                if (ts == HConstants.LATEST_TIMESTAMP) {
                    ts = put.getTimestamp();
                }
                if (ts == HConstants.LATEST_TIMESTAMP) {
                    ts = System.currentTimeMillis();
                }
                byte[] qualifier = CellUtil.cloneQualifier(kv);
                NavigableMap<Long, byte[]> qualifierData = forceFind(familyData, qualifier, new TreeMap<>());
                qualifierData.put(ts, CellUtil.cloneValue(kv));
            }
        }
        data.put(row, rowData);
        data.flush();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void put(List<Put> puts) throws IOException {
        for (Put put : puts) {
            put(put);
        }
    }

    private boolean check(byte[] row, byte[] family, byte[] qualifier, CompareOperator compareOp, byte[] value) {
        return check(row, family, qualifier, compareOp, value, TimeRange.allTime());
    }

    private boolean check(byte[] row, byte[] family, byte[] qualifier, CompareOperator compareOp, byte[] value, TimeRange timeRange) {
        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> rowData = data.get(row);
        if (value == null)
            return rowData == null ||
                !rowData.containsKey(family) ||
                !rowData.get(family).containsKey(qualifier) ||
                rowData.get(family).get(qualifier).subMap(timeRange.getMin(), true, timeRange.getMax(), false).isEmpty();
        else if (rowData != null &&
            rowData.containsKey(family) &&
            rowData.get(family).containsKey(qualifier) &&
            !rowData.get(family).get(qualifier).subMap(timeRange.getMin(), true, timeRange.getMax(), false).isEmpty()) {

            byte[] oldValue = rowData.get(family).get(qualifier).subMap(timeRange.getMin(), true, timeRange.getMax(), false).lastEntry().getValue();
            int compareResult = Bytes.compareTo(value, oldValue);
            switch (compareOp) {
                case LESS:
                    return compareResult < 0;
                case LESS_OR_EQUAL:
                    return compareResult <= 0;
                case EQUAL:
                    return compareResult == 0;
                case NOT_EQUAL:
                    return compareResult != 0;
                case GREATER_OR_EQUAL:
                    return compareResult >= 0;
                case GREATER:
                    return compareResult > 0;
                default:
                    throw new IllegalArgumentException("Unknown Compare op " + compareOp.name());
            }
        } else {
            return false;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier, byte[] value, Put put) throws IOException {
        return checkAndPut(row, family, qualifier, CompareFilter.CompareOp.EQUAL, value, put);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier, CompareFilter.CompareOp compareOp, byte[] value, Put put) throws IOException {
        return checkAndPut(row, family, qualifier, CompareOperator.valueOf(compareOp.name()), value, put);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier, CompareOperator compareOp, byte[] value, Put put) throws IOException {
        if (check(row, family, qualifier, compareOp, value)) {
            put(put);
            return true;
        }
        return false;
    }

    @Override
    public void delete(Delete delete) throws IOException {
        byte[] row = delete.getRow();
        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> rowData = data.get(row);
        if (rowData == null)
            return;
        if (delete.getFamilyCellMap().isEmpty()) {
            data.remove(row);
            data.flush();
            return;
        }
        for (Map.Entry<byte[], List<Cell>> entry : delete.getFamilyCellMap().entrySet()) {
            byte[] family = entry.getKey();
            NavigableMap<byte[], NavigableMap<Long, byte[]>> familyData = rowData.get(family);
            if (familyData == null)
                continue;
            if (entry.getValue().isEmpty()) {
                rowData.remove(family);
                continue;
            }
            for (Cell kv : entry.getValue()) {
                byte[] qualifier = CellUtil.cloneQualifier(kv);
                long ts = kv.getTimestamp();
                NavigableMap<Long, byte[]> qualifierData = familyData.get(qualifier);
                Iterator<Map.Entry<byte[], NavigableMap<Long, byte[]>>> iter;
                switch (kv.getType()) {
                    case DeleteFamily:
                        iter = familyData.entrySet().iterator();
                        while (iter.hasNext()) {
                            Map.Entry<byte[], NavigableMap<Long, byte[]>> innerEntry = iter.next();
                            NavigableMap<Long, byte[]> qualData = innerEntry.getValue();
                            if (qualData != null) {
                                if (ts == HConstants.LATEST_TIMESTAMP) {
                                    iter.remove();
                                } else {
                                    qualData.subMap(0L, true, ts, true).clear();
                                }
                                if (qualData.isEmpty()) {
                                    iter.remove();
                                }
                            }
                        }
                        break;
                    case DeleteFamilyVersion:
                        iter = familyData.entrySet().iterator();
                        while (iter.hasNext()) {
                            Map.Entry<byte[], NavigableMap<Long, byte[]>> innerEntry = iter.next();
                            NavigableMap<Long, byte[]> qualData = innerEntry.getValue();
                            if (qualData != null) {
                                if (ts == HConstants.LATEST_TIMESTAMP) {
                                    qualData.pollLastEntry();
                                } else {
                                    qualData.remove(ts);
                                }
                                if (qualData.isEmpty()) {
                                    iter.remove();
                                }
                            }
                        }
                        break;
                    case DeleteColumn:
                        if (qualifierData != null) {
                            if (ts == HConstants.LATEST_TIMESTAMP) {
                                familyData.remove(qualifier);
                            } else {
                                qualifierData.subMap(0L, true, ts, true).clear();
                            }
                            if (qualifierData.isEmpty()) {
                                familyData.remove(qualifier);
                            }
                        }
                        break;
                    case Delete:
                        if (qualifierData != null) {
                            if (ts == HConstants.LATEST_TIMESTAMP) {
                                qualifierData.pollLastEntry();
                            } else {
                                qualifierData.remove(ts);
                            }
                            if (qualifierData.isEmpty()) {
                                familyData.remove(qualifier);
                            }
                        }
                        break;
                }
            }
            if (familyData.isEmpty()) {
                rowData.remove(family);
            }
        }
        if (rowData.isEmpty()) {
            data.remove(row);
        } else {
            data.put(row, rowData);
        }
        data.flush();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void delete(List<Delete> deletes) throws IOException {
        for (Delete delete : deletes) {
            delete(delete);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier, byte[] value, Delete delete) throws IOException {
        return checkAndDelete(row, family, qualifier, CompareFilter.CompareOp.EQUAL, value, delete);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier, CompareFilter.CompareOp compareOp, byte[] value, Delete delete) throws IOException {
        return checkAndDelete(row, family, qualifier, CompareOperator.valueOf(compareOp.name()), value, delete);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier, CompareOperator compareOp, byte[] value, Delete delete) throws IOException {
        if (check(row, family, qualifier, compareOp, value)) {
            delete(delete);
            return true;
        }
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean checkAndMutate(byte[] row, byte[] family, byte[] qualifier, CompareFilter.CompareOp compareOp, byte[] value, RowMutations rm) throws IOException {
        return checkAndMutate(row, family, qualifier, CompareOperator.valueOf(compareOp.name()), value, rm);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean checkAndMutate(byte[] row, byte[] family, byte[] qualifier, CompareOperator compareOp, byte[] value, RowMutations rm) throws IOException {
        if (check(row, family, qualifier, compareOp, value)) {
            mutateRow(rm);
            return true;
        }
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Result increment(Increment increment) throws IOException {
        List<Cell> kvs = new ArrayList<>();
        Map<byte[], NavigableMap<byte[], Long>> famToVal = increment.getFamilyMapOfLongs();
        for (Map.Entry<byte[], NavigableMap<byte[], Long>> ef : famToVal.entrySet()) {
            byte[] family = ef.getKey();
            NavigableMap<byte[], Long> qToVal = ef.getValue();
            for (Map.Entry<byte[], Long> eq : qToVal.entrySet()) {
                long newValue = incrementColumnValue(increment.getRow(), family, eq.getKey(), eq.getValue(), increment.getTimeRange());
                Map.Entry<Long, byte[]> timestampAndValue = data.get(increment.getRow()).get(family).get(eq.getKey()).lastEntry();
                kvs.add(new KeyValue(increment.getRow(), family, eq.getKey(), timestampAndValue.getKey(), timestampAndValue.getValue()));
            }
        }
        return Result.create(kvs);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount) throws IOException {
        return incrementColumnValue(row, family, qualifier, amount, TimeRange.allTime());
    }

    private long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount, TimeRange timeRange) throws IOException {
        if (check(row, family, qualifier, CompareOperator.EQUAL, null, timeRange)) {
            Put put = new Put(row);
            put.addColumn(family, qualifier, Bytes.toBytes(amount));
            put(put);
            return amount;
        }
        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> rowData = data.get(row);
        long newValue = Bytes.toLong(rowData.get(family).get(qualifier)
            .subMap(timeRange.getMin(), true, timeRange.getMax(), false).lastEntry().getValue()) + amount;
        rowData.get(family).get(qualifier).put(System.currentTimeMillis(), Bytes.toBytes(newValue));
        data.put(row, rowData);
        data.flush();
        return newValue;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier,
                                     long amount, Durability durability) throws IOException {
        return incrementColumnValue(row, family, qualifier, amount);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws IOException {
        // no-op
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CoprocessorRpcChannel coprocessorService(byte[] row) {
        throw new UnsupportedOperationException("coprocessorService");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T extends Service, R> Map<byte[], R> coprocessorService(final Class<T> service,
                                                                    byte[] startKey, byte[] endKey, final Batch.Call<T, R> callable)
        throws ServiceException {
        throw new UnsupportedOperationException("coprocessorService");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T extends Service, R> void coprocessorService(final Class<T> service,
                                                          byte[] startKey, byte[] endKey, final Batch.Call<T, R> callable,
                                                          final Batch.Callback<R> callback) throws ServiceException {
        throw new UnsupportedOperationException("coprocessorService");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <R extends Message> Map<byte[], R> batchCoprocessorService(
        Descriptors.MethodDescriptor methodDescriptor, Message request,
        byte[] startKey, byte[] endKey, R responsePrototype) throws ServiceException {
        throw new UnsupportedOperationException("batchCoprocessorService");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <R extends Message> void batchCoprocessorService(Descriptors.MethodDescriptor methodDescriptor,
                                                            Message request, byte[] startKey, byte[] endKey, R responsePrototype,
                                                            Batch.Callback<R> callback) throws ServiceException {
        throw new UnsupportedOperationException("batchCoprocessorService");
    }

    public RegionLocator getRegionLocator() {
        return new RegionLocator() {
            @Override
            public HRegionLocation getRegionLocation(byte[] bytes) throws IOException {
                return new HRegionLocation(null, ServerName.valueOf("localhost:0", 0));
            }

            @Override
            public HRegionLocation getRegionLocation(byte[] bytes, boolean b) throws IOException {
                return new HRegionLocation(null, ServerName.valueOf("localhost:0", 0));
            }

            @Override
            public HRegionLocation getRegionLocation(byte[] bytes, int regionId, boolean b) throws IOException {
                return new HRegionLocation(null, ServerName.valueOf("localhost:0", 0));
            }

            @Override
            public List<HRegionLocation> getRegionLocations(byte[] bytes, boolean b) throws IOException {
                return Collections.singletonList(getRegionLocation(bytes, b));
            }

            @Override
            public void clearRegionLocationCache() {
            }

            @Override
            public List<HRegionLocation> getAllRegionLocations() throws IOException {
                return null;
            }

            @Override
            public byte[][] getStartKeys() throws IOException {
                return getStartEndKeys().getFirst();
            }

            @Override
            public byte[][] getEndKeys() throws IOException {
                return getStartEndKeys().getSecond();
            }

            @Override
            public Pair<byte[][], byte[][]> getStartEndKeys() throws IOException {
                final byte[][] startKeyList = new byte[1][];
                final byte[][] endKeyList = new byte[1][];

                startKeyList[0] = new byte[0];
                endKeyList[0] = new byte[]{(byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff};

                return new Pair<>(startKeyList, endKeyList);
            }

            @Override
            public TableName getName() {
                return KafkaStoreTable.this.getName();
            }

            @Override
            public void close() throws IOException {
            }
        };
    }
}
