/*
 * Copyright 2018 Google Inc. All Rights Reserved.
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

import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import org.junit.internal.ArrayComparisonFailure;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public abstract class AbstractTestBatch extends AbstractTest {
    /**
     * Requirement 8.1 - Batch performs a collection of Deletes, Gets, Puts, Increments, and Appends
     * on multiple rows, returning results in the same order as the requested actions.
     *
     * <p>Requirement 8.5 - A batch() should return an empty Result object for successful put/delete
     * operations and get operations with no matching column.
     *
     * <p>Requirement 8.6 - Get operations with matching values should return populated Result object
     * in a batch() operation.
     */
    @Test
    public void testBatchPutGetAndDelete() throws IOException, InterruptedException {
        testGetPutDelete(5, false);
    }

    @Test
    public void test100BatchPutGetAndDelete() throws IOException, InterruptedException {
        testGetPutDelete(100, true);
    }

    private void testGetPutDelete(int count, boolean sameQualifier)
        throws IOException, InterruptedException, ArrayComparisonFailure {
        Table table = getDefaultTable();
        // Initialize data
        byte[][] rowKeys = new byte[count][];
        byte[][] quals = new byte[count][];
        byte[][] values = new byte[count][];
        byte[] emptyRowKey = dataHelper.randomData("testrow-");

        List<Row> puts = new ArrayList<>(count);
        List<Row> gets = new ArrayList<>(count);
        List<Row> deletes = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            rowKeys[i] = dataHelper.randomData("testrow-");
            quals[i] = sameQualifier && i > 0 ? quals[0] : dataHelper.randomData("qual-");
            values[i] = dataHelper.randomData("value-");
            puts.add(new Put(rowKeys[i]).addColumn(COLUMN_FAMILY, quals[i], values[i]));
            gets.add(new Get(rowKeys[i]));
            deletes.add(new Delete(rowKeys[i]));
        }
        gets.add(new Get(emptyRowKey));

        Object[] results = new Object[count];
        table.batch(puts, results);
        for (int i = 0; i < count; i++) {
            Assert.assertTrue("Should be a Result", results[i] instanceof Result);
            Assert.assertTrue("Should be empty", ((Result) results[i]).isEmpty());
        }
        Assert.assertEquals("Batch should not have been cleared", count, puts.size());

        // Check values
        results = new Object[count + 1];
        table.batch(gets, results);
        for (int i = 0; i < count; i++) {
            Assert.assertTrue("Should be Result", results[i] instanceof Result);
            Assert.assertEquals("Should be one value", 1, ((Result) results[i]).size());
            Assert.assertArrayEquals(
                "Value is incorrect",
                values[i],
                CellUtil.cloneValue(((Result) results[i]).getColumnLatestCell(COLUMN_FAMILY, quals[i])));
        }
        Assert.assertEquals("Should be empty", 0, ((Result) results[count]).size());

        // Delete values
        results = new Object[count];
        table.batch(deletes, results);
        for (int i = 0; i < count; i++) {
            Assert.assertTrue("Should be a Result", results[i] instanceof Result);
            Assert.assertTrue("Should be empty", ((Result) results[i]).isEmpty());
        }

        // Check that delete succeeded
        results = new Object[count + 1];
        table.batch(gets, results);
        for (int i = 0; i < count; i++) {
            Assert.assertTrue("Should be empty", ((Result) results[i]).isEmpty());
        }

        table.close();
    }

    /**
     * Requirement 8.1
     */
    @Test
    public void testBatchIncrement() throws IOException, InterruptedException {
        // Initialize data
        Table table = getDefaultTable();
        byte[] rowKey1 = dataHelper.randomData("testrow-");
        byte[] qual1 = dataHelper.randomData("qual-");
        Random random = new Random();
        long value1 = random.nextLong();
        byte[] rowKey2 = dataHelper.randomData("testrow-");
        byte[] qual2 = dataHelper.randomData("qual-");
        long value2 = random.nextLong();

        // Put
        Put put1 = new Put(rowKey1).addColumn(COLUMN_FAMILY, qual1, Bytes.toBytes(value1));
        Put put2 = new Put(rowKey2).addColumn(COLUMN_FAMILY, qual2, Bytes.toBytes(value2));
        List<Row> batch = new ArrayList<>(2);
        batch.add(put1);
        batch.add(put2);
        table.batch(batch, null);

        // Increment
        Increment increment1 = new Increment(rowKey1).addColumn(COLUMN_FAMILY, qual1, 1L);
        Increment increment2 = new Increment(rowKey2).addColumn(COLUMN_FAMILY, qual2, 1L);
        batch.clear();
        batch.add(increment1);
        batch.add(increment2);
        Object[] results = new Object[2];
        table.batch(batch, results);
        Assert.assertEquals(
            "Should be value1 + 1",
            value1 + 1,
            Bytes.toLong(
                CellUtil.cloneValue(((Result) results[0]).getColumnLatestCell(COLUMN_FAMILY, qual1))));
        Assert.assertEquals(
            "Should be value2 + 1",
            value2 + 1,
            Bytes.toLong(
                CellUtil.cloneValue(((Result) results[1]).getColumnLatestCell(COLUMN_FAMILY, qual2))));

        table.close();
    }

    /**
     * Requirement 8.3 - MutateRow performs a combination of Put and Delete operations for a single
     * row.
     */
    @Test
    public void testRowMutations() throws IOException {
        // Initialize data
        Table table = getDefaultTable();
        byte[] rowKey = dataHelper.randomData("testrow-");
        byte[][] quals = dataHelper.randomData("qual-", 3);
        byte[][] values = dataHelper.randomData("value-", 3);

        // Put a couple of values
        Put put0 = new Put(rowKey).addColumn(COLUMN_FAMILY, quals[0], values[0]);
        Put put1 = new Put(rowKey).addColumn(COLUMN_FAMILY, quals[1], values[1]);
        RowMutations rm = new RowMutations(rowKey);
        rm.add(put0);
        rm.add(put1);
        table.mutateRow(rm);

        // Check
        Result result = table.get(new Get(rowKey));
        Assert.assertEquals("Should have two values", 2, result.size());
        Assert.assertArrayEquals(
            "Value #0 should exist",
            values[0],
            CellUtil.cloneValue(result.getColumnLatestCell(COLUMN_FAMILY, quals[0])));
        Assert.assertArrayEquals(
            "Value #1 should exist",
            values[1],
            CellUtil.cloneValue(result.getColumnLatestCell(COLUMN_FAMILY, quals[1])));

        // Now delete the value #1 and insert value #3
        Delete delete = new Delete(rowKey).addColumns(COLUMN_FAMILY, quals[1]);
        Put put2 = new Put(rowKey).addColumn(COLUMN_FAMILY, quals[2], values[2]);
        rm = new RowMutations(rowKey);
        rm.add(delete);
        rm.add(put2);
        table.mutateRow(rm);

        // Check
        result = table.get(new Get(rowKey));
        Assert.assertEquals("Should have two values", 2, result.size());
        Assert.assertArrayEquals(
            "Value #0 should exist",
            values[0],
            CellUtil.cloneValue(result.getColumnLatestCell(COLUMN_FAMILY, quals[0])));
        Assert.assertArrayEquals(
            "Value #2 should exist",
            values[2],
            CellUtil.cloneValue(result.getColumnLatestCell(COLUMN_FAMILY, quals[2])));

        table.close();
    }

    @Test
    public void testBatchGets() throws Exception {
        // Initialize data
        Table table = getDefaultTable();
        byte[] rowKey1 = dataHelper.randomData("testrow-");
        byte[] qual1 = dataHelper.randomData("qual-");
        byte[] value1 = dataHelper.randomData("value-");
        byte[] rowKey2 = dataHelper.randomData("testrow-");
        byte[] qual2 = dataHelper.randomData("qual-");
        byte[] value2 = dataHelper.randomData("value-");
        byte[] emptyRowKey = dataHelper.randomData("testrow-");

        Put put1 = new Put(rowKey1).addColumn(COLUMN_FAMILY, qual1, value1);
        Put put2 = new Put(rowKey2).addColumn(COLUMN_FAMILY, qual2, value2);
        List<Row> batch = new ArrayList<>(2);
        batch.add(put1);
        batch.add(put2);
        Object[] results = new Object[batch.size()];
        table.batch(batch, results);
        Assert.assertTrue("Should be a Result", results[0] instanceof Result);
        Assert.assertTrue("Should be a Result", results[1] instanceof Result);
        Assert.assertTrue("Should be empty", ((Result) results[0]).isEmpty());
        Assert.assertTrue("Should be empty", ((Result) results[1]).isEmpty());
        Assert.assertEquals("Batch should not have been cleared", 2, batch.size());

        // Check values
        Get get1 = new Get(rowKey1);
        Get get2 = new Get(rowKey2);
        Get get3 = new Get(emptyRowKey);
        batch.clear();
        batch.add(get1);
        batch.add(get2);
        batch.add(get3);
        results = new Object[batch.size()];
        table.batch(batch, results);
        Assert.assertTrue("Should be Result", results[0] instanceof Result);
        Assert.assertTrue("Should be Result", results[1] instanceof Result);
        Assert.assertTrue("Should be Result", results[2] instanceof Result);
        Assert.assertEquals("Should be one value", 1, ((Result) results[0]).size());
        Assert.assertEquals("Should be one value", 1, ((Result) results[1]).size());
        Assert.assertEquals("Should be empty", 0, ((Result) results[2]).size());
        Assert.assertArrayEquals(
            "Should be value1",
            value1,
            CellUtil.cloneValue(((Result) results[0]).getColumnLatestCell(COLUMN_FAMILY, qual1)));
        Assert.assertArrayEquals(
            "Should be value2",
            value2,
            CellUtil.cloneValue(((Result) results[1]).getColumnLatestCell(COLUMN_FAMILY, qual2)));

        table.close();
    }
}
