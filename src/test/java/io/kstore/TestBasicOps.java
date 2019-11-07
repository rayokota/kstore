/*
 * Copyright 2015 Google Inc. All Rights Reserved.
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

import io.kcache.exceptions.CacheException;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class TestBasicOps extends AbstractTest {
    /**
     * Happy path for a single value.
     */
    @Test
    public void testPutGetDelete() throws IOException {
        // Initialize
        byte[] rowKey = dataHelper.randomData("testrow-");
        byte[] testQualifier = dataHelper.randomData("testQualifier-");
        byte[] testValue = dataHelper.randomData("testValue-");
        testPutGetDelete(true, rowKey, testQualifier, testValue);
    }

    /**
     * Requirement 1.2 - Rowkey, family, qualifer, and value are byte[]
     */
    @Test
    public void testBinaryPutGetDelete() throws IOException {
        // Initialize
        Random random = new Random();
        byte[] rowKey = new byte[100];
        random.nextBytes(rowKey);
        byte[] testQualifier = new byte[100];
        random.nextBytes(testQualifier);
        byte[] testValue = new byte[100];
        random.nextBytes(testValue);

        // Put
        testPutGetDelete(true, rowKey, testQualifier, testValue);
    }

    /**
     * Requirement 1.9 - Referring to a column without the qualifier implicitly sets a special "empty"
     * qualifier.
     */
    @Test
    public void testNullQualifier() throws IOException {
        // Initialize values
        Table table = getDefaultTable();
        byte[] rowKey = dataHelper.randomData("testrow-");
        byte[] testValue = dataHelper.randomData("testValue-");

        // Insert value with null qualifier
        Put put = new Put(rowKey);
        put.addColumn(COLUMN_FAMILY, null, testValue);
        table.put(put);

        // This is treated the same as an empty String (which is just an empty byte array).
        Get get = new Get(rowKey);
        get.addColumn(COLUMN_FAMILY, Bytes.toBytes(""));
        Result result = table.get(get);
        Assert.assertEquals(1, result.size());
        Assert.assertTrue(result.containsColumn(COLUMN_FAMILY, null));
        Assert.assertArrayEquals(
            testValue, CellUtil.cloneValue(result.getColumnLatestCell(COLUMN_FAMILY, null)));

        // Get as a null.  This should work.
        get = new Get(rowKey);
        get.addColumn(COLUMN_FAMILY, null);
        result = table.get(get);
        Assert.assertEquals(1, result.size());
        Assert.assertTrue(result.containsColumn(COLUMN_FAMILY, null));
        Assert.assertArrayEquals(
            testValue, CellUtil.cloneValue(result.getColumnLatestCell(COLUMN_FAMILY, null)));

        // This should return when selecting the whole family too.
        get = new Get(rowKey);
        get.addFamily(COLUMN_FAMILY);
        result = table.get(get);
        Assert.assertEquals(1, result.size());
        Assert.assertTrue(result.containsColumn(COLUMN_FAMILY, null));
        Assert.assertArrayEquals(
            testValue, CellUtil.cloneValue(result.getColumnLatestCell(COLUMN_FAMILY, null)));

        // Delete
        Delete delete = new Delete(rowKey);
        delete.addColumns(COLUMN_FAMILY, null);
        table.delete(delete);

        // Confirm deleted
        Assert.assertFalse(table.exists(get));
        table.close();
    }

    /**
     * Requirement 2.4 - Maximum cell size is 10MB by default. Can be overriden using
     * hbase.client.keyvalue.maxsize property.
     *
     * <p>Cell size includes value and key info, so the value needs to a bit less than the max to
     * work.
     */
    @Test(expected = CacheException.class)
    public void testPutGetBigValue() throws IOException {
        testPutGetDeleteExists((10 << 20) - 1024, false, true); // 10 MB - 1kB
    }

    void testPutGetDeleteExists(int size, boolean removeMetadataSize, boolean doGet)
        throws IOException {
        // Initialize variables
        byte[] testRowKey = dataHelper.randomData("testrow-");
        byte[] testQualifier = dataHelper.randomData("testQualifier-");

        int valueSize = size;
        if (removeMetadataSize) {
            int metadataSize = (20 + 4 + testRowKey.length + COLUMN_FAMILY.length + testQualifier.length);
            valueSize -= metadataSize;
        }

        byte[] testValue = new byte[valueSize];
        new Random().nextBytes(testValue);

        testPutGetDelete(doGet, testRowKey, testQualifier, testValue);
    }

    private void testPutGetDelete(
        boolean doGet, byte[] rowKey, byte[] testQualifier, byte[] testValue) throws IOException {
        Table table = getDefaultTable();

        Stopwatch stopwatch = new Stopwatch();
        // Put
        Put put = new Put(rowKey);
        put.addColumn(COLUMN_FAMILY, testQualifier, testValue);
        table.put(put);
        stopwatch.print("Put took %d ms");

        // Get
        Get get = new Get(rowKey);
        get.addColumn(COLUMN_FAMILY, testQualifier);

        // Do the get on some tests, but not others.  The rationale for that is to do performance
        // testing on large values.
        if (doGet) {
            Result result = table.get(get);
            stopwatch.print("Get took %d ms");
            Assert.assertTrue(result.containsColumn(COLUMN_FAMILY, testQualifier));
            List<Cell> cells = result.getColumnCells(COLUMN_FAMILY, testQualifier);
            Assert.assertEquals(1, cells.size());
            Assert.assertTrue(Arrays.equals(testValue, CellUtil.cloneValue(cells.get(0))));
            stopwatch.print("Verifying took %d ms");
        }
        // Delete
        Delete delete = new Delete(rowKey);
        delete.addColumns(COLUMN_FAMILY, testQualifier);
        table.delete(delete);
        stopwatch.print("Delete took %d ms");

        // Confirm deleted
        Assert.assertFalse(table.exists(get));
        stopwatch.print("Exists took %d ms");
        table.close();

        stopwatch.print("close took %d ms");
    }

    private class Stopwatch {
        long lastCheckin = System.currentTimeMillis();

        private void print(String string) {
            long now = System.currentTimeMillis();
            lastCheckin = now;
        }
    }
}
