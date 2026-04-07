/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.udfs.bitmap;

import org.junit.jupiter.api.Test;
import org.roaringbitmap.RoaringBitmap;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RbBuildAggFunctionTest {

    private final RbBuildAggFunction function = new RbBuildAggFunction();

    @Test
    void testBuildFromMultipleValues() {
        RoaringBitmap acc = function.createAccumulator();
        function.accumulate(acc, 1);
        function.accumulate(acc, 2);
        function.accumulate(acc, 3);

        byte[] result = function.getValue(acc);
        assertNotNull(result);

        RoaringBitmap bitmap = BitmapUtils.fromBytes(result);
        assertEquals(3L, bitmap.getLongCardinality());
        assertTrue(bitmap.contains(1));
        assertTrue(bitmap.contains(2));
        assertTrue(bitmap.contains(3));
    }

    @Test
    void testDuplicateValuesAreDeduped() {
        RoaringBitmap acc = function.createAccumulator();
        function.accumulate(acc, 1);
        function.accumulate(acc, 1);
        function.accumulate(acc, 2);

        byte[] result = function.getValue(acc);
        RoaringBitmap bitmap = BitmapUtils.fromBytes(result);
        assertEquals(2L, bitmap.getLongCardinality());
    }

    @Test
    void testNullValueIsIgnored() {
        RoaringBitmap acc = function.createAccumulator();
        function.accumulate(acc, null);
        function.accumulate(acc, 5);

        byte[] result = function.getValue(acc);
        RoaringBitmap bitmap = BitmapUtils.fromBytes(result);
        assertEquals(1L, bitmap.getLongCardinality());
    }

    @Test
    void testResetAccumulator() {
        RoaringBitmap acc = function.createAccumulator();
        function.accumulate(acc, 1);
        function.accumulate(acc, 2);

        function.resetAccumulator(acc);
        assertNull(function.getValue(acc));
    }

    @Test
    void testMerge() {
        RoaringBitmap acc1 = function.createAccumulator();
        function.accumulate(acc1, 1);
        function.accumulate(acc1, 2);

        RoaringBitmap acc2 = function.createAccumulator();
        function.accumulate(acc2, 3);
        function.accumulate(acc2, 4);

        RoaringBitmap target = function.createAccumulator();
        function.merge(target, Arrays.asList(acc1, acc2));

        byte[] result = function.getValue(target);
        RoaringBitmap bitmap = BitmapUtils.fromBytes(result);

        assertEquals(4L, bitmap.getLongCardinality());
        assertTrue(bitmap.contains(1));
        assertTrue(bitmap.contains(4));
    }

    @Test
    void testMergeWithNullElementInIterable() {
        RoaringBitmap acc1 = function.createAccumulator();
        function.accumulate(acc1, 1);

        RoaringBitmap target = function.createAccumulator();
        function.merge(target, Arrays.asList(acc1, null));

        byte[] result = function.getValue(target);
        RoaringBitmap bitmap = BitmapUtils.fromBytes(result);
        assertEquals(1L, bitmap.getLongCardinality());
        assertTrue(bitmap.contains(1));
    }
}
