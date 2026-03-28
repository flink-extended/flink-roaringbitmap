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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class RbOrAggFunctionTest {

    private final RbOrAggFunction function = new RbOrAggFunction();

    @Test
    void testOrAgg() {
        RoaringBitmap left = new RoaringBitmap();
        left.add(1);
        left.add(2);

        RoaringBitmap right = new RoaringBitmap();
        right.add(2);
        right.add(3);

        RoaringBitmap acc = function.createAccumulator();
        function.accumulate(acc, BitmapUtils.toBytes(left));
        function.accumulate(acc, BitmapUtils.toBytes(right));

        byte[] resultBytes = function.getValue(acc);
        RoaringBitmap result = BitmapUtils.fromBytes(resultBytes);

        // union of {1,2} and {2,3} = {1,2,3}
        assertEquals(3L, result.getLongCardinality());
        assertEquals(true, result.contains(1));
        assertEquals(true, result.contains(2));
        assertEquals(true, result.contains(3));
    }

    @Test
    void testNullInputIsIgnored() {
        RoaringBitmap bitmap = new RoaringBitmap();
        bitmap.add(10);

        RoaringBitmap acc = function.createAccumulator();
        function.accumulate(acc, BitmapUtils.toBytes(bitmap));
        function.accumulate(acc, null);

        byte[] resultBytes = function.getValue(acc);
        RoaringBitmap result = BitmapUtils.fromBytes(resultBytes);

        assertEquals(1L, result.getLongCardinality());
    }

    @Test
    void testResetAccumulator() {
        RoaringBitmap bitmap = new RoaringBitmap();
        bitmap.add(1);
        bitmap.add(2);

        RoaringBitmap acc = function.createAccumulator();
        function.accumulate(acc, BitmapUtils.toBytes(bitmap));
        function.resetAccumulator(acc);

        byte[] resultBytes = function.getValue(acc);
        RoaringBitmap result = BitmapUtils.fromBytes(resultBytes);
        assertEquals(0L, result.getLongCardinality());
    }
}