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
import static org.junit.jupiter.api.Assertions.assertTrue;

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

        assertEquals(3L, result.getLongCardinality());
        assertTrue(result.contains(1));
        assertTrue(result.contains(2));
        assertTrue(result.contains(3));
    }

    @Test
    void testNullAndEmptyInputsAreIgnored() {
        RoaringBitmap bitmap = new RoaringBitmap();
        bitmap.add(10);

        for (byte[] ignored : new byte[][] {null, new byte[0]}) {
            RoaringBitmap acc = function.createAccumulator();
            function.accumulate(acc, BitmapUtils.toBytes(bitmap));
            function.accumulate(acc, ignored);

            byte[] resultBytes = function.getValue(acc);
            RoaringBitmap result = BitmapUtils.fromBytes(resultBytes);
            assertEquals(1L, result.getLongCardinality());
        }
    }
}
