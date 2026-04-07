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

class RbCardinalityFunctionTest {

    private final RbCardinalityFunction function = new RbCardinalityFunction();

    @Test
    void testNullInput() {
        assertNull(function.eval(null));
    }

    @Test
    void testEmptyBitmap() {
        RoaringBitmap bitmap = new RoaringBitmap();
        byte[] bytes = BitmapUtils.toBytes(bitmap);
        assertEquals(0L, function.eval(bytes));
    }

    @Test
    void testEmptyByteArray() {
        assertEquals(0L, function.eval(new byte[0]));
    }

    @Test
    void testCardinality() {
        RoaringBitmap bitmap = new RoaringBitmap();
        bitmap.add(1);
        bitmap.add(2);
        bitmap.add(100);

        byte[] bytes = BitmapUtils.toBytes(bitmap);
        assertEquals(3L, function.eval(bytes));
    }
}