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

import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.junit.jupiter.api.Test;
import org.roaringbitmap.RoaringBitmap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;

class RoaringBitmapSerializerTest {

    private final RoaringBitmapSerializer serializer = RoaringBitmapSerializer.INSTANCE;

    @Test
    void testSerializeDeserializeRoundTrip() throws Exception {
        RoaringBitmap original = new RoaringBitmap();
        original.add(1);
        original.add(100);
        original.add(100_000);

        DataOutputSerializer out = new DataOutputSerializer(128);
        serializer.serialize(original, out);

        DataInputDeserializer in = new DataInputDeserializer(out.getCopyOfBuffer());
        RoaringBitmap restored = serializer.deserialize(in);

        assertEquals(original, restored);
    }

    @Test
    void testEmptyBitmap() throws Exception {
        RoaringBitmap empty = new RoaringBitmap();

        DataOutputSerializer out = new DataOutputSerializer(32);
        serializer.serialize(empty, out);

        DataInputDeserializer in = new DataInputDeserializer(out.getCopyOfBuffer());
        RoaringBitmap restored = serializer.deserialize(in);

        assertEquals(empty, restored);
        assertEquals(0L, restored.getLongCardinality());
    }

    @Test
    void testCopy() {
        RoaringBitmap original = new RoaringBitmap();
        original.add(1);
        original.add(2);

        RoaringBitmap copy = serializer.copy(original);

        assertEquals(original, copy);
        assertNotSame(original, copy);

        // mutating copy should not affect original
        copy.add(999);
        assertEquals(2L, original.getLongCardinality());
        assertEquals(3L, copy.getLongCardinality());
    }
}
