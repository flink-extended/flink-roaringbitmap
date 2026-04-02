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

import org.apache.flink.table.functions.ScalarFunction;
import org.roaringbitmap.RoaringBitmap;

import javax.annotation.Nullable;

/**
 * rb_cardinality(bitmap BYTES) -> BIGINT
 *
 * <p>Returns the number of distinct integers stored in a serialized
 * RoaringBitmap. Used to compute unique counts (e.g., unique visitors)
 * from a bitmap column.
 *
 * <p>Usage in Flink SQL (after registering the JAR):
 * <pre>{@code
 * CREATE TEMPORARY FUNCTION rb_cardinality
 *     AS 'org.apache.flink.udfs.bitmap.RbCardinalityFunction';
 *
 * SELECT rb_cardinality(bitmap) FROM bitmaps;
 * }</pre>
 */
public class RbCardinalityFunction extends ScalarFunction {

    /**
     * Returns the cardinality (count of unique integers) of the given bitmap.
     *
     * @param bitmapBytes serialized RoaringBitmap bytes
     * @return number of unique integers, or null if input is null,
     *         or 0 if the bitmap is empty
     */
    @Nullable
    public Long eval(@Nullable byte[] bitmapBytes) {
        if (bitmapBytes == null) {
            return null;
        }

        if (bitmapBytes.length == 0) {
            return 0L;
        }

        RoaringBitmap bitmap = BitmapUtils.fromBytes(bitmapBytes);
        return bitmap.getLongCardinality();
    }
}