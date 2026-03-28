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

import org.apache.flink.table.functions.AggregateFunction;
import org.roaringbitmap.RoaringBitmap;

/**
 * rb_build_agg(value INT) -> BYTES
 *
 * <p>Aggregates a column of 32-bit integer values into a single serialized
 * RoaringBitmap. Each integer is added to the bitmap during accumulation.
 *
 * <p>This is the primary way to build a bitmap from raw integer IDs (e.g.,
 * user IDs). The result can be stored in a Fluss table with
 * {@code 'fields.<col>.agg' = 'rbm64'} for server-side OR-aggregation.
 *
 * <p>Usage in Flink SQL (after registering the JAR):
 * <pre>{@code
 * CREATE TEMPORARY FUNCTION rb_build_agg
 *     AS 'org.apache.flink.udfs.bitmap.RbBuildAggFunction';
 *
 * SELECT rb_build_agg(user_id) FROM events GROUP BY dimension;
 * }</pre>
 */
public class RbBuildAggFunction extends AggregateFunction<byte[], RoaringBitmap> {

    @Override
    public RoaringBitmap createAccumulator() {
        return new RoaringBitmap();
    }

    /**
     * Adds a single integer value to the accumulator bitmap.
     *
     * @param acc   the running bitmap accumulator
     * @param value the integer user ID to add; null values are ignored
     */
    public void accumulate(RoaringBitmap acc, Integer value) {
        if (value == null) {
            return;
        }
        acc.add(value);
    }

    /**
     * Resets the accumulator to an empty bitmap.
     *
     * @param acc the accumulator to reset
     */
    public void resetAccumulator(RoaringBitmap acc) {
        acc.clear();
    }

    /**
     * Returns the serialized bitmap as a byte array.
     *
     * @param acc the final accumulator
     * @return serialized RoaringBitmap bytes, or null if accumulator is null
     */
    @Override
    public byte[] getValue(RoaringBitmap acc) {
        if (acc == null) {
            return null;
        }
        acc.runOptimize();
        return BitmapUtils.toBytes(acc);
    }
}