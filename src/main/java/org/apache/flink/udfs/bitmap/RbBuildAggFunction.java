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

import javax.annotation.Nullable;

/**
 * rb_build_agg(value INT) -> BYTES
 *
 * <p>Aggregates a column of 32-bit integer values into a single serialized
 * RoaringBitmap. Each integer is added to the bitmap during accumulation.
 *
 * <p>Usage in Flink SQL (after registering the JAR):
 * <pre>{@code
 * CREATE TEMPORARY FUNCTION rb_build_agg
 *     AS 'org.apache.flink.udfs.bitmap.RbBuildAggFunction';
 *
 * SELECT rb_build_agg(user_id) FROM events GROUP BY dimension;
 * }</pre>
 */
public class RbBuildAggFunction extends AggregateFunction<byte[], BitmapAccumulator> {

    @Override
    public BitmapAccumulator createAccumulator() {
        return new BitmapAccumulator();
    }

    /**
     * Adds a single integer value to the accumulator bitmap.
     *
     * @param acc   the running bitmap accumulator
     * @param value the integer user ID to add; null values are ignored
     */
    public void accumulate(BitmapAccumulator acc, @Nullable Integer value) {
        if (value == null) {
            return;
        }
        RoaringBitmap bitmap = acc.bytes == null
                ? new RoaringBitmap()
                : BitmapUtils.fromBytes(acc.bytes);
        bitmap.add(value);
        acc.bytes = BitmapUtils.toBytes(bitmap);
    }

    /**
     * Merges multiple partial accumulators into one.
     * Required for session window aggregation and two-phase distributed aggregation.
     *
     * @param acc the target accumulator
     * @param it  the partial accumulators to merge in
     */
    public void merge(BitmapAccumulator acc, Iterable<BitmapAccumulator> it) {
        RoaringBitmap result = acc.bytes == null
                ? new RoaringBitmap()
                : BitmapUtils.fromBytes(acc.bytes);
        for (BitmapAccumulator other : it) {
            if (other != null && other.bytes != null) {
                RoaringBitmap otherBitmap = BitmapUtils.fromBytes(other.bytes);
                if (otherBitmap != null) {
                    result.or(otherBitmap);
                }
            }
        }
        acc.bytes = BitmapUtils.toBytes(result);
    }

    /**
     * Resets the accumulator to an empty state.
     *
     * @param acc the accumulator to reset
     */
    public void resetAccumulator(BitmapAccumulator acc) {
        acc.bytes = null;
    }

    /**
     * Returns the serialized bitmap as a byte array.
     *
     * @param acc the final accumulator
     * @return serialized RoaringBitmap bytes, or null if nothing was accumulated
     */
    @Override
    @Nullable
    public byte[] getValue(BitmapAccumulator acc) {
        if (acc == null || acc.bytes == null) {
            return null;
        }
        RoaringBitmap bitmap = BitmapUtils.fromBytes(acc.bytes);
        if (bitmap == null) {
            return null;
        }
        bitmap.runOptimize();
        return BitmapUtils.toBytes(bitmap);
    }
}