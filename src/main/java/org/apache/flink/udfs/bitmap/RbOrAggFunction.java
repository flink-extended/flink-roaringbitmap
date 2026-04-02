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
 * rb_or_agg(bitmap BYTES) -> BYTES
 *
 * <p>Aggregates multiple serialized RoaringBitmaps using OR (union). Used in
 * roll-up aggregation scenarios where per-group bitmaps (e.g., per minute)
 * need to be merged into a coarser granularity (e.g., per hour).
 *
 * <p>Usage in Flink SQL (after registering the JAR):
 * <pre>{@code
 * CREATE TEMPORARY FUNCTION rb_or_agg
 *     AS 'org.apache.flink.udfs.bitmap.RbOrAggFunction';
 *
 * SELECT
 *     hour_start,
 *     rb_cardinality(rb_or_agg(minute_bitmap)) AS unique_visitors
 * FROM minute_stats
 * GROUP BY hour_start;
 * }</pre>
 */
public class RbOrAggFunction extends AggregateFunction<byte[], BitmapAccumulator> {

    @Override
    public BitmapAccumulator createAccumulator() {
        return new BitmapAccumulator();
    }

    /**
     * OR-s the input bitmap into the accumulator.
     *
     * @param acc         the running bitmap accumulator
     * @param bitmapBytes serialized RoaringBitmap bytes to union in;
     *                    null and empty arrays are ignored
     */
    public void accumulate(BitmapAccumulator acc, @Nullable byte[] bitmapBytes) {
        if (bitmapBytes == null || bitmapBytes.length == 0) {
            return;
        }
        RoaringBitmap current = acc.bytes == null
                ? new RoaringBitmap()
                : BitmapUtils.fromBytes(acc.bytes);
        RoaringBitmap input = BitmapUtils.fromBytes(bitmapBytes);
        current.or(input);
        acc.bytes = BitmapUtils.toBytes(current);
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
     * Returns the serialized union bitmap as a byte array.
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