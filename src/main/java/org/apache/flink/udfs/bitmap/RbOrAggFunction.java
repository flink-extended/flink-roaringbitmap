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
 * rb_or_agg(bitmap BYTES) -> BYTES
 *
 * <p>Aggregates multiple serialized RoaringBitmaps using OR (union). This is
 * used in roll-up aggregation scenarios where per-group bitmaps (e.g., per
 * minute) need to be merged into a coarser granularity (e.g., per hour) to
 * compute the total unique visitor count across groups.
 *
 * <p>Usage in Flink SQL (after registering the JAR):
 * <pre>{@code
 * CREATE TEMPORARY FUNCTION rb_or_agg
 *     AS 'org.apache.flink.udfs.bitmap.RbOrAggFunction';
 *
 * -- Roll-up: union per-minute bitmaps into hourly unique visitor bitmap
 * SELECT
 *     hour_start,
 *     rb_cardinality(rb_or_agg(minute_bitmap)) AS unique_visitors
 * FROM minute_stats
 * GROUP BY hour_start;
 * }</pre>
 */
public class RbOrAggFunction extends AggregateFunction<byte[], RoaringBitmap> {

    @Override
    public RoaringBitmap createAccumulator() {
        return new RoaringBitmap();
    }

    /**
     * OR-s the input bitmap into the accumulator.
     *
     * @param acc         the running bitmap accumulator
     * @param bitmapBytes serialized RoaringBitmap bytes to union in; null is ignored
     */
    public void accumulate(RoaringBitmap acc, byte[] bitmapBytes) {
        if (bitmapBytes == null) {
            return;
        }

        RoaringBitmap input = BitmapUtils.fromBytes(bitmapBytes);
        if (input != null) {
            acc.or(input);
        }
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
     * Returns the serialized union bitmap as a byte array.
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