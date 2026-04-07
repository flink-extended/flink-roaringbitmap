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

import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;

/** Flink {@link org.apache.flink.api.common.typeutils.TypeSerializer} for {@link RoaringBitmap}. */
public final class RoaringBitmapSerializer extends TypeSerializerSingleton<RoaringBitmap> {

    public static final RoaringBitmapSerializer INSTANCE = new RoaringBitmapSerializer();

    private static final long serialVersionUID = 1L;

    private RoaringBitmapSerializer() {}

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public RoaringBitmap createInstance() {
        return new RoaringBitmap();
    }

    @Override
    public RoaringBitmap copy(RoaringBitmap from) {
        return from.clone();
    }

    @Override
    public RoaringBitmap copy(RoaringBitmap from, RoaringBitmap reuse) {
        return from.clone();
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(RoaringBitmap record, DataOutputView target) throws IOException {
        record.runOptimize();
        target.writeInt(record.serializedSizeInBytes());
        record.serialize(target);
    }

    @Override
    public RoaringBitmap deserialize(DataInputView source) throws IOException {
        source.readInt();
        RoaringBitmap bitmap = new RoaringBitmap();
        bitmap.deserialize(source);
        return bitmap;
    }

    @Override
    public RoaringBitmap deserialize(RoaringBitmap reuse, DataInputView source) throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        int length = source.readInt();
        target.writeInt(length);
        byte[] buffer = new byte[length];
        source.readFully(buffer);
        target.write(buffer);
    }

    @Override
    public TypeSerializerSnapshot<RoaringBitmap> snapshotConfiguration() {
        return new RoaringBitmapSerializerSnapshot();
    }

    /** Snapshot for {@link RoaringBitmapSerializer}. */
    public static final class RoaringBitmapSerializerSnapshot
            extends SimpleTypeSerializerSnapshot<RoaringBitmap> {

        public RoaringBitmapSerializerSnapshot() {
            super(() -> INSTANCE);
        }
    }
}
