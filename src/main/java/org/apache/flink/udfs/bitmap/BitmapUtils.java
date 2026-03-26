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

import org.roaringbitmap.RoaringBitmap;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public final class BitmapUtils {

    private BitmapUtils() {
    }

    public static RoaringBitmap fromBytes(byte[] bytes) {
        if (bytes == null) {
            return null;
        }

        try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
             DataInputStream dis = new DataInputStream(bis)) {
            RoaringBitmap bitmap = new RoaringBitmap();
            bitmap.deserialize(dis);
            return bitmap;
        } catch (IOException e) {
            throw new RuntimeException("Failed to deserialize bitmap bytes.", e);
        }
    }

    public static byte[] toBytes(RoaringBitmap bitmap) {
        if (bitmap == null) {
            return null;
        }

        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(bos)) {
            bitmap.serialize(dos);
            dos.flush();
            return bos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize bitmap.", e);
        }
    }
}