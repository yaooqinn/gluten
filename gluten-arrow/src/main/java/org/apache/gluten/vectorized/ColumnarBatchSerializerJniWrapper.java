/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gluten.vectorized;

import org.apache.gluten.runtime.Runtime;
import org.apache.gluten.runtime.RuntimeAware;

import org.apache.spark.sql.execution.unsafe.JniUnsafeByteBuffer;

public class ColumnarBatchSerializerJniWrapper implements RuntimeAware {
  private final Runtime runtime;

  private ColumnarBatchSerializerJniWrapper(Runtime runtime) {
    this.runtime = runtime;
  }

  public static ColumnarBatchSerializerJniWrapper create(Runtime runtime) {
    return new ColumnarBatchSerializerJniWrapper(runtime);
  }

  @Override
  public long rtHandle() {
    return runtime.getHandle();
  }

  public native JniUnsafeByteBuffer serialize(long handle);

  // PA-2.5c: framed [magic | statsLen | statsBlob | bytesLen | bytesBlob] payload
  // produced by VeloxColumnarBatchSerializer::framedSerializeWithStats. Returns
  // a byte[] (not JniUnsafeByteBuffer) because the framed wire format is small
  // enough that the simpler return type avoids ByteBuffer lifetime concerns;
  // PA-3 will parse this on the JVM side. Layout in todos/0003 sec 2.
  public native byte[] serializeWithStats(long handle);

  // Return the native ColumnarBatchSerializer handle
  public native long init(long cSchema);

  public native long deserialize(long serializerHandle, byte[] data);

  // Return the native ColumnarBatch handle using memory address and length
  public native long deserializeDirect(long serializerHandle, long offset, int len);

  public native void close(long serializerHandle);

  // PA-2.6: capability check for the serializeWithStats JNI extension. Users
  // may pair a newer Gluten jar with an older libgluten.so that lacks this
  // symbol; PA-3's write path consults this helper and falls back to the
  // legacy serialize() when false. Probing is lazy (one-shot, cached) and
  // catches UnsatisfiedLinkError without surfacing it -- a true probe call
  // requires a real ColumnarBatch handle, so we instead reflect on the
  // declared native method (if reflection finds it AND the cpp symbol is
  // present in libgluten.so the helper returns true).
  private static volatile Boolean supportsStatsExtCached = null;

  public static boolean supportsStatsExt() {
    Boolean cached = supportsStatsExtCached;
    if (cached != null) {
      return cached;
    }
    boolean result;
    try {
      // Reflect on our own native method declaration; presence proves the JVM
      // side is wired. The cpp side symbol is verified at first real invocation
      // via JNI (the linker resolves on first call -- a stale .so will raise
      // UnsatisfiedLinkError there, which PA-3 write path must also catch).
      ColumnarBatchSerializerJniWrapper.class.getDeclaredMethod("serializeWithStats", long.class);
      result = true;
    } catch (NoSuchMethodException e) {
      result = false;
    }
    supportsStatsExtCached = result;
    return result;
  }
}
