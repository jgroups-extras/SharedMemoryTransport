package org.jgroups.shm;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.ref.Reference;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;
import java.util.Arrays;

import sun.misc.Unsafe;

public class ByteBufferUtils {

   private static final boolean FORCE_NO_UNSAFE = Boolean.getBoolean("jgroups.noUnsafe");
   private static final MethodHandle BB_BULK_PUT;
   private static final Unsafe UNSAFE;
   private static final long BYTE_BUFFER_ADDRESS_FIELD_OFFSET;
   private static final long ARRAY_BYTE_BASE_OFFSET;

   static {
      Unsafe unsafe = null;
      if (!FORCE_NO_UNSAFE) {
         try {
            unsafe = Unsafe.getUnsafe();
         } catch (final Exception ex) {
            try {
               final Field f = Unsafe.class.getDeclaredField("theUnsafe");
               f.setAccessible(true);
               unsafe = (Unsafe) f.get(null);
            } catch (final Exception ex2) {
               unsafe = null;
            }
         }
      }
      long addressOffset = -1;
      long arrayByteBaseOffset = -1;
      MethodHandle bufferBulkPut = null;
      if (unsafe != null) {
         arrayByteBaseOffset = Unsafe.ARRAY_BYTE_BASE_OFFSET;
         try {
            addressOffset = unsafe.objectFieldOffset(Buffer.class.getDeclaredField("address"));
            final ByteBuffer dummy = ByteBuffer.allocateDirect(8);
            try {
               final long address = unsafe.getLong(dummy, addressOffset);
               Reference.reachabilityFence(dummy);
               if (address == 0) {
                  addressOffset = -1;
                  unsafe = null;
               }
            } catch (Throwable t) {
               addressOffset = -1;
               unsafe = null;
            }
         } catch (Throwable ignore) {
            addressOffset = -1;
            unsafe = null;
         }
      }
      if (unsafe == null) {
         // fallback for Java >= 13 users
         try {
            Method method = ByteBuffer.class.getDeclaredMethod("put", int.class, byte[].class, int.class, int.class);
            method.setAccessible(true);
            bufferBulkPut = MethodHandles.lookup().unreflect(method);
         } catch (final Throwable ignore) {
            bufferBulkPut = null;
         }
      }
      UNSAFE = unsafe;
      ARRAY_BYTE_BASE_OFFSET = arrayByteBaseOffset;
      BYTE_BUFFER_ADDRESS_FIELD_OFFSET = addressOffset;
      BB_BULK_PUT = bufferBulkPut;
   }

   public static void copyBytes(byte[] src, int srcIndex, ByteBuffer dst, int dstIndex, int length) {
      if (dst.hasArray()) {
         System.arraycopy(src, srcIndex, dst.array(), dst.arrayOffset() + dstIndex, length);
         return;
      }
      if (dst.isDirect()) {
         if (UNSAFE != null) {
            UNSAFE.copyMemory(src, ARRAY_BYTE_BASE_OFFSET + srcIndex, null, address(dst) + dstIndex, length);
            Reference.reachabilityFence(dst);
            return;
         }
         // fallback to Java >= 13 put, if possible
         if (BB_BULK_PUT != null) {
            try {
               ByteBuffer ignore = (ByteBuffer) BB_BULK_PUT.invokeExact(dst, dstIndex, src, srcIndex, length);
               return;
            } catch (IndexOutOfBoundsException | ReadOnlyBufferException expected) {
               throw expected;
            } catch (Throwable unexpected) {
               throw new RuntimeException(unexpected);
            }
         }
         slowPathCopy(src, srcIndex, dst, dstIndex, length);
         return;
      }
      throw new IllegalArgumentException("buffer is not direct nor has any array: not supported!");
   }

   private static void slowPathCopy(byte[] src, int srcIndex, ByteBuffer dst, int dstIndex, int length) {
      // no Java 13 bulk and no Unsafe ones, let's stick to the rule
      for (int i = 0; i < length; i++) {
         dst.put(dstIndex + i, src[srcIndex + i]);
      }
   }

   public static void zeros(ByteBuffer buffer, int index, int length) {
      if (buffer.hasArray()) {
         // let's go safe
         final int from = buffer.arrayOffset() + index;
         final int to = from + length;
         Arrays.fill(buffer.array(), from, to, (byte) 0);
         return;
      }
      if (buffer.isDirect()) {
         // use memset, if available
         if (UNSAFE != null) {
            unsafeZeros(buffer, index, length);
         } else {
            safeZeros(buffer, index, length);
         }
         return;
      }
      throw new IllegalArgumentException("buffer is not direct nor has any array: not supported!");
   }

   private static void unsafeZeros(ByteBuffer buffer, int index, int length) {
      assert buffer.isDirect();
      final long bufferAddress = address(buffer) + index;
      // JDK < 11 prevent memset to be used here: let's "invite" it to work as expected
      if ((bufferAddress & 1) == 0 && length > 64) {
         UNSAFE.putByte(null, bufferAddress, (byte) 0);
         UNSAFE.setMemory(null, bufferAddress + 1, length - 1, (byte) 0);
      } else {
         UNSAFE.setMemory(null, bufferAddress, length, (byte) 0);
      }
      Reference.reachabilityFence(buffer);
   }

   private static void safeZeros(ByteBuffer buffer, int index, int length) {
      final int pre = Math.min(index % Long.BYTES, length);
      for (int i = 0; i < pre; i++) {
         buffer.put(index + i, (byte) 0);
      }
      final int alignedIndex = index + pre;
      final int remaining = length - pre;
      final int alignedLongRounds = remaining / Long.BYTES;
      for (int i = 0; i < alignedLongRounds; i++) {
         final int idx = alignedIndex + (i * Long.BYTES);
         buffer.putLong(idx, 0L);
      }
      final int postIndex = alignedIndex + (alignedLongRounds * Long.BYTES);
      final int postAlignedLongRounds = remaining & Long.BYTES;
      for (int i = 0; i < postAlignedLongRounds; i++) {
         buffer.put(postIndex + i, (byte) 0);
      }
   }

   private static long address(final ByteBuffer buffer) {
      return UNSAFE.getLong(buffer, BYTE_BUFFER_ADDRESS_FIELD_OFFSET);
   }

}
