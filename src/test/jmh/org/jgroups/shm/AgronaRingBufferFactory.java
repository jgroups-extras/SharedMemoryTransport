package org.jgroups.shm;

import java.nio.ByteBuffer;

import org.agrona.BitUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;
import org.agrona.concurrent.ringbuffer.RecordDescriptor;
import org.agrona.concurrent.ringbuffer.RingBufferDescriptor;

final class AgronaRingBufferFactory {

   public static ManyToOneRingBuffer createManyToOneRingBuffer(int expectedEntrySize, int capacity) {
      final int entries = Math.max(8, capacity);
      final int entryCapacity = BitUtil.align(expectedEntrySize + RecordDescriptor.HEADER_LENGTH, RecordDescriptor.ALIGNMENT);
      final int dataCapacity = BitUtil.findNextPositivePowerOfTwo(entryCapacity * entries);
      final int bufferCapacity = dataCapacity + RingBufferDescriptor.TRAILER_LENGTH;
      return new ManyToOneRingBuffer(new UnsafeBuffer(ByteBuffer.allocateDirect(bufferCapacity)));
   }

}
