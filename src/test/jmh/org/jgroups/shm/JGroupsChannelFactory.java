package org.jgroups.shm;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.agrona.BitUtil;

final class JGroupsChannelFactory {

   public static ManyToOneBoundedChannel createManyToOneBoundedChannel(int expectedEntrySize, int capacity) {
      final int entries = Math.max(8, capacity);
      final int entryCapacity = BitUtil.align(expectedEntrySize + ManyToOneBoundedChannel.RecordDescriptor.HEADER_LENGTH, ManyToOneBoundedChannel.RecordDescriptor.ALIGNMENT);
      final int dataCapacity = BitUtil.findNextPositivePowerOfTwo(entryCapacity * entries);
      final int bufferCapacity = dataCapacity + ManyToOneBoundedChannel.TRAILER_LENGTH;
      return new ManyToOneBoundedChannel(ByteBuffer.allocateDirect(bufferCapacity).order(ByteOrder.nativeOrder()));
   }
}
