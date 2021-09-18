package org.jgroups.shm;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.jgroups.shm.ManyToOneBoundedChannel.RecordDescriptor.*;

/**
 * The algorithm of this ring buffer has been borrowed (and simplified) from Agrona's many to one ring buffer, referenced below.
 *
 * @author Francesco Nigro
 * @see <a href="https://github.com/real-logic/agrona/blob/master/agrona/src/main/java/org/agrona/concurrent/ringbuffer/ManyToOneRingBuffer.java">ManyToOneRingBuffer</a>'s Agrona library.
 */
public class ManyToOneBoundedChannel {

   @FunctionalInterface
   public interface MessageHandler {

      /**
       * Called for the processing of each message read from a buffer in turn.
       *
       * @param msgTypeId type of the encoded message.
       * @param buffer    containing the encoded message.
       * @param index     at which the encoded message begins.
       * @param length    in bytes of the encoded message.
       */
      void onMessage(int msgTypeId, ByteBuffer buffer, int index, int length);
   }

   /**
    * Buffer has insufficient capacity to record a message or satisfy {@link #tryClaim(int, int)} request.
    */
   public static final long INSUFFICIENT_CAPACITY = tryClaimResult(-1, -1);

   /**
    * Get the {@code recordLength} as a result of the {@link #tryClaim(int, int)} operation.
    */
   private static int recordLength(final long tryClaimResult) {
      return (int) tryClaimResult;
   }

   /**
    * Get the {@code index} as a result of the {@link #tryClaim(int, int)} operation.
    */
   public static int claimedIndex(final long tryClaimResult) {
      return (int) (tryClaimResult >> 32);
   }

   /**
    * Pack a termId and recordLength into a raw tail value.
    *
    * @param index        to be packed.
    * @param recordLength to be packed.
    * @return the packed value.
    */
   private static long tryClaimResult(final int index, final int recordLength) {
      return ((long) index << 32) | (recordLength & 0xFFFF_FFFFL);
   }

   protected static final class RecordDescriptor {

      /**
       * Header length made up of fields for length, type, and then the encoded message.
       * <p>
       * Writing of a positive record length signals the message recording is complete.
       * <pre>
       *   0                   1                   2                   3
       *   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
       *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
       *  |                           Length                              |
       *  +---------------------------------------------------------------+
       *  |                            Type                               |
       *  +---------------------------------------------------------------+
       *  |                       Encoded Message                        ...
       * ...                                                              |
       *  +---------------------------------------------------------------+
       * </pre>
       */
      public static final int HEADER_LENGTH = Integer.BYTES * 2;

      /**
       * Alignment as a multiple of bytes for each record.
       */
      public static final int ALIGNMENT = HEADER_LENGTH;

      private RecordDescriptor() {
      }

      /**
       * The offset from the beginning of a record at which the message length field begins.
       *
       * @param recordOffset beginning index of the record.
       * @return offset from the beginning of a record at which the type field begins.
       */
      public static int lengthOffset(final int recordOffset) {
         return recordOffset;
      }

      /**
       * The offset from the beginning of a record at which the message type field begins.
       *
       * @param recordOffset beginning index of the record.
       * @return offset from the beginning of a record at which the type field begins.
       */
      public static int typeOffset(final int recordOffset) {
         return recordOffset + Integer.BYTES;
      }

      /**
       * The offset from the beginning of a record at which the encoded message begins.
       *
       * @param recordOffset beginning index of the record.
       * @return offset from the beginning of a record at which the encoded message begins.
       */
      public static int encodedMsgOffset(final int recordOffset) {
         return recordOffset + HEADER_LENGTH;
      }

      /**
       * Check that and message id is in the valid range.
       *
       * @param msgTypeId to be checked.
       * @throws IllegalArgumentException if the id is not in the valid range.
       */
      public static void checkTypeId(final int msgTypeId) {
         if (msgTypeId < 1) {
            final String msg = "message type id must be greater than zero, msgTypeId=" + msgTypeId;
            throw new IllegalArgumentException(msg);
         }
      }
   }

   /**
    * Record type is padding to prevent fragmentation in the buffer.
    */
   public static final int PADDING_MSG_TYPE_ID = -1;
   public static final int PRODUCER_SEQUENCE_OFFSET;
   public static final int CONSUMER_CACHE_SEQUENCE_OFFSET;
   public static final int CONSUMER_SEQUENCE_OFFSET;

   private static final int CACHE_LINE_LENGTH = 64;

   /**
    * Total length of the trailer in bytes.
    */
   public static final int TRAILER_LENGTH;

   static {
      int offset = 0;
      offset += (CACHE_LINE_LENGTH * 2);
      PRODUCER_SEQUENCE_OFFSET = offset;

      offset += (CACHE_LINE_LENGTH * 2);
      CONSUMER_CACHE_SEQUENCE_OFFSET = offset;

      offset += (CACHE_LINE_LENGTH * 2);
      CONSUMER_SEQUENCE_OFFSET = offset;

      offset += (CACHE_LINE_LENGTH * 2);
      TRAILER_LENGTH = offset;
   }

   /**
    * Align a value to the next multiple up of alignment.
    * If the value equals an alignment multiple then it is returned unchanged.
    * <p>
    * This method executes without branching. This code is designed to be use in the fast path and should not
    * be used with negative numbers. Negative numbers will result in undefined behaviour.
    *
    * @param value     to be aligned up.
    * @param alignment to be used.
    * @return the value aligned to the next boundary.
    */
   public static int align(final int value, final int alignment) {
      return (value + (alignment - 1)) & -alignment;
   }

   public static boolean isPowerOfTwo(final int value) {
      return value > 0 && ((value & (~value + 1)) == value);
   }

   /**
    * Check if buffer capacity is the correct size (a power of 2 + {@link #TRAILER_LENGTH}).
    *
    * @param capacity to be checked.
    * @throws IllegalStateException if the buffer capacity is incorrect.
    */
   public static void checkCapacity(final int capacity) {
      if (!isPowerOfTwo(capacity - TRAILER_LENGTH)) {
         final String msg = "capacity must be a positive power of 2 + TRAILER_LENGTH: capacity=" + capacity;
         throw new IllegalStateException(msg);
      }
   }

   private static final VarHandle MSG_STATE_UPDATER = MethodHandles.byteBufferViewVarHandle(int[].class, ByteOrder.nativeOrder());
   private static final VarHandle SEQUENCES_UPDATER = MethodHandles.byteBufferViewVarHandle(long[].class, ByteOrder.nativeOrder());
   private final int capacity;
   private final int maxMsgLength;
   private final int tailPositionIndex;
   private final int headCachePositionIndex;
   private final int headPositionIndex;
   private final ByteBuffer buffer;

   public ManyToOneBoundedChannel(final ByteBuffer buffer) {
      if (buffer.isReadOnly()) {
         throw new IllegalArgumentException("buffer cannot be read-only");
      }
      this.buffer = buffer;
      if (buffer.order() != ByteOrder.nativeOrder()) {
         throw new IllegalArgumentException("buffer must use " + ByteOrder.nativeOrder());
      }
      checkCapacity(buffer.capacity());
      capacity = buffer.capacity() - TRAILER_LENGTH;
      // this is going to ensure that VarHandle bytebuffer views works ok for both int/long-aligned accesses
      if (capacity % Long.BYTES != 0) {
         throw new IllegalArgumentException("buffer capacity must be a multiple of " + Long.BYTES);
      }
      maxMsgLength = capacity >> 3;
      tailPositionIndex = capacity + PRODUCER_SEQUENCE_OFFSET;
      headCachePositionIndex = capacity + CONSUMER_CACHE_SEQUENCE_OFFSET;
      headPositionIndex = capacity + CONSUMER_SEQUENCE_OFFSET;
   }

   public int capacity() {
      return capacity;
   }

   public long tryClaim(final int msgTypeId, final int length) {
      checkTypeId(msgTypeId);
      checkMsgLength(length);

      final ByteBuffer buffer = this.buffer;
      final int recordLength = length + HEADER_LENGTH;
      final int recordIndex = claimCapacity(buffer, recordLength);

      if (recordIndex < 0) {
         return INSUFFICIENT_CAPACITY;
      }
      buffer.putInt(typeOffset(recordIndex), msgTypeId);
      return tryClaimResult(encodedMsgOffset(recordIndex), recordLength);
   }

   public void commit(final long claim) {
      // unpack index and recordLength
      final int index = claimedIndex(claim);
      if (index < 0) {
         throw new IllegalArgumentException("invalid claim result");
      }
      final int lengthOffset = lengthOffset(computeRecordIndex(index));
      final int recordLength = recordLength(claim);
      if (recordLength < 0) {
         throw new IllegalArgumentException("invalid claim result");
      }
      MSG_STATE_UPDATER.setRelease(buffer, lengthOffset, recordLength);
   }

   public void abort(final long claim) {
      // unpack index and recordLength
      final int index = claimedIndex(claim);
      if (index < 0) {
         throw new IllegalArgumentException("invalid claim result");
      }
      final int recordIndex = computeRecordIndex(index);
      final int lengthOffset = lengthOffset(recordIndex);
      final int recordLength = recordLength(claim);
      if (recordLength < 0) {
         throw new IllegalArgumentException("invalid claim result");
      }
      // go back to the record header and replace any existing entry type
      buffer.putInt(typeOffset(recordIndex), PADDING_MSG_TYPE_ID);
      MSG_STATE_UPDATER.setRelease(buffer, lengthOffset, recordLength);
   }

   public int read(final MessageHandler handler) {
      return read(handler, Integer.MAX_VALUE);
   }

   public int read(final MessageHandler handler, final int messageCountLimit) {
      int messagesRead = 0;

      final ByteBuffer buffer = this.buffer;
      final int headPositionIndex = this.headPositionIndex;
      final long head = buffer.getLong(headPositionIndex);

      final int capacity = this.capacity;
      final int headIndex = (int) head & (capacity - 1);
      final int maxBlockLength = capacity - headIndex;
      int bytesRead = 0;

      try {
         while ((bytesRead < maxBlockLength) && (messagesRead < messageCountLimit)) {
            final int recordIndex = headIndex + bytesRead;
            final int recordLength = (int) MSG_STATE_UPDATER.getAcquire(buffer, lengthOffset(recordIndex));
            if (recordLength <= 0) {
               break;
            }

            bytesRead += align(recordLength, ALIGNMENT);

            final int messageTypeId = buffer.getInt(typeOffset(recordIndex));
            if (PADDING_MSG_TYPE_ID == messageTypeId) {
               continue;
            }

            handler.onMessage(messageTypeId, buffer, recordIndex + HEADER_LENGTH, recordLength - HEADER_LENGTH);
            ++messagesRead;
         }
      } finally {
         if (bytesRead > 0) {
            ByteBufferUtils.zeros(buffer, headIndex, bytesRead);
            SEQUENCES_UPDATER.setRelease(buffer, headPositionIndex, head + bytesRead);
         }
      }

      return messagesRead;
   }

   public int maxMsgLength() {
      return maxMsgLength;
   }

   public ByteBuffer buffer() {
      return buffer;
   }

   public long producerPosition() {
      return (long) SEQUENCES_UPDATER.getVolatile(buffer, tailPositionIndex);
   }

   public long consumerPosition() {
      return (long) SEQUENCES_UPDATER.getVolatile(buffer, headPositionIndex);
   }

   public int size() {
      final ByteBuffer buffer = this.buffer;
      final int headPositionIndex = this.headPositionIndex;
      final int tailPositionIndex = this.tailPositionIndex;
      long headBefore;
      long tail;
      long headAfter = (long) SEQUENCES_UPDATER.getVolatile(buffer, headPositionIndex);

      do {
         headBefore = headAfter;
         tail = (long) SEQUENCES_UPDATER.getVolatile(buffer, tailPositionIndex);
         headAfter = (long) SEQUENCES_UPDATER.getVolatile(buffer, headPositionIndex);
      } while (headAfter != headBefore);

      final long size = tail - headAfter;
      if (size < 0) {
         return 0;
      } else if (size > capacity) {
         return capacity;
      }

      return (int) size;
   }

   private void checkMsgLength(final int length) {
      if (length < 0) {
         throw new IllegalArgumentException("invalid message length=" + length);
      } else if (length > maxMsgLength) {
         throw new IllegalArgumentException("encoded message exceeds maxMsgLength=" + maxMsgLength + ", length=" + length);
      }
   }

   private int claimCapacity(final ByteBuffer buffer, final int recordLength) {
      final int requiredCapacity = align(recordLength, ALIGNMENT);
      final int capacity = this.capacity;
      final int tailPositionIndex = this.tailPositionIndex;
      final int headCachePositionIndex = this.headCachePositionIndex;
      final int mask = capacity - 1;

      long head = (long) SEQUENCES_UPDATER.getOpaque(buffer, headCachePositionIndex);

      long tail;
      int tailIndex;
      int padding;
      do {
         tail = (long) SEQUENCES_UPDATER.getVolatile(buffer, tailPositionIndex);
         final int availableCapacity = capacity - (int) (tail - head);

         if (requiredCapacity > availableCapacity) {
            head = (long) SEQUENCES_UPDATER.getVolatile(buffer, headPositionIndex);

            if (requiredCapacity > (capacity - (int) (tail - head))) {
               return -1;
            }
            SEQUENCES_UPDATER.setOpaque(buffer, headCachePositionIndex, head);
         }

         padding = 0;
         tailIndex = (int) tail & mask;
         final int toBufferEndLength = capacity - tailIndex;

         if (requiredCapacity > toBufferEndLength) {
            int headIndex = (int) head & mask;

            if (requiredCapacity > headIndex) {
               head = (long) SEQUENCES_UPDATER.getVolatile(buffer, headPositionIndex);
               headIndex = (int) head & mask;
               if (requiredCapacity > headIndex) {
                  return -1;
               }
               SEQUENCES_UPDATER.setOpaque(buffer, headCachePositionIndex, head);
            }

            padding = toBufferEndLength;
         }
      } while (!SEQUENCES_UPDATER.compareAndSet(buffer, tailPositionIndex, tail, tail + requiredCapacity + padding));

      if (0 != padding) {
         buffer.putInt(typeOffset(tailIndex), PADDING_MSG_TYPE_ID);
         MSG_STATE_UPDATER.setRelease(buffer, lengthOffset(tailIndex), padding);
         tailIndex = 0;
      }

      return tailIndex;
   }

   private int computeRecordIndex(final int index) {
      final int recordIndex = index - HEADER_LENGTH;
      if (recordIndex < 0 || recordIndex > (capacity - HEADER_LENGTH)) {
         throw new IllegalArgumentException("invalid message index " + index);
      }

      return recordIndex;
   }
}

