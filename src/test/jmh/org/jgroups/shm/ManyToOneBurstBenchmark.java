package org.jgroups.shm;

import org.agrona.BitUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;
import org.agrona.concurrent.ringbuffer.RecordDescriptor;
import org.agrona.concurrent.ringbuffer.RingBufferDescriptor;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.BenchmarkParams;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.Consumer;

@State(Scope.Benchmark)
@Fork(value = 2, jvmArgsAppend = "-Dagrona.disable.bounds.checks=true")
public class ManyToOneBurstBenchmark {

   private static final int MESSAGE_COUNT_LIMIT = 16;

   @Param({"1", "100"})
   private int burstLength;

   @Param({"agrona", "jgroups"})
   private String ringBufferType;

   private final AtomicBoolean running = new AtomicBoolean(true);
   private final AtomicInteger producerId = new AtomicInteger();
   private ProducerState[] producerStates;

   private Thread consumerThread;

   private Consumer<int[]> sendBurstOperation;

   @Setup
   public synchronized void setup(BenchmarkParams params) {
      producerStates = new ProducerState[params.getThreads()];

      Runnable consumerTask;

      switch (ringBufferType) {
         case "agrona":
            final ManyToOneRingBuffer agronaRingBuffer = createAgronaRingBuffer(Integer.BYTES, burstLength, params.getThreads());
            consumerTask = createAgronaConsumer(agronaRingBuffer, producerStates, running, MESSAGE_COUNT_LIMIT);
            sendBurstOperation = burst -> {
               sendAgronaBurst(agronaRingBuffer, burst);
            };
            break;
         case "jgroups":
            final ManyToOneBoundedChannel jgroupsChannel = createJGroupsChannel(Integer.BYTES, burstLength, params.getThreads());
            consumerTask = createJGroupsConsumer(jgroupsChannel, producerStates, running, MESSAGE_COUNT_LIMIT);
            sendBurstOperation = burst -> {
               sendJGroupsBurst(jgroupsChannel, burst);
            };
            break;
         default:
            throw new UnsupportedOperationException("unsupported ring buffer type");
      }

      consumerThread = new Thread(consumerTask);

      consumerThread.setName("consumer");
      consumerThread.start();
   }

   private static Runnable createAgronaConsumer(ManyToOneRingBuffer ringBuffer,
                                                ProducerState[] states,
                                                AtomicBoolean running,
                                                int messageCountLimit) {
      return () -> {
         while (true) {
            final int msgCount = ringBuffer.read((msgTypeId, buffer, index, length) -> {
               final int value = buffer.getInt(index);
               if (value >= 0) {
                  states[value].completed();
               }
            }, messageCountLimit);
            if (0 == msgCount && !running.get()) {
               break;
            }
         }
      };
   }

   private static Runnable createJGroupsConsumer(ManyToOneBoundedChannel ringBuffer,
                                                 ProducerState[] states,
                                                 AtomicBoolean running,
                                                 int messageCountLimit) {
      return () -> {
         while (true) {
            final int msgCount = ringBuffer.read((msgTypeId, buffer, index, length) -> {
               final int value = buffer.getInt(index);
               if (value >= 0) {
                  states[value].completed();
               }
            }, messageCountLimit);
            if (0 == msgCount && !running.get()) {
               break;
            }
         }
      };
   }

   @TearDown
   public synchronized void tearDown() throws Exception {
      running.set(false);
      consumerThread.join();
   }

   private static ManyToOneRingBuffer createAgronaRingBuffer(int expectedEntrySize, int burstSize, int producers) {
      final int entries = Math.max(8, burstSize);
      final int entryCapacity = BitUtil.align(expectedEntrySize + RecordDescriptor.HEADER_LENGTH, RecordDescriptor.ALIGNMENT);
      final int dataCapacity = BitUtil.findNextPositivePowerOfTwo(entryCapacity * entries * producers);
      final int bufferCapacity = dataCapacity + RingBufferDescriptor.TRAILER_LENGTH;
      return new ManyToOneRingBuffer(new UnsafeBuffer(ByteBuffer.allocateDirect(bufferCapacity)));
   }

   private static ManyToOneBoundedChannel createJGroupsChannel(int expectedEntrySize, int burstSize, int producers) {
      final int entries = Math.max(8, burstSize);
      final int entryCapacity = BitUtil.align(expectedEntrySize + ManyToOneBoundedChannel.RecordDescriptor.HEADER_LENGTH, ManyToOneBoundedChannel.RecordDescriptor.ALIGNMENT);
      final int dataCapacity = BitUtil.findNextPositivePowerOfTwo(entryCapacity * entries * producers);
      final int bufferCapacity = dataCapacity + ManyToOneBoundedChannel.TRAILER_LENGTH;
      return new ManyToOneBoundedChannel(ByteBuffer.allocateDirect(bufferCapacity).order(ByteOrder.nativeOrder()));
   }

   @State(Scope.Thread)
   public static class ProducerState {

      private static final int TRUE = 1;
      private static final int FALSE = 0;
      private static final AtomicIntegerFieldUpdater<ProducerState> COMPLETED_UPDATER = AtomicIntegerFieldUpdater.newUpdater(ProducerState.class, "completed");
      /**
       * Using updater + int to allow JMH to pad it to avoid false sharing with other producers states
       */
      private volatile int completed;
      private int id;
      private int[] burst;

      @Setup
      public void setup(final ManyToOneBurstBenchmark sharedState) {
         id = sharedState.producerId.getAndIncrement();
         burst = new int[sharedState.burstLength];
         Arrays.fill(burst, Integer.MIN_VALUE);
         // last value must be the producer index/id to allow consumer to "complete" burst
         burst[burst.length - 1] = id;
         completed = FALSE;
         sharedState.producerStates[id] = this;
      }

      public void completed() {
         COMPLETED_UPDATER.lazySet(this, TRUE);
      }

      private int waitCompletionAndReset() {
         int value;
         while ((value = completed) != TRUE) {
            Thread.onSpinWait();
         }
         COMPLETED_UPDATER.lazySet(this, FALSE);
         return value;
      }
   }

   private static void sendAgronaBurst(ManyToOneRingBuffer ringBuffer, final int[] burst) {
      for (int value : burst) {
         int index;
         while ((index = ringBuffer.tryClaim(1, Integer.BYTES)) <= 0) {
            Thread.onSpinWait();
         }
         ringBuffer.buffer().putInt(index, value);
         ringBuffer.commit(index);
      }
   }

   private static void sendJGroupsBurst(ManyToOneBoundedChannel ringBuffer, final int[] burst) {
      for (int value : burst) {
         long claim;
         while ((claim = ringBuffer.tryClaim(1, Integer.BYTES)) == ManyToOneBoundedChannel.INSUFFICIENT_CAPACITY) {
            Thread.onSpinWait();
         }
         ringBuffer.buffer().putInt(ManyToOneBoundedChannel.claimedIndex(claim), value);
         ringBuffer.commit(claim);
      }
   }

   public int sendAndAwaitBurstCompletion(ProducerState producer) {
      this.sendBurstOperation.accept(producer.burst);
      return producer.waitCompletionAndReset();
   }

   @Benchmark
   @BenchmarkMode({Mode.AverageTime})
   @Warmup(time = 1)
   @Measurement(time = 1)
   @OutputTimeUnit(TimeUnit.MICROSECONDS)
   @Threads(1)
   public int test1Producer(final ProducerState state) {
      return sendAndAwaitBurstCompletion(state);
   }

   @Benchmark
   @BenchmarkMode({Mode.AverageTime})
   @Warmup(time = 1)
   @Measurement(time = 1)
   @OutputTimeUnit(TimeUnit.MICROSECONDS)
   @Threads(2)
   public int test2Producers(final ProducerState state) {
      return sendAndAwaitBurstCompletion(state);
   }

   @Benchmark
   @BenchmarkMode({Mode.AverageTime})
   @Warmup(time = 1)
   @Measurement(time = 1)
   @OutputTimeUnit(TimeUnit.MICROSECONDS)
   @Threads(3)
   public int test3Producers(final ProducerState state) {
      return sendAndAwaitBurstCompletion(state);
   }
}