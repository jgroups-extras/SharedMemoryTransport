package org.jgroups.shm;

import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.ToIntFunction;

import org.agrona.concurrent.MessageHandler;
import org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;
import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

@State(Scope.Group)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 10, time = 1)
@Measurement(iterations = 10, time = 1)
@Fork(value = 2, jvmArgsAppend = "-Dagrona.disable.bounds.checks=true")
public class ManyToOneStreamBenchmark {

   private static final long DELAY_PRODUCER = Long.getLong("delay.p", 0L);
   private static final long DELAY_CONSUMER = Long.getLong("delay.c", 0L);

   @Param({"agrona", "jgroups"})
   private String ringBufferType;

   @Param({"100", "1000"})
   private int bytes;

   @Param(value = {"132000"})
   int capacity;

   private int sentinelValue = -1;

   private ToIntFunction<OfferCounters> sendOperation;
   private ToIntFunction<PollCounters> receiveOperation;
   private BooleanSupplier isEmpty;

   @Setup
   public void setup() {
      if (bytes < 4) {
         throw new IllegalArgumentException("cannot configure less then 4 bytes per ring buffer entry");
      }

      final int bytes = this.bytes;

      switch (ringBufferType) {
         case "agrona":
            final ManyToOneRingBuffer agronaRingBuffer = AgronaRingBufferFactory.createManyToOneRingBuffer(bytes, capacity);
            sendOperation = counters -> {
               int index;
               while ((index = agronaRingBuffer.tryClaim(1, bytes)) < 0) {
                  counters.offersFailed++;
                  backoff();
               }
               agronaRingBuffer.buffer().putInt(index, sentinelValue);
               agronaRingBuffer.commit(index);
               counters.offersMade++;
               return index;
            };
            MessageHandler agronaHandler = (msgTypeId, buffer, index, length) -> {
               if (buffer.getInt(index) != sentinelValue) {
                  throw new RuntimeException("CANNOT HAPPEN!");
               }
               if (DELAY_CONSUMER != 0) {
                  Blackhole.consumeCPU(DELAY_CONSUMER);
               }
            };
            receiveOperation = counters -> {
               final int done = agronaRingBuffer.read(agronaHandler);
               if (done == 0) {
                  counters.pollsFailed++;
                  backoff();
               } else {
                  counters.pollsMade += done;
               }
               return done;
            };
            isEmpty = () -> agronaRingBuffer.size() == 0;
            break;
         case "jgroups":
            final ManyToOneBoundedChannel jgroupsChannel = JGroupsChannelFactory.createManyToOneBoundedChannel(bytes, capacity);
            sendOperation = counters -> {
               long claim;
               while ((claim = jgroupsChannel.tryClaim(1, bytes)) < 0) {
                  counters.offersFailed++;
                  backoff();
               }
               final int index = ManyToOneBoundedChannel.claimedIndex(claim);
               jgroupsChannel.buffer().putInt(index, sentinelValue);
               jgroupsChannel.commit(claim);
               counters.offersMade++;
               return index;
            };
            ManyToOneBoundedChannel.MessageHandler handler = (msgTypeId, buffer, index, length) -> {
               if (buffer.getInt(index) != sentinelValue) {
                  throw new RuntimeException("CANNOT HAPPEN!");
               }
               if (DELAY_CONSUMER != 0) {
                  Blackhole.consumeCPU(DELAY_CONSUMER);
               }
            };
            receiveOperation = counters -> {
               final int done = jgroupsChannel.read(handler);
               if (done == 0) {
                  counters.pollsFailed++;
                  backoff();
               } else {
                  counters.pollsMade += done;
               }
               return done;
            };
            isEmpty = () -> jgroupsChannel.size() == 0;
            break;
         default:
            throw new UnsupportedOperationException("unsupported ring buffer type");
      }
   }

   @AuxCounters
   @State(Scope.Thread)
   public static class PollCounters {

      public long pollsFailed;
      public long pollsMade;
   }

   @AuxCounters
   @State(Scope.Thread)
   public static class OfferCounters {

      public long offersFailed;
      public long offersMade;
   }

   @Benchmark
   @Group
   public int send(OfferCounters counters) {
      final int index = sendOperation.applyAsInt(counters);
      if (DELAY_PRODUCER != 0) {
         Blackhole.consumeCPU(DELAY_PRODUCER);
      }
      return index;
   }

   @Benchmark
   @Group
   public int receive(PollCounters counters) {
      return receiveOperation.applyAsInt(counters);
   }

   @TearDown(Level.Iteration)
   public void emptyRingBuffer() {
      synchronized (receiveOperation) {
         PollCounters dummy = new PollCounters();
         while (!isEmpty.getAsBoolean()) {
            while (receiveOperation.applyAsInt(dummy) == 0) {
               backoff();
            }
         }
      }
   }

   protected void backoff() {
      Thread.onSpinWait();
   }
}
