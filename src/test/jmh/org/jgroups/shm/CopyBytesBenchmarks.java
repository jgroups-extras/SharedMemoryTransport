package org.jgroups.shm;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import org.agrona.concurrent.UnsafeBuffer;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import static org.jgroups.shm.ByteBufferUtils.copyBytes;

@State(Scope.Benchmark)
@Fork(value = 2, jvmArgsAppend = "-Dagrona.disable.bounds.checks=true")
public class CopyBytesBenchmarks {

   @Param({"8", "64", "1024"})
   int bytes;
   @Param({"true", "false"})
   boolean direct;
   private byte[] src;
   private UnsafeBuffer agronaBuffer;
   private ByteBuffer nioBuffer;
   private int srcIndex;
   private int dstIndex;

   @Setup
   public void setup() {
      src = new byte[bytes];
      nioBuffer = direct ? ByteBuffer.allocateDirect(bytes) : ByteBuffer.allocate(bytes);
      agronaBuffer = new UnsafeBuffer(nioBuffer);
      srcIndex = 0;
      dstIndex = 0;
   }

   @Benchmark
   @BenchmarkMode({Mode.AverageTime})
   @Warmup(time = 1)
   @Measurement(time = 1)
   @OutputTimeUnit(TimeUnit.NANOSECONDS)
   public byte jgroupsCopyBytes() {
      copyBytes(src, srcIndex, nioBuffer, dstIndex, bytes);
      return nioBuffer.get(dstIndex);
   }

   @Benchmark
   @BenchmarkMode({Mode.AverageTime})
   @Warmup(time = 1)
   @Measurement(time = 1)
   @OutputTimeUnit(TimeUnit.NANOSECONDS)
   public int agronaCopyBytes() {
      agronaBuffer.putBytes(dstIndex, src, srcIndex, bytes);
      return nioBuffer.get(dstIndex);
   }

}
