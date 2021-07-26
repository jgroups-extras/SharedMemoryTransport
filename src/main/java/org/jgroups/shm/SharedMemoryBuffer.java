package org.jgroups.shm;


import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.*;
import org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;
import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.jgroups.util.DefaultThreadFactory;
import org.jgroups.util.Runner;
import org.jgroups.util.ThreadFactory;
import org.jgroups.util.Util;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiConsumer;

/**
 * Wraps {@link org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer}. Can be used for writing; reading is enabled
 * by setting a consumer ({@link #setConsumer(BiConsumer)}).
 * @author Bela Ban
 * @since  1.0.0
 */
public class SharedMemoryBuffer implements MessageHandler, Closeable {
    protected final String         file_name;   // name of the shared memory-mapped file (e.g. /tmp/shm/uuid-1
    protected BiConsumer<ByteBuffer,Integer> consumer; // a received message calls consumer.receive();
    protected FileChannel          channel;     // the memory-mapped file
    protected ManyToOneRingBuffer  rb;
    protected final Runner         runner;
    protected IdleStrategy         idle_strategy;
    protected boolean              delete_file_on_exit;
    protected final LongAdder      insufficient_capacity=new LongAdder();


    public SharedMemoryBuffer(String file_name, int buffer_length, boolean create) throws IOException {
        this.file_name=file_name;
        // idle stragegy spins, the yields, then parks between 1000ns and 64ms by default
        idle_strategy=new BackoffIdleStrategy(BackoffIdleStrategy.DEFAULT_MAX_SPINS,
                                              BackoffIdleStrategy.DEFAULT_MAX_YIELDS,
                                              BackoffIdleStrategy.DEFAULT_MIN_PARK_PERIOD_NS,
                                              BackoffIdleStrategy.DEFAULT_MAX_PARK_PERIOD_NS<<6);
        init(buffer_length, create);
        ThreadFactory tf=new DefaultThreadFactory("runner", true, true);
        runner=new Runner(tf, String.format("shm-%s", file_name), this::doWork, null);
    }


    public SharedMemoryBuffer idleStrategy(IdleStrategy s) {idle_strategy=Objects.requireNonNull(s); return this;}
    public long               insufficientCapacity()       {return insufficient_capacity.sum();}
    public SharedMemoryBuffer resetStats()                 {insufficient_capacity.reset(); return this;}

    public SharedMemoryBuffer maxSleep(long m) {
        long max_sleep_ns=TimeUnit.NANOSECONDS.convert(m, TimeUnit.MILLISECONDS);
        idle_strategy=new BackoffIdleStrategy(BackoffIdleStrategy.DEFAULT_MAX_SPINS, BackoffIdleStrategy.DEFAULT_MAX_YIELDS,
                                              BackoffIdleStrategy.DEFAULT_MIN_PARK_PERIOD_NS, max_sleep_ns);
        return this;
    }

    public SharedMemoryBuffer deleteFileOnExit(boolean f)  {
        if((delete_file_on_exit=f) == true) {
            File tmp=new File(file_name);
            tmp.deleteOnExit();
        }
        return this;
    }

    public SharedMemoryBuffer setConsumer(BiConsumer<ByteBuffer,Integer> c) {
        consumer=Objects.requireNonNull(c);
        runner.start();
        return this;
    }

    public void write(byte[] buf, int offset, int length) {
        int claim_index=rb.tryClaim(1, length);
        if(claim_index > 0) {
            try {
                AtomicBuffer bb=rb.buffer();
                bb.putBytes(claim_index, buf, offset, length);
                rb.commit(claim_index);
            }
            catch(Throwable t) {
                rb.abort(claim_index);
            }
        }
        else if(claim_index == RingBuffer.INSUFFICIENT_CAPACITY)
            insufficient_capacity.increment();
    }

    /**
     * Read from the ringbuffer and call receiver.receive(). As ManyToOneRingBuffer.read() doesn't block until data is
     * available, back off (yield, park etc) until data is available, to avoid burning CPU.
     */
    public void doWork() {
        int num_msgs=rb.read(this);
        idle_strategy.idle(num_msgs);
    }

    @Override
    public void onMessage(int msg_type, MutableDirectBuffer buf, int offset, int length) {
        if(msg_type != 1)
            return;
        ByteBuffer bb=buf.byteBuffer().position(offset);
        consumer.accept(bb,length);
    }

    public void close() {
        Util.close(runner, channel);
        File tmp=new File(file_name);
        tmp.delete();
    }

    protected void init(int buffer_length, boolean create) throws IOException {
        try {
            if(delete_file_on_exit) {
                File tmp=new File(file_name);
                tmp.deleteOnExit();
            }
            OpenOption[] options=create?
              new OpenOption[]{StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE} :
              new OpenOption[]{StandardOpenOption.READ, StandardOpenOption.WRITE};

            channel=FileChannel.open(Paths.get(file_name), options);
            UnsafeBuffer buffer=new UnsafeBuffer(channel.map(FileChannel.MapMode.READ_WRITE, 0, buffer_length));

            ByteBuffer bb=buffer.byteBuffer();
            byte[] tmp=new byte[buffer_length];

            // Francesco Nigro: zero the buffer so all pages are in memory
            if(create)
                bb.put(tmp);
            else
                bb.get(tmp);
            bb.rewind();
            rb=new ManyToOneRingBuffer(buffer);
        }
        catch(IOException ex) {
            close();
            throw ex;
        }
    }


}
