package org.jgroups.shm;


import org.jgroups.shm.ManyToOneBoundedChannel.MessageHandler;
import org.jgroups.util.DefaultThreadFactory;
import org.jgroups.util.Runner;
import org.jgroups.util.ThreadFactory;
import org.jgroups.util.Util;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;

import static org.jgroups.shm.ByteBufferUtils.copyBytes;
import static org.jgroups.shm.ManyToOneBoundedChannel.claimedIndex;

/**
 * Wraps {@link org.jgroups.shm.ManyToOneBoundedChannel}. Can be used for writing; reading is enabled
 * by setting a consumer ({@link #setConsumer(Consumer)}).
 * @author Bela Ban
 * @since  1.0.0
 */
public class SharedMemoryBuffer implements MessageHandler, Closeable {

    protected final String              file_name;   // name of the shared memory-mapped file (e.g. /tmp/shm/uuid-1
    protected Consumer<ByteBuffer>      consumer;    // a received message calls consumer.receive();
    protected FileChannel               channel;     // the memory-mapped file
    protected ManyToOneBoundedChannel   rb;
    protected ByteBuffer                readBuffer;
    protected final Runner              runner;
    protected IdleStrategy              idle_strategy;
    protected boolean                   delete_file_on_exit;
    protected final LongAdder           insufficient_capacity=new LongAdder();


    public SharedMemoryBuffer(String file_name, int buffer_length, boolean create, ThreadFactory f) throws IOException {
        this.file_name=file_name;
        // idle stragegy spins, the yields, then parks between 1000ns and 64ms by default
        idle_strategy=IdleStrategy.backoffIdle(IdleStrategy.DEFAULT_MAX_SPINS,
                                              IdleStrategy.DEFAULT_MAX_YIELDS,
                                              IdleStrategy.DEFAULT_MIN_PARK_PERIOD_NS,
                                              IdleStrategy.DEFAULT_MAX_PARK_PERIOD_NS<<6);
        init(buffer_length, create);
        ThreadFactory tf=f != null? f : new DefaultThreadFactory("runner", true, true);
        runner=new Runner(tf, String.format("shm-%s", file_name), this::doWork, null);
    }


    public SharedMemoryBuffer idleStrategy(IdleStrategy s) {idle_strategy=Objects.requireNonNull(s); return this;}
    public long               insufficientCapacity()       {return insufficient_capacity.sum();}
    public SharedMemoryBuffer resetStats()                 {insufficient_capacity.reset(); return this;}

    public SharedMemoryBuffer maxSleep(long m) {
        long max_sleep_ns=TimeUnit.NANOSECONDS.convert(m, TimeUnit.MILLISECONDS);
        idle_strategy=IdleStrategy.backoffIdle(IdleStrategy.DEFAULT_MAX_SPINS, IdleStrategy.DEFAULT_MAX_YIELDS,
                                                IdleStrategy.DEFAULT_MIN_PARK_PERIOD_NS, max_sleep_ns);
        return this;
    }

    public SharedMemoryBuffer deleteFileOnExit(boolean f)  {
        if((delete_file_on_exit=f) == true) {
            File tmp=new File(file_name);
            tmp.deleteOnExit();
        }
        return this;
    }

    public SharedMemoryBuffer setConsumer(Consumer<ByteBuffer> c) {
        consumer=Objects.requireNonNull(c);
        runner.start();
        return this;
    }

    public boolean write(byte[] buf, int offset, int length) {
        final ManyToOneBoundedChannel rb = this.rb;
        final long claim = rb.tryClaim(1, length);
        if(claim == ManyToOneBoundedChannel.INSUFFICIENT_CAPACITY) {
            insufficient_capacity.increment();
            return false;
        }
        try {
            copyBytes(buf, offset, rb.buffer(), claimedIndex(claim), length);
        } finally {
            rb.commit(claim);
        }
        return true;
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
    public void onMessage(int msg_type, ByteBuffer buf, int offset, int length) {
        if(msg_type != 1)
            return;
        final ByteBuffer readbuf = this.readBuffer;
        readbuf.position(offset);
        readbuf.limit(offset + length);
        try {
            consumer.accept(readbuf);
        } finally {
            readbuf.clear();
        }
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
            ByteBuffer bb=channel.map(FileChannel.MapMode.READ_WRITE, 0, buffer_length)
               .order(ByteOrder.nativeOrder());
            // Francesco Nigro: zero the buffer so all pages are in memory
            ByteBufferUtils.zeros(bb, 0, buffer_length);
            rb=new ManyToOneBoundedChannel(bb);
            readBuffer=bb.asReadOnlyBuffer();
        }
        catch(IOException ex) {
            close();
            throw ex;
        }
    }


}
