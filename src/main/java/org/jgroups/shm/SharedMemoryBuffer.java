package org.jgroups.shm;


import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.*;
import org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;
import org.jgroups.logging.Log;
import org.jgroups.util.DefaultThreadFactory;
import org.jgroups.util.Runner;
import org.jgroups.util.ThreadFactory;
import org.jgroups.util.Util;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Wraps {@link org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer}.
 * @author Bela Ban
 * @since  1.0.0
 */
public class SharedMemoryBuffer implements MessageHandler, Closeable {
    protected final String        file_name;   // name of the shared memory-mapped file (e.g. /tmp/shm/uuid-1
    protected final Consumer<ByteBuffer> consumer; // a received message calls consumer.receive();
    protected FileChannel         channel;     // the memory-mapped file
    protected UnsafeBuffer        buffer;
    protected ManyToOneRingBuffer rb;
    protected final Runner        runner;
    protected IdleStrategy        idle_strategy=new BackoffIdleStrategy();
    protected final Log           log;

    public SharedMemoryBuffer(String file_name, Consumer<ByteBuffer> c, int buffer_length, Log log) throws IOException {
        this.file_name=file_name;
        this.consumer=Objects.requireNonNull(c);
        this.log=log;
        init(buffer_length);
        ThreadFactory tf=new DefaultThreadFactory("runner", true, true);
        runner=new Runner(tf, String.format("shm-%s", file_name), this::doWork, null);
        runner.start();
    }

    public SharedMemoryBuffer idleStrategy(IdleStrategy s) {idle_strategy=Objects.requireNonNull(s); return this;}

    public boolean add(byte[] buf, int offset, int length) {
        int claim_index=rb.tryClaim(1, length);
        if(claim_index > 0) {
            AtomicBuffer bb=rb.buffer();
            bb.putBytes(claim_index, buf, offset, buf.length);
            rb.commit(claim_index);
            return true;
        }
        return false;
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
        ByteBuffer bb=buf.byteBuffer();
        bb.position(offset);
        consumer.accept(bb);
    }

    public void close() {
        Util.close(runner, channel);
        File tmp=new File(file_name);
        tmp.delete();
    }

    protected void init(int buffer_length) throws IOException {
        try {
            File tmp=new File(file_name);
            tmp.deleteOnExit();
            channel=FileChannel.open(Paths.get(file_name),
                                     StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
                                     // StandardOpenOption.DELETE_ON_CLOSE);
            buffer=new UnsafeBuffer(channel.map(FileChannel.MapMode.READ_WRITE, 0, buffer_length));
            rb=new ManyToOneRingBuffer(buffer);
        }
        catch(IOException ex) {
            close();
            throw ex;
        }
    }


}
