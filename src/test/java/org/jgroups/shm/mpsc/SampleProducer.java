package org.jgroups.shm.mpsc;

import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;
import org.agrona.concurrent.ringbuffer.RingBufferDescriptor;
import org.jgroups.util.Util;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

/**
 * @author Bela Ban
 * @since x.y
 */
public class SampleProducer {
    public static final String FILE_NAME = "/Users/bela/tmp/shm/broadcast";
    public static final int TOTAL_BUFFER_LENGTH=(2 << 5) + RingBufferDescriptor.TRAILER_LENGTH;


    protected UnsafeBuffer buffer;
    protected ManyToOneRingBuffer rb;

    protected void start() throws IOException {
        FileChannel ch=FileChannel.open(Paths.get(FILE_NAME),
                                        StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        buffer=new UnsafeBuffer(ch.map(FileChannel.MapMode.READ_WRITE, 0, TOTAL_BUFFER_LENGTH));
        rb=new ManyToOneRingBuffer(buffer);
        AtomicBuffer bb=rb.buffer();
        int index=1;
        for(;;) {
            int claim_index=rb.tryClaim(1, Integer.BYTES);
            if(claim_index > 0) {
                bb.putInt(claim_index, index);
                rb.commit(claim_index);
                System.out.printf(" %d", index);
                ++index;
            }
            else
                System.err.printf("** claim failed; index=%d, capacity=%d, size=%d\n",
                                  claim_index, rb.capacity(), rb.size());
            Util.sleep(1000);
        }
    }


    public static void main(String[] args) throws IOException {
        new SampleProducer().start();
    }


}
