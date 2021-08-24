package org.jgroups.shm.mpsc;

import org.jgroups.shm.ManyToOneBoundedChannel;
import org.jgroups.util.Util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import static org.jgroups.shm.ManyToOneBoundedChannel.claimedIndex;

/**
 * @author Bela Ban
 * @since x.y
 */
public class SampleProducer {
    public static final String FILE_NAME = "/Users/bela/tmp/shm/broadcast";
    public static final int TOTAL_BUFFER_LENGTH=(2 << 5) + ManyToOneBoundedChannel.TRAILER_LENGTH;


    protected ByteBuffer buffer;
    protected ManyToOneBoundedChannel rb;

    protected void start() throws IOException {
        FileChannel ch=FileChannel.open(Paths.get(FILE_NAME),
                                        StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        buffer=ch.map(FileChannel.MapMode.READ_WRITE, 0, TOTAL_BUFFER_LENGTH);
        rb=new ManyToOneBoundedChannel(buffer);
        int index=1;
        final ByteBuffer bb = rb.buffer();
        for(;;) {
            long claim=rb.tryClaim(1, Integer.BYTES);
            if(claim != ManyToOneBoundedChannel.INSUFFICIENT_CAPACITY) {
                bb.putInt(claimedIndex(claim), index);
                rb.commit(claim);
                System.out.printf(" %d", index);
                ++index;
            }
            else
                System.err.printf("** claim failed; index=%d, capacity=%d, size=%d\n",
                                  claim, rb.capacity(), rb.size());
            Util.sleep(1000);
        }
    }


    public static void main(String[] args) throws IOException {
        new SampleProducer().start();
    }


}
