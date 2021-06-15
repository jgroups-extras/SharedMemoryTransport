package org.jgroups.shm.mpsc;

import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.MessageHandler;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;
import org.jgroups.util.Util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

/**
 * @author Bela Ban
 * @since x.y
 */
public class SampleConsumer2 implements MessageHandler {
    protected UnsafeBuffer buffer;
    protected ManyToOneRingBuffer rb;

    protected void start() throws IOException {
        FileChannel ch=FileChannel.open(Paths.get(SampleProducer2.FILE_NAME),
                                        StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        buffer=new UnsafeBuffer(ch.map(FileChannel.MapMode.READ_WRITE, 0, SampleProducer2.TOTAL_BUFFER_LENGTH));
        rb=new ManyToOneRingBuffer(buffer);

        for(;;) {
            rb.read(this);
            Util.sleep(1000);
        }
    }

    public void onMessage(int msg_type, MutableDirectBuffer buf, int offset, int length) {
        ByteBuffer bb=buf.byteBuffer().position(offset);
        try {
            System.out.printf("type=%d, buf=%s, offset=%d, length=%d\n", msg_type, buf, offset, length);

            SampleProducer2.Person p=Util.streamableFromByteBuffer(SampleProducer2.Person.class, bb);
            System.out.printf("%s\n", p);
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
        new SampleConsumer2().start();
    }


}
