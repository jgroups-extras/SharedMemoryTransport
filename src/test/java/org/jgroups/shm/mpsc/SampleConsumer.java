package org.jgroups.shm.mpsc;

import org.jgroups.shm.ManyToOneBoundedChannel;
import org.jgroups.shm.ManyToOneBoundedChannel.MessageHandler;
import org.jgroups.util.Util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

/**
 * @author Bela Ban
 * @since x.y
 */
public class SampleConsumer implements MessageHandler {
    protected ByteBuffer buffer;
    protected ManyToOneBoundedChannel rb;

    protected void start() throws IOException {
        FileChannel ch=FileChannel.open(Paths.get(SampleProducer.FILE_NAME),
                                        StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        buffer=ch.map(FileChannel.MapMode.READ_WRITE, 0, SampleProducer.TOTAL_BUFFER_LENGTH).order(ByteOrder.nativeOrder());
        rb=new ManyToOneBoundedChannel(buffer);

        for(;;) {
            rb.read(this);
            Util.sleep(1000);
        }
    }

    @Override
    public void onMessage(int msg_type, ByteBuffer buf, int index, int length) {
        int val=buf.getInt(index);
        System.out.printf("%d ", val);
    }

    public static void main(String[] args) throws IOException {
        new SampleConsumer().start();
    }


}
