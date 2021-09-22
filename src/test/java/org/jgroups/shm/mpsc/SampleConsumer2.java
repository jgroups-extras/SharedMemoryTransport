package org.jgroups.shm.mpsc;

import org.jgroups.shm.SharedMemoryBuffer;
import org.jgroups.util.Util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.Consumer;

/**
 * @author Bela Ban
 * @since x.y
 */
public class SampleConsumer2 implements Consumer<ByteBuffer> {
    protected SharedMemoryBuffer buf;

    @Override
    public void accept(ByteBuffer bb) {
        try {
            SampleProducer2.Person p=Util.streamableFromByteBuffer(SampleProducer2.Person.class, bb);
            System.out.printf("%s\n", p);
        }
        catch(Exception e) {
            e.printStackTrace();
        }

    }

    protected void start() throws IOException {
        buf=new SharedMemoryBuffer(SampleProducer2.FILE_NAME, SampleProducer2.TOTAL_BUFFER_LENGTH, true, null, false)
          .setConsumer(this);
    }


    public static void main(String[] args) throws IOException {
        new SampleConsumer2().start();
    }


}
