package org.jgroups.shm.mpsc;

import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
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
    protected final Log log=LogFactory.getLog(SampleConsumer2.class);

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
        buf=new SharedMemoryBuffer(SampleProducer2.FILE_NAME, this, SampleProducer2.TOTAL_BUFFER_LENGTH, log);
    }


    public static void main(String[] args) throws IOException {
        new SampleConsumer2().start();
    }


}
