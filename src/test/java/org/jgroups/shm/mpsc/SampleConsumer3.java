package org.jgroups.shm.mpsc;

import org.jgroups.DefaultMessageFactory;
import org.jgroups.Message;
import org.jgroups.MessageFactory;
import org.jgroups.Version;
import org.jgroups.shm.SharedMemoryBuffer;
import org.jgroups.util.ByteArrayDataInputStream;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.Consumer;

/**
 * @author Bela Ban
 * @since x.y
 */
public class SampleConsumer3 implements Consumer<ByteBuffer> {
    protected SharedMemoryBuffer   buf;
    protected final MessageFactory msg_factory=new DefaultMessageFactory();

    @Override
    public void accept(ByteBuffer bb) {
        byte[] array=new byte[bb.remaining()];
        bb.get(array);
        ByteArrayDataInputStream in=new ByteArrayDataInputStream(array);

        short version=0;
        try {
            version=in.readShort();
            boolean compatible=Version.isBinaryCompatible(version);
            System.out.printf("version %s is compatible: %b\n", Version.print(version), compatible);

            byte flags=in.readByte();
            short type=in.readShort();
            Message msg=msg_factory.create(type); // don't create headers, readFrom() will do this
            msg.readFrom(in);
            System.out.printf("msg: %s\n", msg);
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }

    protected void start() throws IOException {
        buf=new SharedMemoryBuffer(SampleProducer3.FILE_NAME, SampleProducer3.TOTAL_BUFFER_LENGTH, true, null, false)
          .setConsumer(this).deleteFileOnExit(true);
    }


    public static void main(String[] args) throws IOException {
        new SampleConsumer3().start();
    }


}
