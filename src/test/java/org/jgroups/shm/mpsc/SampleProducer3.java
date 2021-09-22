package org.jgroups.shm.mpsc;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.ObjectMessage;
import org.jgroups.protocols.UnicastHeader3;
import org.jgroups.shm.ManyToOneBoundedChannel;
import org.jgroups.shm.SharedMemoryBuffer;
import org.jgroups.util.ByteArrayDataOutputStream;
import org.jgroups.util.Util;

/**
 * @author Bela Ban
 * @since x.y
 */
public class SampleProducer3 {
    public static final String FILE_NAME = "/Users/bela/tmp/shm/broadcast3";
    public static final int TOTAL_BUFFER_LENGTH=(2 << 15) + ManyToOneBoundedChannel.TRAILER_LENGTH;

    protected SharedMemoryBuffer buf;


    protected void start() throws Exception {
        buf=new SharedMemoryBuffer(FILE_NAME, TOTAL_BUFFER_LENGTH, true, null, false); // .deleteFileOnExit(true);

        Address sender=Util.createRandomAddress("B");
        Message msg=new ObjectMessage(null, "hello world from Bela").setFlag(Message.Flag.OOB)
          .putHeader((short)11, UnicastHeader3.createDataHeader(322649, (short)1, false))
          .setSrc(sender);
        ByteArrayDataOutputStream out=new ByteArrayDataOutputStream(msg.serializedSize());
        Util.writeMessage(msg, out, true);

        buf.write(out.buffer(), 0, out.position());

        // Util.sleep(60_000);
    }


    public static void main(String[] args) throws Exception {
        new SampleProducer3().start();
    }



}
