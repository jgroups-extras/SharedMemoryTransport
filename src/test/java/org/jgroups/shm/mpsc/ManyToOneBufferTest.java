package org.jgroups.shm.mpsc;

import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;
import org.jgroups.util.Util;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author Bela Ban
 * @since x.y
 */
@Test
public class ManyToOneBufferTest {
    protected ManyToOneRingBuffer rb;
    protected AtomicBuffer        buf;

    @BeforeTest
    protected void setup() {
        buf=new UnsafeBuffer(ByteBuffer.allocate(1024));
        rb=new ManyToOneRingBuffer(buf);
    }


    public void testWriteAndRead() throws IOException {
        byte[] buf1=Util.objectToByteBuffer("hello world");
        byte[] buf2=Util.objectToByteBuffer("from Bela Ban");

        int index1=rb.tryClaim(1, buf1.length), index2=rb.tryClaim(1, buf2.length);
        if(index2 > 0) {
            try {
                AtomicBuffer tmp=rb.buffer();
                tmp.putBytes(index2, buf2);
                rb.commit(index2);
            }
            catch(Throwable t) {
                rb.abort(index2);
            }
        }

        if(index1 > 0) {
            try {
                AtomicBuffer tmp=rb.buffer();
                tmp.putBytes(index1, buf1);
                rb.commit(index1);
            }
            catch(Throwable t) {
                rb.abort(index1);
            }
        }


        int num_msgs=rb.read((msgTypeId, buffer, offset, length) -> {
            ByteBuffer b=buffer.byteBuffer().position(offset);
            byte[] tmp=new byte[length];
            b.get(tmp, 0, tmp.length);
            String s=null;
            try {
                s=Util.objectFromByteBuffer(tmp);
                System.out.println("s = " + s);
            }
            catch(Exception e) {
                e.printStackTrace();
            }
        });
        assert num_msgs == 2;
    }
}
