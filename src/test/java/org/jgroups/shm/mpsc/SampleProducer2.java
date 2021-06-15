package org.jgroups.shm.mpsc;

import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;
import org.agrona.concurrent.ringbuffer.RingBufferDescriptor;
import org.jgroups.Global;
import org.jgroups.util.SizeStreamable;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

/**
 * @author Bela Ban
 * @since x.y
 */
public class SampleProducer2 {
    public static final String FILE_NAME = "/Users/bela/tmp/shm/broadcast2";
    public static final int TOTAL_BUFFER_LENGTH=(2 << 15) + RingBufferDescriptor.TRAILER_LENGTH;


    protected UnsafeBuffer buffer;
    protected ManyToOneRingBuffer rb;

    protected void start() throws Exception {
        FileChannel ch=FileChannel.open(Paths.get(FILE_NAME),
                                        StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        buffer=new UnsafeBuffer(ch.map(FileChannel.MapMode.READ_WRITE, 0, TOTAL_BUFFER_LENGTH));
        rb=new ManyToOneRingBuffer(buffer);
        AtomicBuffer bb=rb.buffer();
        int age=10;
        for(;;) {
            Person p=new Person(age > 30? "Old Bela" : "Bela", age);
            byte[] data=Util.streamableToByteBuffer(p);

            int claim_index=rb.tryClaim(1, data.length);
            if(claim_index > 0) {
                bb.putBytes(claim_index, data, 0, data.length);
                rb.commit(claim_index);
                System.out.printf(" %d", claim_index);
                ++age;
            }
            else
                System.err.printf("** claim failed; index=%d, capacity=%d, size=%d\n",
                                  claim_index, rb.capacity(), rb.size());
            Util.sleep(1000);
        }
    }


    public static void main(String[] args) throws Exception {
        new SampleProducer2().start();
    }


    public static class Person implements SizeStreamable {
        protected String name;
        protected int age;

        public Person() {
        }

        public Person(String name, int age) {
            this.name=name;
            this.age=age;
        }

        public int serializedSize() {
            return Util.size(name) + Global.INT_SIZE;
        }

        public void writeTo(DataOutput out) throws IOException {
            Util.writeString(name, out);
            out.writeInt(age);
        }

        public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
            name=Util.readString(in);
            age=in.readInt();
        }

        public String toString() {
            return String.format("name=%s age=%d", name, age);
        }
    }

}
