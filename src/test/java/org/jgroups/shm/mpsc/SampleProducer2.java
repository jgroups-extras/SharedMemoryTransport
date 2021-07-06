package org.jgroups.shm.mpsc;

import org.agrona.concurrent.ringbuffer.RingBufferDescriptor;
import org.jgroups.Global;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.shm.SharedMemoryBuffer;
import org.jgroups.util.SizeStreamable;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.Consumer;

/**
 * @author Bela Ban
 * @since x.y
 */
public class SampleProducer2 implements Consumer<ByteBuffer> {
    public static final String FILE_NAME = "/Users/bela/tmp/shm/broadcast2";
    public static final int TOTAL_BUFFER_LENGTH=(2 << 15) + RingBufferDescriptor.TRAILER_LENGTH;
    protected final Log log=LogFactory.getLog(SampleProducer2.class);

    protected int                age=10;
    protected SharedMemoryBuffer buf;

    @Override
    public void accept(ByteBuffer bb) {

    }

    protected void start() throws Exception {
        buf=new SharedMemoryBuffer(FILE_NAME, this, TOTAL_BUFFER_LENGTH, log);
        for(;;) {
            Person p=new Person(age > 30? "Old Bela" : "Bela", age);
            byte[] data=Util.streamableToByteBuffer(p);
            buf.add(data, 0, data.length);
            ++age;
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
