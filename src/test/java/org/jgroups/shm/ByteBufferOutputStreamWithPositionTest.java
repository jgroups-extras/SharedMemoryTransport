package org.jgroups.shm;

import org.jgroups.protocols.PingHeader;
import org.jgroups.util.ByteBufferInputStream;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author Bela Ban
 * @since x.y
 */
@Test
public class ByteBufferOutputStreamWithPositionTest {
    protected ByteBuffer                         buf, bb;
    protected ByteBufferOutputStreamWithPosition out;
    protected static final int                   OFFSET=10;

    @BeforeMethod protected void init() {
        buf=ByteBuffer.allocate(64);
        out=new ByteBufferOutputStreamWithPosition(buf, OFFSET);
        bb=out.getBuffer();
    }


    public void testWriteByte() throws IOException {
        int buf_pos=bb.position();
        out.write(1);
        assert out.getPosition() == OFFSET+1;
        assert buf_pos == bb.position(); // the pos of the underlying ByteBuffer mustn't have changed
    }

    public void testWriteArray() throws IOException {
        byte[] array="hello world".getBytes();
        int buf_pos=bb.position();
        out.write(array);
        assert out.getPosition() == OFFSET + array.length;
        assert buf_pos == bb.position(); // the pos of the underlying ByteBuffer mustn't have changed

        byte[] tmp=new byte[array.length];
        for(int i=0; i < tmp.length; i++)
            tmp[i]=bb.get(i + OFFSET); // there is no bulk get with an offset, so we copy byte-by-byte

        String s=new String(tmp);
        assert s.equals("hello world");
    }

    public void testWriteShort() throws IOException {
        int buf_pos=bb.position();
        out.writeShort(1024);
        assert out.getPosition() == OFFSET + Short.BYTES;
        assert buf_pos == bb.position(); // the pos of the underlying ByteBuffer mustn't have changed

        short s=bb.getShort(OFFSET);
        assert s == 1024;
    }

    public void testWriteChar() throws IOException {
        int buf_pos=bb.position();
        out.writeChar('T');
        assert out.getPosition() == OFFSET + Character.BYTES;
        assert buf_pos == bb.position(); // the pos of the underlying ByteBuffer mustn't have changed

        char c=bb.getChar(OFFSET);
        assert c == 'T';
    }

    public void testWriteInt() throws IOException {
        int buf_pos=bb.position();
        out.writeInt(322649);
        assert out.getPosition() == OFFSET + Integer.BYTES;
        assert buf_pos == bb.position(); // the pos of the underlying ByteBuffer mustn't have changed
        int i=bb.getInt(OFFSET);
        assert i == 322649;
    }

    public void testWriteLong() throws IOException {
        int buf_pos=bb.position();
        out.writeLong(322649);
        assert out.getPosition() == OFFSET + Long.BYTES;
        assert buf_pos == bb.position(); // the pos of the underlying ByteBuffer mustn't have changed
        long i=bb.getLong(OFFSET);
        assert i == 322649;
    }

    public void testWriteFloat() throws IOException {
        int buf_pos=bb.position();
        out.writeFloat((float)3.23);
        assert out.getPosition() == OFFSET + Float.BYTES;
        assert buf_pos == bb.position(); // the pos of the underlying ByteBuffer mustn't have changed
        float f=bb.getFloat(OFFSET);
        assert f == (float)3.23;
    }


    public void testWriteDouble() throws IOException {
        int buf_pos=bb.position();
        out.writeDouble(Math.PI);
        assert out.getPosition() == OFFSET + Double.BYTES;
        assert buf_pos == bb.position(); // the pos of the underlying ByteBuffer mustn't have changed
        double d=bb.getDouble(OFFSET);
        assert d == Math.PI;
    }

    public void testWriteBytes() throws IOException {
        int buf_pos=bb.position();
        out.writeBytes("hello world");
        assert out.getPosition() == OFFSET + "hello world".length();
        assert buf_pos == bb.position(); // the pos of the underlying ByteBuffer mustn't have changed
    }

    public void testWriteChars() throws IOException {
        int buf_pos=bb.position();
        out.writeChars("hello world");
        assert out.getPosition() == OFFSET + "hello world".length() *2;
        assert buf_pos == bb.position(); // the pos of the underlying ByteBuffer mustn't have changed
     }

    public void testWriteUTF() throws IOException {
        int buf_pos=bb.position();
        out.writeUTF("hello world");
        assert buf_pos == bb.position(); // the pos of the underlying ByteBuffer mustn't have changed

        ByteBuffer dupl=bb.duplicate().position(OFFSET);
        ByteBufferInputStream in=new ByteBufferInputStream(dupl);
        String str=in.readUTF();
        assert str.equals("hello world");
    }

    public void testWriteUTF2() throws IOException {
        final String s="hello";
        ByteBuffer buffer=ByteBuffer.allocateDirect(20);

        DataOutput o=new ByteBufferOutputStreamWithPosition(buffer, 0);
        o.writeUTF(s);

        buffer.position(0);

        DataInput in=new ByteBufferInputStream(buffer);

        String str=in.readUTF();
        System.out.println("str = " + str);
        assert s.equals(str);
    }


    public void testPingHeader() throws IOException {
        ByteBuffer buffer=ByteBuffer.allocateDirect(100);
        PingHeader hdr=new PingHeader((byte)1).clusterName("mperf");
        ByteBufferOutputStreamWithPosition out=new ByteBufferOutputStreamWithPosition(buffer, 0);
        hdr.writeTo(out);

        buffer.position(0);
        ByteBufferInputStream in=new ByteBufferInputStream(buffer);
        PingHeader hdr2=new PingHeader();
        hdr2.readFrom(in);
        System.out.println("hdr2 = " + hdr2);

    }

}
