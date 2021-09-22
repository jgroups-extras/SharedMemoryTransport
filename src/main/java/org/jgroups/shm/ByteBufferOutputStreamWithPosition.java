package org.jgroups.shm;

import org.jgroups.util.ByteArray;

import java.io.DataOutput;
import java.io.IOException;
import java.io.UTFDataFormatException;
import java.nio.ByteBuffer;

/**
 * Class using {@link ByteBuffer} and implementing {@link DataOutput}. Note that all instances need to pass the
 * exact size of the marshalled object as a ByteBuffer cannot be expanded.<br/>
 * Contrary to {@link org.jgroups.util.ByteBufferOutputStream}, this class maintains its own position, and never
 * changes position of the underlying ByteBuffer. This class is <em>not</em> thread-safe.
 * @author Bela Ban
 */
public class ByteBufferOutputStreamWithPosition implements DataOutput {
    protected final ByteBuffer buf;
    protected int              position, written;


    public ByteBufferOutputStreamWithPosition(ByteBuffer buf, int position) {
        this.buf=buf;
        this.position=position;
    }

    public ByteBuffer getBuffer() {return buf;}

    public int getPosition() {return position;}
    public int getWritten()  {return written;}

    public ByteArray getBufferAsBuffer() {
        return new ByteArray(buf.array(), buf.arrayOffset(), position);
    }


    public void write(int b) throws IOException {
        buf.put(position++, (byte)b);
        written++;
    }

    public void write(byte[] b) throws IOException {
        write(b,0,b.length);
    }

    public void write(byte[] b, int off, int len) throws IOException {
        ByteBufferUtils.copyBytes(b, off, buf, position, len);
        position+=len;
        written+=len;
    }

    public void writeBoolean(boolean v) throws IOException {
        write(v? 1 : 0);
    }

    public void writeByte(int v) throws IOException {
        write(v);
    }

    public void writeShort(int v) throws IOException {
        buf.putShort(position, (short)v);
        position+=Short.BYTES;
        written+=Short.BYTES;
    }

    public void writeChar(int v) throws IOException {
        buf.putChar(position, (char)v);
        position+=Character.BYTES;
        written+=Character.BYTES;
    }

    public void writeInt(int v) throws IOException {
        buf.putInt(position, v);
        position+=Integer.BYTES;
        written+=Integer.BYTES;
    }

    public void writeLong(long v) throws IOException {
        buf.putLong(position, v);
        position+=Long.BYTES;
        written+=Long.BYTES;
    }

    public void writeFloat(float v) throws IOException {
        buf.putFloat(position, v);
        position+=Float.BYTES;
        written+=Float.BYTES;
    }

    public void writeDouble(double v) throws IOException {
        buf.putDouble(position, v);
        position+=Double.BYTES;
        position+=Double.BYTES;
    }

    public void writeBytes(String s) throws IOException {
        int len=s.length();
        for(int i = 0 ; i < len ; i++)
            write((byte)s.charAt(i));
    }

    public void writeChars(String s) throws IOException {
        int len=s.length();
        for (int i = 0 ; i < len ; i++) {
            int v = s.charAt(i);
            write((v >>> 8) & 0xFF);
            write((v >>> 0) & 0xFF);
        }
    }

    public void writeUTF(String str) throws IOException {
        int strlen = str.length();
        int utflen = 0;
        int c, count = 0;

        /* use charAt instead of copying String to char array */
        for (int i = 0; i < strlen; i++) {
            c = str.charAt(i);
            if ((c >= 0x0001) && (c <= 0x007F)) {
                utflen++;
            } else if (c > 0x07FF) {
                utflen += 3;
            } else {
                utflen += 2;
            }
        }

        if (utflen > 65535)
            throw new UTFDataFormatException("encoded string too long: " + utflen + " bytes");

        writeShort(utflen);

        byte[] bytearr=new byte[utflen];

        int i=0;
        for (i=0; i<strlen; i++) {
            c = str.charAt(i);
            if (!((c >= 0x0001) && (c <= 0x007F))) break;
            bytearr[count++] = (byte) c;
        }

        for (;i < strlen; i++){
            c = str.charAt(i);
            if ((c >= 0x0001) && (c <= 0x007F)) {
                bytearr[count++] = (byte) c;

            } else if (c > 0x07FF) {
                bytearr[count++] = (byte) (0xE0 | ((c >> 12) & 0x0F));
                bytearr[count++] = (byte) (0x80 | ((c >>  6) & 0x3F));
                bytearr[count++] = (byte) (0x80 | ((c >>  0) & 0x3F));
            } else {
                bytearr[count++] = (byte) (0xC0 | ((c >>  6) & 0x1F));
                bytearr[count++] = (byte) (0x80 | ((c >>  0) & 0x3F));
            }
        }
        write(bytearr,0,utflen);
    }

    public String toString() {
        return String.format("pos=%d written=%d", position, written);
    }
}
