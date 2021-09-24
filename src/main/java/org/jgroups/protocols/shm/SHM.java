package org.jgroups.protocols.shm;

import org.jgroups.*;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.conf.AttributeType;
import org.jgroups.protocols.TP;
import org.jgroups.shm.ManyToOneBoundedChannel;
import org.jgroups.shm.SharedMemoryBuffer;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.*;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * Transport using shared memory to exchange messages
 * @author Bela Ban
 * @since  1.0.0
 */
@MBean(description="Transport which exchanges messages by adding them to shared memory. This works only when all " +
  "members are processes on the same host")
public class SHM extends TP implements Consumer<ByteBuffer> {

    @Property(description="Folder under which the memory-mapped files for the queues are created.")
    protected String                                location="/tmp/shm";

    @Property(description="Max capacity of a queue (in bytes)",type=AttributeType.BYTES)
    protected int                                   queue_capacity=2 << 22; // ca 8MB

    @Property(description="The max time (in millis) a receiver loop should park when idle. 0=default",
      type=AttributeType.TIME)
    protected long                                  max_sleep;

    protected SharedMemoryBuffer                    buf;

    protected LeakyByteBufferInputStream            cachedReceiveStream;

    protected final Map<Address,SharedMemoryBuffer> cache=new ConcurrentHashMap<>();

    protected static final PhysicalAddress          PHYSICAL_ADDRESS=new IpAddress(10000);


    @ManagedAttribute(description="The sum of failed writes due to insufficient capacity of all ring buffers")
    public int getFailedWritesDueToInsufficientCapacity() {
        return cache.values().stream().mapToInt(c -> (int)c.insufficientCapacity()).sum();
    }

    @ManagedOperation(description="Changes max_sleep")
    public void maxSleep(long ms) {
        this.max_sleep=ms;
        if(buf != null)
            buf.maxSleep(ms);
    }


    @Override
    public void init() throws Exception {
        int cap=Util.getNextHigherPowerOfTwo(queue_capacity);
        if(queue_capacity != cap) {
            log.warn("queue_capacity (%d) must be a power of 2, changing it to %d", queue_capacity, cap);
            queue_capacity=cap;
        }
        File f=new File(location);
        if(!f.exists())
            throw new IllegalArgumentException(String.format("location %s does not exist", location));
        super.init();
    }

    public void resetStats() {
        super.resetStats();
        cache.values().forEach(SharedMemoryBuffer::resetStats);
    }


    @Override
    public String getInfo() {
        return null;
    }


    @Override
    public boolean supportsMulticasting() {
        return false;
    }

    @Override
    protected PhysicalAddress getPhysicalAddress() {
        return PHYSICAL_ADDRESS;
    }

    @Override
    public Object down(Event evt) {
        Object ret=super.down(evt);
        switch(evt.getType()) {
            case Event.CONNECT:
            case Event.CONNECT_USE_FLUSH:
            case Event.CONNECT_WITH_STATE_TRANSFER:
            case Event.CONNECT_WITH_STATE_TRANSFER_USE_FLUSH:
                try {
                    buf=createBuffer(local_addr, null, true, thread_factory)
                      .setConsumer(this).deleteFileOnExit(true);
                    if(max_sleep > 0)
                        buf.maxSleep(max_sleep);
                    cache.putIfAbsent(local_addr, buf);
                    initCache();
                }
                catch(IOException ex) {
                    log.error("failed creating buffer", ex);
                }
                break;
            case Event.DISCONNECT:
                Util.close(buf);
                break;

            case Event.VIEW_CHANGE:
                View v=evt.getArg();
                Set<Address> keys=cache.keySet();
                keys.retainAll(v.getMembers());
                for(Address mbr: v.getMembersRaw()) {
                    if(!keys.contains(mbr)) {
                        try {
                            getOrCreateBuffer(mbr);
                        }
                        catch(IOException e) {
                            log.warn("failed creating shared buffer for %s: %s", mbr, e);
                        }
                    }
                }
                break;
        }
        return ret;
    }


    @Override
    public void sendUnicast(PhysicalAddress dest, byte[] data, int offset, int length) throws Exception {
        throw new UnsupportedOperationException("method sendUnicast() should not be called");
    }

    @Override
    public void accept(ByteBuffer bb) {
        try {
            LeakyByteBufferInputStream receiveStream = this.cachedReceiveStream;
            if (receiveStream == null || receiveStream.buf() != bb) {
                receiveStream =new LeakyByteBufferInputStream(bb);
                this.cachedReceiveStream = null;
            }
            receive(null, receiveStream);
        }
        catch(Exception ex) {
            log.error("failed handling message", ex);
        }
    }


    @Override
    protected void sendTo(Address dest, byte[] buf, int offset, int length) throws Exception {
        SharedMemoryBuffer shm_buf=getOrCreateBuffer(dest);
        if(shm_buf == null)
            throw new IllegalStateException(String.format("buffer for %s not found", dest));
        shm_buf.write(buf, offset, length);
    }


    @Override
    protected void sendToAll(byte[] buf, int offset, int length) throws Exception {
        Set<Address> mbrs=cache.keySet();
        for(Address dest: mbrs) {
            if(Objects.equals(dest, local_addr))
                continue;
            sendTo(dest, buf, offset, length);
        }
    }

    @Override
    public void sendTo(Address dest, Message msg) throws Exception {
        SharedMemoryBuffer shm_buf=getOrCreateBuffer(dest);
        if(shm_buf == null)
            throw new IllegalStateException(String.format("buffer for %s not found", msg.dest()));
        int length=Util.size(msg);
        shm_buf.write(msg, length);
    }

    @Override
    public void sendToAll(Message msg) throws Exception {
        Set<Address> mbrs=cache.keySet();
        for(Address dest: mbrs) {
            if(Objects.equals(dest, local_addr))
                continue;
            sendTo(dest, msg);
        }
    }

    @Override
    public void sendTo(Address dest, Address src, List<Message> msgs) throws Exception {
        int serialized_size=Util.sizeMessageList(dest, src, cluster_name != null? cluster_name.chars() : null, msgs, getId());
        sendTo(dest, src, msgs, serialized_size, false);
    }

    @Override
    public void sendToAll(Address src, List<Message> msgs) throws Exception {
        Set<Address> mbrs=cache.keySet();
        int size=Util.sizeMessageList(null, src, cluster_name != null? cluster_name.chars() : null, msgs, getId());
        for(Address dest: mbrs) {
            if(Objects.equals(dest, local_addr))
                continue;
            sendTo(dest, src, msgs, size, true);
        }
    }

    protected void sendTo(Address dest, Address src, List<Message> msgs, int serialized_size, boolean multicast) throws Exception {
        SharedMemoryBuffer shm_buf=getOrCreateBuffer(dest);
        if(shm_buf == null)
            throw new IllegalStateException(String.format("buffer for %s not found", dest));
        shm_buf.write(dest, src, msgs, serialized_size, cluster_name.chars(), getId(), multicast);
    }

    protected SharedMemoryBuffer createBuffer(Address addr, String logical_name, boolean create,
                                              ThreadFactory thread_factory) throws IOException {
        String buffer_name=addressToFilename(addr, logical_name);
        return new SharedMemoryBuffer(buffer_name,queue_capacity+ ManyToOneBoundedChannel.TRAILER_LENGTH,
                                      create, thread_factory, late_marshalling);
    }


    protected String addressToFilename(Address addr, String logical_name) {
        String cluster=cluster_name != null? cluster_name.toString() : null;
        Path dir=Path.of(Objects.requireNonNull(location), Objects.requireNonNull(cluster));
        File tmp_dir=dir.toFile();
        if(!tmp_dir.exists())
            tmp_dir.mkdirs();
        String addr_name=((UUID)addr).toStringLong();
        if(logical_name == null)
            logical_name=NameCache.get(addr);
        if(logical_name != null)
            addr_name=String.format("%s::%s", addr_name, logical_name);
        return Path.of(dir.toString(), addr_name).toString();
    }

    protected static Tuple<Address,String> filenameToAddress(String fname) {
        int index=fname.indexOf("::");
        String s=index != -1? fname.substring(0, index) : fname;
        String logical_name=index != -1? fname.substring(index+2) : null;
        return new Tuple<>(UUID.fromString(s), logical_name);
    }

    protected SharedMemoryBuffer getOrCreateBuffer(Address addr) throws IOException {
        SharedMemoryBuffer shm_buf=cache.get(addr);
        if(shm_buf == null) {
            shm_buf=createBuffer(addr, null, false, thread_factory);
            SharedMemoryBuffer tmp=cache.putIfAbsent(addr, shm_buf);
            if(tmp != null)
                shm_buf=tmp;
        }
        return shm_buf;
    }

    /** Reads all files under location/group/ and populates cache */
    protected void initCache() throws IOException {
        String cluster=cluster_name != null? cluster_name.toString() : null;
        Path dir=Path.of(Objects.requireNonNull(location), Objects.requireNonNull(cluster));
        File[] files=dir.toFile().listFiles();
        for(File f: files) {
            String tmp=f.getName();
            Tuple<Address,String> t=filenameToAddress(tmp);
            String logical_name=t.getVal2();
            Address uuid=t.getVal1();
            if(!cache.containsKey(uuid))
                cache.putIfAbsent(uuid, createBuffer(uuid, logical_name, false, thread_factory));
            addPhysicalAddressToCache(uuid, PHYSICAL_ADDRESS);
        }
    }


    protected static final class LeakyByteBufferInputStream extends ByteBufferInputStream {

        public LeakyByteBufferInputStream(ByteBuffer buf) {
            super(buf);
        }

        private ByteBuffer buf() {
            return buf;
        }
    }

}
