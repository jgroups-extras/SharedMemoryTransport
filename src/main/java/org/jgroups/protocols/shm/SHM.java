package org.jgroups.protocols.shm;

import org.agrona.concurrent.ringbuffer.RingBufferDescriptor;
import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.PhysicalAddress;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.Property;
import org.jgroups.conf.AttributeType;
import org.jgroups.protocols.TP;
import org.jgroups.shm.SharedMemoryBuffer;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.ByteBufferInputStream;
import org.jgroups.util.UUID;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
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
    protected String             location="/tmp/shm";

    @Property(description="Max capacity of a queue (in bytes)",type=AttributeType.BYTES)
    protected int                queue_capacity=0xffff+1;

    protected SharedMemoryBuffer buf;

    protected final Map<Address,SharedMemoryBuffer> cache=new ConcurrentHashMap<>();

    protected static final PhysicalAddress PHYSICAL_ADDRESS=new IpAddress(10000);

    @Override
    public void init() throws Exception {
        File f=new File(location);
        if(!f.exists())
            throw new IllegalArgumentException("location %s does not exist");
        super.init();
    }

    @Override
    public void start() throws Exception {
        super.start();
    }

    @Override
    public void stop() {
        super.stop();
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
                    buf=createBuffer(local_addr);
                    initCache();
                }
                catch(IOException ex) {
                    log.error("failed creating buffer", ex);
                }
                break;
            case Event.DISCONNECT:
                Util.close(buf);
                break;
        }
        return ret;
    }

    @Override
    public void sendMulticast(byte[] data, int offset, int length) throws Exception {
        sendToMembers(null, data, offset, length);
    }

    @Override
    public void sendUnicast(PhysicalAddress dest, byte[] data, int offset, int length) throws Exception {
        throw new UnsupportedOperationException("method sendUnicast() should not be called");
    }


    @Override
    protected void sendToSingleMember(Address dest, byte[] buf, int offset, int length) throws Exception {
        SharedMemoryBuffer shm_buf=getOrCreateBuffer(dest);
        shm_buf.add(buf, offset, length);
    }

    @Override
    public void accept(ByteBuffer bb) {
        try {
            DataInput in=new ByteBufferInputStream(bb);
            receive(null, in);
        }
        catch(Exception ex) {
            log.error("failed handling message", ex);
        }
    }

    @Override
    protected void sendToMembers(Collection<Address> mbrs, byte[] buf, int offset, int length) throws Exception {
        if(mbrs == null || mbrs.isEmpty())
            mbrs=logical_addr_cache.keySet();
        for(Address dest: mbrs) {
            if(Objects.equals(dest, local_addr))
                continue;
            sendToSingleMember(dest, buf, offset, length);
        }
    }

    protected SharedMemoryBuffer createBuffer(Address dest) throws IOException {
        String addr=dest != null? ((UUID)dest).toStringLong() : null;
        return createBuffer(addr);
    }

    protected SharedMemoryBuffer createBuffer(String addr) throws IOException {
        String cluster=cluster_name != null? cluster_name.toString() : null;
        Path dir=Path.of(Objects.requireNonNull(location), Objects.requireNonNull(cluster));
        File tmp_dir=dir.toFile();
        if(!tmp_dir.exists())
            tmp_dir.mkdirs();
        String buffer_name=Path.of(dir.toString(), Objects.requireNonNull(addr)).toString();
        return new SharedMemoryBuffer(buffer_name, this, queue_capacity+ RingBufferDescriptor.TRAILER_LENGTH, log);
    }

    protected SharedMemoryBuffer getOrCreateBuffer(Address addr) throws IOException {
        SharedMemoryBuffer shm_buf=cache.get(addr);
        if(shm_buf == null) {
            shm_buf=createBuffer(addr);
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
            String uuid_name=f.getName();
            UUID uuid=UUID.fromString(uuid_name);
            if(!cache.containsKey(uuid))
                cache.putIfAbsent(uuid, createBuffer(uuid));
            addPhysicalAddressToCache(uuid, PHYSICAL_ADDRESS);
        }
    }

}
