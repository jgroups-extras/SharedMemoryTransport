package org.jgroups.protocols.shm;

import org.jgroups.Address;
import org.jgroups.PhysicalAddress;
import org.jgroups.View;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.conf.AttributeType;
import org.jgroups.protocols.LocalTransport;
import org.jgroups.protocols.TP;
import org.jgroups.shm.ManyToOneBoundedChannel;
import org.jgroups.shm.SharedMemoryBuffer;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.UUID;
import org.jgroups.util.*;

import java.io.File;
import java.io.IOException;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Implementation of {@link org.jgroups.protocols.LocalTransport} based on shared memory
 * @author Bela Ban
 * @since  1.0.0
 */
@MBean(description="Implementation of LocalTransport using shared memory")
public class SharedMemoryLocalTransport implements LocalTransport, Consumer<ByteBuffer> {
    protected TP                                    tp;

    @Property(description="Folder under which the memory-mapped files for the queues are created.")
    protected String                                location="/tmp/shm";

    @Property(description="Max capacity of a queue (in bytes)",type=AttributeType.BYTES)
    protected int                                   queue_capacity=2 << 22; // ca 8MB

    @Property(description="The max time (in millis) a receiver loop should park when idle. 0=default",
      type=AttributeType.TIME)
    protected long                                  max_sleep;

    protected SharedMemoryBuffer                    buf;

    protected ByteBufferInputStream                 cachedReceiveStream;

    @ManagedAttribute(description="List of _all_ members of the current view")
    protected final Set<Address>                    members=new CopyOnWriteArraySet<>();

    protected Collection<InetAddress>               local_addresses;
    @ManagedAttribute(description="List of members with local addresses (same-host members) of the current view")
    protected final List<Address>                   local_members=new ArrayList<>();

    protected final Map<Address,SharedMemoryBuffer> cache=new ConcurrentHashMap<>();

    protected final LongAdder                       num_unicasts=new LongAdder();
    protected final LongAdder                       num_mcasts=new LongAdder();

    protected static final String LOCATION="location", QUEUE_CAPACITY="queue_capacity", MAX_SLEEP="max_sleep";



    @ManagedAttribute(description="The sum of failed writes due to insufficient capacity of all ring buffers")
    public int getFailedWritesDueToInsufficientCapacity() {
        return cache.values().stream().mapToInt(c -> (int)c.insufficientCapacity()).sum();
    }

    @ManagedAttribute(description="Number of unicasts sent via this transport",type=AttributeType.SCALAR)
    public long localUnicasts() {return num_unicasts.sum();}

    @ManagedAttribute(description="Number of multicasts sent via this transport",type=AttributeType.SCALAR)
    public long localMulticasts() {return num_mcasts.sum();}

    @ManagedOperation(description="Print the local addresses of this host")
    public String getLocalAddresses() {return local_addresses != null? local_addresses.toString() : "null";}

    @ManagedOperation(description="Changes max_sleep")
    public void maxSleep(long ms) {
        this.max_sleep=ms;
        if(buf != null)
            buf.maxSleep(ms);
    }

    public boolean isLocalMember(Address a) {
        return local_members != null && local_members.contains(a);
    }

    @Override
    public LocalTransport resetStats() {
        cache.values().forEach(SharedMemoryBuffer::resetStats);
        num_unicasts.reset();
        num_mcasts.reset();
        return this;
    }



    @Override
    public LocalTransport init(TP transport) {
        this.tp=Objects.requireNonNull(transport);

        Collection<InetAddress> addrs=Util.getAllAvailableAddresses(null);
        local_addresses=new ArrayList<>(addrs.size());
        // add IPv4 addresses at the head, IPv6 address at the end
        local_addresses.addAll(addrs.stream().filter(a -> a instanceof Inet4Address).collect(Collectors.toList()));
        local_addresses.addAll(addrs.stream().filter(a -> a instanceof Inet6Address).collect(Collectors.toList()));
        return this;
    }


    @Override
    public LocalTransport start() throws Exception {
        int cap=Util.getNextHigherPowerOfTwo(queue_capacity);
        if(queue_capacity != cap) {
            tp.getLog().warn("queue_capacity (%d) must be a power of 2, changing it to %d", queue_capacity, cap);
            queue_capacity=cap;
        }
        File f=new File(location);
        if(!f.exists())
            throw new IllegalArgumentException(String.format("location %s does not exist", location));

        try {
            buf=createBuffer(tp.localAddress(), null, true, tp.getThreadFactory())
              .setConsumer(this).deleteFileOnExit(true);
            if(max_sleep > 0)
                buf.maxSleep(max_sleep);
            cache.putIfAbsent(tp.localAddress(), buf);
            initCache();
        }
        catch(IOException ex) {
            tp.getLog().error("failed creating buffer", ex);
        }
        return this;
    }

    @Override
    public LocalTransport stop() {
        Util.close(buf);
        return this;
    }

    public LocalTransport destroy() {
        return this;
    }

    public LocalTransport viewChange(View v) {
        members.clear();
        members.addAll(v.getMembers());
        cache.keySet().retainAll(members);
        local_members.clear();
        for(Address mbr: members) {
            PhysicalAddress pa=tp.getPhysicalAddressFromCache(mbr);
            if(pa == null || Objects.equals(pa, tp.localPhysicalAddress()))
                continue;
            InetAddress addr=pa instanceof IpAddress? ((IpAddress)pa).getIpAddress() : null;
            if(addr != null && local_addresses.contains(addr))
                if(!local_members.contains(mbr))
                    local_members.add(mbr);
        }
        return this;
    }

    @Override
    public void accept(ByteBuffer bb) {
        try {
            ByteBufferInputStream receiveStream = this.cachedReceiveStream;
            if (receiveStream == null || receiveStream.buf() != bb) {
                receiveStream =new ByteBufferInputStream(bb);
                this.cachedReceiveStream=null;
            }
            tp.receive(null, receiveStream);
        }
        catch(Exception ex) {
            tp.getLog().error("failed handling message", ex);
        }
    }


    @Override
    public void sendTo(Address dest, byte[] buf, int offset, int length) throws Exception {
        _sendTo(dest, buf, offset, length);
        num_unicasts.increment();
    }


    @Override
    public void sendToAll(byte[] buf, int offset, int length) throws Exception {
        Set<Address>  mbrs=members;
        if(mbrs == null || mbrs.isEmpty())
            mbrs=tp.getLogicalAddressCache().keySet();

        for(Address dest: mbrs) {
            if(Objects.equals(dest, tp.localAddress()))
                continue;
            if(isLocalMember(dest)) // takes null values into account
                _sendTo(dest, buf, offset, length);
        }
        num_mcasts.increment();
    }


    protected void _sendTo(Address dest, byte[] buf, int offset, int length) throws Exception {
        SharedMemoryBuffer shm_buf=getOrCreateBuffer(dest);
        if(shm_buf == null)
            throw new IllegalStateException(String.format("buffer for %s not found", dest));
        shm_buf.write(buf, offset, length);
    }


    protected SharedMemoryBuffer createBuffer(Address addr, String logical_name, boolean create,
                                              ThreadFactory thread_factory) throws IOException {
        String buffer_name=addressToFilename(addr, logical_name);
        return new SharedMemoryBuffer(buffer_name,
                                      queue_capacity+ ManyToOneBoundedChannel.TRAILER_LENGTH,
                                      create, thread_factory);
    }

    protected String addressToFilename(Address addr, String logical_name) {
        String cluster=tp.getClusterName();
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

    /** Parses config and sets attributes. Format: key1=val1;key2=val2 */
    protected void parse(String config) {
        List<String> attributes=Util.parseStringList(Objects.requireNonNull(config), ";");
        for(String s: attributes) {
            s=s.trim();
            int index=s.indexOf("=");
            if(index == -1)
                throw new IllegalArgumentException(String.format("'=' not found in %s", s));
            String key=s.substring(0, index).trim(), value=s.substring(index+1).trim();
            switch(key) {
                case LOCATION:
                    location=value;
                    break;
                case QUEUE_CAPACITY:
                    queue_capacity=Util.readBytesInteger(value);
                    break;
                case MAX_SLEEP:
                    max_sleep=Long.parseLong(value);
                    break;
                default:
                    throw new IllegalArgumentException(String.format("attribute %s not known", key));
            }
        }
    }

    protected SharedMemoryBuffer getOrCreateBuffer(Address addr) throws IOException {
        SharedMemoryBuffer shm_buf=cache.get(addr);
        if(shm_buf == null) {
            shm_buf=createBuffer(addr, null, false, tp.getThreadFactory());
            SharedMemoryBuffer tmp=cache.putIfAbsent(addr, shm_buf);
            if(tmp != null)
                shm_buf=tmp;
        }
        return shm_buf;
    }

    /** Reads all files under location/group/ and populates cache */
    protected void initCache() throws IOException {
        String cluster=tp.getClusterName();
        Path dir=Path.of(Objects.requireNonNull(location), Objects.requireNonNull(cluster));
        File[] files=dir.toFile().listFiles();
        for(File f: files) {
            String tmp=f.getName();
            Tuple<Address,String> t=filenameToAddress(tmp);
            String logical_name=t.getVal2();
            Address uuid=t.getVal1();
            if(!cache.containsKey(uuid))
                cache.putIfAbsent(uuid, createBuffer(uuid, logical_name, false, tp.getThreadFactory()));
            tp.addPhysicalAddressToCache(uuid, tp.localPhysicalAddress());
        }
    }


}
