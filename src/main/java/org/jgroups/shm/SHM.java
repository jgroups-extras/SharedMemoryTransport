package org.jgroups.shm;

import org.jgroups.PhysicalAddress;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.Property;
import org.jgroups.conf.AttributeType;
import org.jgroups.protocols.TP;

/**
 * Transport using shared memory to exchange messages
 * @author Bela Ban
 * @since  1.0.0
 */
@MBean(description="Transport which exchanges messages by adding them to shared memory. This works only when all " +
  "members are processes on the same host")
public class SHM extends TP {

    @Property(description="Folder under which the memory-mapped files for the queues are created.")
    protected String location="/tmp/shm";

    @Property(description="Max capacity of a queue (in bytes)",type=AttributeType.BYTES)
    protected int queue_capacity=0xffff;



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
        return null;
    }

    @Override
    public void sendMulticast(byte[] data, int offset, int length) throws Exception {

    }

    @Override
    public void sendUnicast(PhysicalAddress dest, byte[] data, int offset, int length) throws Exception {

    }



}
