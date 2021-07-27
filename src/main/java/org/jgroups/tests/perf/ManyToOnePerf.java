package org.jgroups.tests.perf;

import org.agrona.concurrent.ringbuffer.RingBufferDescriptor;
import org.jgroups.Receiver;
import org.jgroups.shm.SharedMemoryBuffer;
import org.jgroups.util.Util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;

/**
 * Tests performance of {@link org.jgroups.shm.SharedMemoryBuffer}. Start one receiver with sender=false (this one needs
 * to be started first), and all others with sender=true. The receiver prints stats every N seconds.
 * @author Bela Ban (belaban@gmail.com)
 */
public class ManyToOnePerf implements Receiver, Consumer<ByteBuffer> {
    protected SharedMemoryBuffer buf;
    protected final LongAdder    msgs_received=new LongAdder();
    protected final LongAdder    bytes_received=new LongAdder();
    protected byte[]             receive_buffer;
    protected static final long  STATS_INTERVAL=10_000; // interval (ms) at which we print stats

    protected void start(int msg_size, int num_threads, boolean sender, String shared_file, int queue_size)
      throws IOException {
        buf=new SharedMemoryBuffer(shared_file, queue_size+ RingBufferDescriptor.TRAILER_LENGTH, !sender);
        if(sender)
            startSenders(msg_size, num_threads);
        else {
            buf.deleteFileOnExit(true);
            startReceiver(msg_size);
        }
    }

    public void startReceiver(int msg_size) {
        receive_buffer=new byte[msg_size];
        buf.setConsumer(this);
        for(;;) {
            long msgs_before=msgs_received.sum(), bytes_before=bytes_received.sum();
            Util.sleep(STATS_INTERVAL);
            long msgs_after=msgs_received.sumThenReset(), bytes_after=bytes_received.sumThenReset();
            if(msgs_after > msgs_before) {
                long msgs=msgs_after-msgs_before, bytes=bytes_after-bytes_before;
                double msgs_per_sec=msgs / (STATS_INTERVAL / 1000.0),
                  bytes_per_sec=bytes/(STATS_INTERVAL/1000.0);
                System.out.printf("-- read %,.2f msgs/sec %s/sec\n", msgs_per_sec, Util.printBytes(bytes_per_sec));
            }
        }
    }

    public void startSenders(int msg_size, int num_threads) {
        Sender[] senders=new Sender[num_threads];
        LongAdder sent_msgs=new LongAdder();
        for(int i=0; i < senders.length; i++) {
            senders[i]=new Sender(msg_size, sent_msgs);
            senders[i].setName("sender-" + i);
            senders[i].start();
        }
        for(;;) {
            long msgs_before=sent_msgs.sum(), bytes_before=msgs_before*msg_size,
              failed_writes_before=buf.insufficientCapacity();
            Util.sleep(STATS_INTERVAL);
            long msgs_after=sent_msgs.sumThenReset(), bytes_after=msgs_after * msg_size,
              failed_writes_after=buf.insufficientCapacity();
            buf.resetStats();
            if(msgs_after > msgs_before) {
                long msgs=msgs_after-msgs_before, bytes=bytes_after-bytes_before,
                  failed_writes=failed_writes_after-failed_writes_before;
                double msgs_per_sec=msgs / (STATS_INTERVAL / 1000.0),
                  bytes_per_sec=bytes/(STATS_INTERVAL/1000.0);
                System.out.printf("-- sent %,.2f msgs/sec %s/sec (failed writes/sec: %s)\n",
                                  msgs_per_sec, Util.printBytes(bytes_per_sec),
                                  String.format("%,.2f", failed_writes/(STATS_INTERVAL/1000.0)));
            }
        }
    }

    @Override
    public void accept(ByteBuffer buf) {
        final int length = buf.remaining();
        buf.get(receive_buffer);
        msgs_received.increment();
        bytes_received.add(length);
    }


    protected class Sender extends Thread {
        protected final int       size;
        protected final LongAdder sent;

        public Sender(int size, LongAdder sent) {
            this.size=size;
            this.sent=sent;
        }

        @Override public void run() {
            byte[] buffer=new byte[size];
            for(;;) {
                if(buf.write(buffer, 0, buffer.length))
                    sent.increment();
            }
        }
    }


    public static void main(String[] args) throws IOException {
        int msg_size=1000, num_threads=100, queue_size=2 << 22;
        boolean sender=false;
        String shared_file="/tmp/shm/perftest";

        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-msg_size")) {
                msg_size=Integer.parseInt(args[++i]);
                continue;
            }
            if("-num_threads".equals(args[i])) {
                num_threads=Integer.parseInt(args[++i]);
                continue;
            }
            if("-sender".equals(args[i])) {
                sender=Boolean.parseBoolean(args[++i]);
                continue;
            }
            if("-file".equals(args[i])) {
                shared_file=args[++i];
                continue;
            }
            if("-queue_size".equals(args[i])) {
                queue_size=Integer.parseInt(args[++i]);
                continue;
            }
            System.out.println("ManyToOnePerf [-msg_size <bytes>] [-num_threads <threads>] " +
                                 "[-sender true|false] [-file <shared file>] [-queue_size <bytes>]");
            return;
        }

        int cap=Util.getNextHigherPowerOfTwo(queue_size);
        if(queue_size != cap) {
            System.err.printf("queue_size (%d) must be a power of 2, changing it to %d\n", queue_size, cap);
            queue_size=cap;
        }

        final ManyToOnePerf test=new ManyToOnePerf();
        test.start(msg_size, num_threads, sender, shared_file, queue_size);
    }


}
