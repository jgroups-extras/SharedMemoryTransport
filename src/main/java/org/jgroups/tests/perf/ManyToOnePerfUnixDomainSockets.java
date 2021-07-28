package org.jgroups.tests.perf;

import org.jgroups.util.DefaultThreadFactory;
import org.jgroups.util.Runner;
import org.jgroups.util.ThreadFactory;
import org.jgroups.util.Util;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.LongAdder;

/**
 * Tests performance of UNIX domain sockets. Start one receiver with sender=false (this one needs
 * to be started first), and all others with sender=true. The receiver prints stats every N seconds.
 * @author Bela Ban (belaban@gmail.com)
 */
public class ManyToOnePerfUnixDomainSockets {
    protected SocketAddress      address;
    protected final LongAdder    msgs_received=new LongAdder();
    protected final LongAdder    bytes_received=new LongAdder();
    protected static final long  STATS_INTERVAL=10_000; // interval (ms) at which we print stats

    protected void start(int msg_size, int num_threads, boolean sender, String shared_file) throws Exception {

        // To use Unix Doamin Sockets, uncomment below

        address=new InetSocketAddress(InetAddress.getLoopbackAddress(), 7800); //UnixDomainSocketAddress.of(shared_file);
        if(sender)
            startSenders(msg_size, num_threads);
        else {
            File tmp=new File(shared_file);
            tmp.deleteOnExit();
            startReceiver(msg_size);
        }
    }

    public void startReceiver(int msg_size) throws Exception {

        // To use Unix Doamin Sockets, uncomment below

        try(var srv_ch=ServerSocketChannel.open(); /*ServerSocketChannel.open(UNIX);*/ var runner=createRunner()) {
            srv_ch.bind(address);
            runner.start();
            for(;;) {
                try {
                    var client_ch=srv_ch.accept();
                    Receiver r=new Receiver(client_ch, msg_size);
                    r.start();
                }
                catch(IOException ioex) {
                    ioex.printStackTrace();
                }
            }
        }
    }

    public void startSenders(int msg_size, int num_threads) throws IOException {
        try(var ch=SocketChannel.open(address)) {
            Sender[] senders=new Sender[num_threads];
            LongAdder sent_msgs=new LongAdder();
            for(int i=0; i < senders.length; i++) {
                senders[i]=new Sender(ch, msg_size, sent_msgs);
                senders[i].setName("sender-" + i);
                senders[i].start();
            }

            for(;;) {
                long msgs_before=sent_msgs.sum(), bytes_before=msgs_before*msg_size;
                Util.sleep(STATS_INTERVAL);
                long msgs_after=sent_msgs.sumThenReset(), bytes_after=msgs_after * msg_size;
                if(msgs_after > msgs_before) {
                    long msgs=msgs_after-msgs_before, bytes=bytes_after-bytes_before;
                    double msgs_per_sec=msgs / (STATS_INTERVAL / 1000.0),
                      bytes_per_sec=bytes/(STATS_INTERVAL/1000.0);
                    System.out.printf("-- sent %,.2f msgs/sec %s/sec\n",
                                      msgs_per_sec, Util.printBytes(bytes_per_sec));
                }
            }

        }
    }



    protected Runner createRunner() {
        ThreadFactory f=new DefaultThreadFactory("receiver-stats", true, true);
        return new Runner(f, "stats", this::printReceiverStats, null);
    }

    protected void printReceiverStats() {
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

    protected class Receiver extends Thread {
        protected final SocketChannel client_ch;
        protected final int           msg_size;
        protected final ByteBuffer    read_buffer;

        public Receiver(SocketChannel client_ch, int msg_size) {
            this.client_ch=client_ch;
            this.msg_size=msg_size;
            read_buffer=ByteBuffer.allocate(msg_size);
        }

        public void run() {
            try {
                for(;;) {
                    int read=client_ch.read(read_buffer);
                    if(read == -1)
                        break;
                    msgs_received.increment();
                    bytes_received.add(msg_size);
                    read_buffer.flip();
                }
            }
            catch(Exception ex) {
                ex.printStackTrace();
            }
            finally {
                Util.close(client_ch);
            }
        }
    }

    protected static class Sender extends Thread {
        protected final SocketChannel ch;
        protected final int           size;
        protected final LongAdder     sent;

        public Sender(SocketChannel ch, int size, LongAdder sent) {
            this.ch=ch;
            this.size=size;
            this.sent=sent;
        }

        @Override public void run() {
            ByteBuffer write_buffer=ByteBuffer.allocate(size);
            for(;;) {
                try {
                    ch.write(write_buffer);
                    write_buffer.flip();
                    sent.increment();
                }
                catch(IOException e) {
                    e.printStackTrace();
                    break;
                }
            }
        }
    }


    public static void main(String[] args) throws Exception {
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

        final ManyToOnePerfUnixDomainSockets test=new ManyToOnePerfUnixDomainSockets();
        test.start(msg_size, num_threads, sender, shared_file);
    }


}
