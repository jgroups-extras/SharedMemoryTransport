package org.jgroups.tests.perf;

import org.jgroups.*;
import org.jgroups.protocols.TP;
import org.jgroups.util.DefaultThreadFactory;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.ThreadFactory;
import org.jgroups.util.Util;

import java.util.Objects;
import java.util.concurrent.atomic.LongAdder;

/**
 * Tests performance multiple senders sending messages to a single receiver (the first member in the cluster)
 * @author Bela Ban (belaban@gmail.com)
 */
public class ManyToOnePerfJGroups implements Receiver {
    protected JChannel           ch;
    protected volatile Address   coord;
    protected final LongAdder    msgs_received=new LongAdder();
    protected final LongAdder    bytes_received=new LongAdder();
    protected ThreadFactory      thread_factory;
    protected static final long  STATS_INTERVAL=2_000; // interval (ms) at which we print stats

    protected void start(int msg_size, int num_threads, String props, String name, boolean use_fibers) throws Exception {
        thread_factory=new DefaultThreadFactory("invoker", false, true)
          .useFibers(use_fibers);
        ch=new JChannel(props).setName(name).setReceiver(this);
        if(use_fibers && Util.fibersAvailable()) {
            TP transport=ch.getProtocolStack().getTransport();
            transport.useFibers(true);
            System.out.println("-- using fibers instead of threads");
        }
        ch.connect("many-to-one-perf");
        View v=ch.getView();
        coord=v.getCoord();
        boolean receiver=Objects.equals(ch.getAddress(), coord);
        if(receiver) {
            System.out.println("** first member, will act as receiver");
            startReceiver();
        }
        else {
            System.out.printf("** %s: will send messages to %s\n", ch.getAddress(), coord);
            ch.setReceiver(null);
            startSenders(msg_size, num_threads);
        }
    }

    public void startReceiver() {
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
            Thread t=thread_factory.newThread(senders[i], "sender-" + i);
            t.start();
        }
        for(;;) {
            long msgs_before=sent_msgs.sum(), bytes_before=msgs_before*msg_size;
            Util.sleep(STATS_INTERVAL);
            long msgs_after=sent_msgs.sumThenReset(), bytes_after=msgs_after * msg_size;
            if(msgs_after > msgs_before) {
                long msgs=msgs_after-msgs_before, bytes=bytes_after-bytes_before;
                double msgs_per_sec=msgs / (STATS_INTERVAL / 1000.0),
                  bytes_per_sec=bytes/(STATS_INTERVAL/1000.0);
                System.out.printf("-- sent %,.2f msgs/sec %s/sec\n", msgs_per_sec, Util.printBytes(bytes_per_sec));
            }
        }
    }

    @Override
    public void viewAccepted(View v) {
        Address new_coord=v.getCoord();
        boolean has_new_coord=Objects.equals(new_coord, coord);
        System.out.printf("** view: %s %s\n", v, has_new_coord? String.format("(new coord: %s)", new_coord) : "");
        coord=new_coord;
    }

    @Override
    public void receive(Message msg) {
        byte[] buf=msg.getArray();
        msgs_received.increment();
        bytes_received.add(buf.length);
    }

    @Override
    public void receive(MessageBatch batch) {
        msgs_received.add(batch.size());
        bytes_received.add(batch.length());
    }

    protected class Sender implements Runnable {
        protected final int       size;
        protected final LongAdder sent;

        public Sender(int size, LongAdder sent) {
            this.size=size;
            this.sent=sent;
        }

        @Override public void run() {
            byte[] buffer=new byte[size];
            for(;;) {
                try {
                    // we can't use DONT_LOOPBACK: https://issues.redhat.com/browse/JGRP-1835
                    Message msg=new BytesMessage(null, buffer); // .setFlag(Message.TransientFlag.DONT_LOOPBACK);

                    // if these 2 flags are used (=uncommented), then perf increases greatly!
                    // msg.setFlag(Message.Flag.NO_FC);
                    // msg.setFlag(Message.Flag.NO_RELIABILITY);

                    ch.send(msg);
                    sent.increment();
                }
                catch(Exception e) {
                    e.printStackTrace();
                    break;
                }
            }
        }
    }


    public static void main(String[] args) throws Exception {
        int msg_size=1000, num_threads=100;
        String props="shm.xml", name=null;
        boolean use_fibers=true;

        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-msg_size")) {
                msg_size=Integer.parseInt(args[++i]);
                continue;
            }
            if("-num_threads".equals(args[i])) {
                num_threads=Integer.parseInt(args[++i]);
                continue;
            }
            if("-props".equals(args[i])) {
                props=args[++i];
                continue;
            }
            if("-name".equals(args[i])) {
                name=args[++i];
                continue;
            }
            if("-use_fibers".equals(args[i])) {
                use_fibers=Boolean.parseBoolean(args[++i]);
                continue;
            }
            System.out.println("ManyToOnePerfJGroups [-msg_size <bytes>] [-num_threads <threads>] " +
                                 "[-props <config>] [-name <name>] [-use_fibers true|false]");
            return;
        }


        final ManyToOnePerfJGroups test=new ManyToOnePerfJGroups();
        test.start(msg_size, num_threads, props, name, use_fibers);
    }


}
