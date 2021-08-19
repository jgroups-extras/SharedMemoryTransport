package org.jgroups.shm.mpsc;

import org.jgroups.shm.ManyToOneBoundedChannel;
import org.jgroups.util.Util;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.atomic.AtomicLong;

import static org.jgroups.shm.ByteBufferUtils.copyBytes;
import static org.jgroups.shm.ManyToOneBoundedChannel.INSUFFICIENT_CAPACITY;
import static org.jgroups.shm.ManyToOneBoundedChannel.claimedIndex;

/**
 * @author Bela Ban
 * @since x.y
 */
public class ManyToOneBufferTest {

   @DataProvider(name = "testConfiguration")
   public static Object[][] testConfiguration() {
      return new Object[][]{{true}, {false}};
   }

   @Test(dataProvider = "testConfiguration")
   public void testWriteAndRead(boolean direct) throws IOException {
      ByteBuffer buf = (direct ? ByteBuffer.allocateDirect(1024 + ManyToOneBoundedChannel.TRAILER_LENGTH) : ByteBuffer.allocate(1024 + ManyToOneBoundedChannel.TRAILER_LENGTH)).order(ByteOrder.nativeOrder());
      ManyToOneBoundedChannel rb = new ManyToOneBoundedChannel(buf);
      byte[] buf1 = Util.objectToByteBuffer("hello world");
      byte[] buf2 = Util.objectToByteBuffer("from Bela Ban");

      long claim1 = rb.tryClaim(1, buf1.length), claim2 = rb.tryClaim(1, buf2.length);
      Assert.assertTrue(claim1 != ManyToOneBoundedChannel.INSUFFICIENT_CAPACITY);
      Assert.assertTrue(claim2 != ManyToOneBoundedChannel.INSUFFICIENT_CAPACITY);
      {
         final int index = claimedIndex(claim1);
         final int prePosition = rb.buffer().position();
         copyBytes(buf1, 0, rb.buffer(), index, buf1.length);
         rb.commit(claim1);
         Assert.assertEquals(prePosition, rb.buffer().position());
      }
      {
         final int index = claimedIndex(claim2);
         final int prePosition = rb.buffer().position();
         copyBytes(buf2, 0, rb.buffer(), index, buf2.length);
         rb.commit(claim2);
         Assert.assertEquals(prePosition, rb.buffer().position());
      }
      int num_msgs = rb.read((msgTypeId, buffer, offset, length) -> {
         ByteBuffer b = buffer.position(offset);
         byte[] tmp = new byte[length];
         b.get(tmp, 0, tmp.length);
         String s = null;
         try {
            s = Util.objectFromByteBuffer(tmp);
            System.out.println("s = " + s);
         } catch (Exception e) {
            e.printStackTrace();
         }
      });
      assert num_msgs == 2;
   }

   @Test(dataProvider = "testConfiguration")
   public void testWriteReadWrapping(boolean direct) {
      ByteBuffer buf = (direct ?
         ByteBuffer.allocateDirect(64 + ManyToOneBoundedChannel.TRAILER_LENGTH) :
         ByteBuffer.allocate(64 + ManyToOneBoundedChannel.TRAILER_LENGTH)).order(ByteOrder.nativeOrder());
      ManyToOneBoundedChannel rb = new ManyToOneBoundedChannel(buf);
      for (int i = 0; i < 4; i++) {
         long claim = rb.tryClaim(1, Long.BYTES);
         Assert.assertTrue(claim != INSUFFICIENT_CAPACITY);
         rb.buffer().putLong(claimedIndex(claim), (i + 1));
         rb.commit(claim);
      }
      final long failedClaim = rb.tryClaim(1, Long.BYTES);
      Assert.assertEquals(failedClaim, INSUFFICIENT_CAPACITY);
      Assert.expectThrows(IllegalArgumentException.class, () -> rb.commit(failedClaim));
      final AtomicLong expectedValue = new AtomicLong();
      int num_msgs = rb.read((msgTypeId, buffer, offset, length) -> {
         Assert.assertEquals(length, Long.BYTES);
         Assert.assertEquals(msgTypeId, 1);
         Assert.assertEquals(buffer.getLong(offset), expectedValue.incrementAndGet());
      });
      for (int i = 0; i < 2; i++) {
         long claim = rb.tryClaim(1, Long.BYTES);
         Assert.assertTrue(claim != INSUFFICIENT_CAPACITY);
         rb.buffer().putLong(claimedIndex(claim), (4 + i + 1));
         rb.commit(claim);
      }
      num_msgs = rb.read((msgTypeId, buffer, offset, length) -> {
         Assert.assertEquals(length, Long.BYTES);
         Assert.assertEquals(msgTypeId, 1);
         Assert.assertEquals(buffer.getLong(offset), expectedValue.incrementAndGet());
      });

      Assert.assertEquals(num_msgs, 2);
      Assert.assertEquals(rb.size(), 0);
   }
}
