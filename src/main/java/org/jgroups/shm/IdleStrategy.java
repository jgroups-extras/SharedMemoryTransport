package org.jgroups.shm;

import java.util.concurrent.locks.LockSupport;

@FunctionalInterface
public interface IdleStrategy {

   /**
    * Default number of times the strategy will spin without work before going to next state.
    */
   long DEFAULT_MAX_SPINS = 10L;

   /**
    * Default number of times the strategy will yield without work before going to next state.
    */
   long DEFAULT_MAX_YIELDS = 5L;

   /**
    * Default minimum interval the strategy will park a thread.
    */
   long DEFAULT_MIN_PARK_PERIOD_NS = 1000L;

   /**
    * Default maximum interval the strategy will park a thread.
    */
   long DEFAULT_MAX_PARK_PERIOD_NS = 1_000_000L;

   void idle(final int workCount);

   static IdleStrategy backoffIdle() {
      return backoffIdle(DEFAULT_MAX_SPINS, DEFAULT_MAX_YIELDS, DEFAULT_MIN_PARK_PERIOD_NS, DEFAULT_MAX_PARK_PERIOD_NS);
   }

   static IdleStrategy backoffIdle(final long maxSpins,
                                   final long maxYields,
                                   final long minParkPeriodNs,
                                   final long maxParkPeriodNs) {
      final int NOT_IDLE = 0;
      final int SPINNING = 1;
      final int YIELDING = 2;
      final int PARKING = 3;
      return new IdleStrategy() {

         private int state = NOT_IDLE;
         private long spins;
         private long yields;
         private long parkPeriodNs;

         @Override
         public void idle(final int workCount) {
            if (workCount > 0) {
               reset();
            } else {
               idle();
            }
         }

         private void reset() {
            spins = 0;
            yields = 0;
            parkPeriodNs = minParkPeriodNs;
            state = NOT_IDLE;
         }

         private void idle() {
            switch (state) {
               case NOT_IDLE:
                  state = SPINNING;
                  spins++;
                  break;

               case SPINNING:
                  Thread.onSpinWait();
                  if (++spins > maxSpins) {
                     state = YIELDING;
                     yields = 0;
                  }
                  break;

               case YIELDING:
                  if (++yields > maxYields) {
                     state = PARKING;
                     parkPeriodNs = minParkPeriodNs;
                  } else {
                     Thread.yield();
                  }
                  break;

               case PARKING:
                  LockSupport.parkNanos(parkPeriodNs);
                  parkPeriodNs = Math.min(parkPeriodNs << 1, maxParkPeriodNs);
                  break;
            }
         }
      };
   }
}
