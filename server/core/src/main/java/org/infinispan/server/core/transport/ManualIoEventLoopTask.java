package org.infinispan.server.core.transport;

import io.netty.channel.IoEventLoopGroup;
import io.netty.channel.IoHandlerFactory;
import io.netty.channel.ManualIoEventLoop;

import java.util.concurrent.TimeUnit;

/**
 * A {@link ManualIoEventLoop} that runs as a virtual thread.
 * Simplified from the Netty VirtualThread Scheduler project:
 * no AwakeAwareIoHandler, no custom scheduler dependency.
 *
 * <p>The run loop alternates between blocking waits (when idle) and
 * non-blocking polls (when active), yielding between non-blocking
 * iterations to cooperate with the ForkJoinPool carrier scheduler.
 */
public class ManualIoEventLoopTask extends ManualIoEventLoop implements Runnable {

   /**
    * Duration budget (in nanoseconds) for processing async tasks
    * per iteration. 50 microseconds matches the VT scheduler default.
    */
   private static final long ASYNC_TASKS_NS = TimeUnit.MICROSECONDS.toNanos(50);

   public ManualIoEventLoopTask(IoEventLoopGroup parent, Thread owningThread, IoHandlerFactory factory) {
      super(parent, owningThread, factory);
   }

   @Override
   public void run() {
      int events = 0;
      while (!isShuttingDown()) {
         if (events == 0) {
            // No events last round — do a blocking wait
            events = run(0, ASYNC_TASKS_NS);
         } else {
            // Had events — yield and poll non-blocking
            Thread.yield();
            events = runNow(ASYNC_TASKS_NS);
         }
      }
      // Drain remaining tasks during shutdown
      while (!isTerminated()) {
         runNow();
         Thread.yield();
      }
   }
}
