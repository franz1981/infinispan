package org.infinispan.server.core.transport;

import io.netty.channel.IoEventLoop;
import io.netty.channel.IoHandlerFactory;
import io.netty.channel.MultiThreadIoEventLoopGroup;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * Event loop group for virtual thread-based NIO transport using stickyAffinity.
 * Each event loop runs as a virtual thread with sticky carrier affinity, using
 * the JDK's built-in ForkJoinPool scheduler.
 *
 * <p>Simplified from the Netty VirtualThread Scheduler project — uses
 * {@code Thread.ofVirtual().stickyAffinity()} directly instead of a custom scheduler.
 * All virtual thread APIs are invoked via MethodHandle since the project targets
 * Java 17 source level while stickyAffinity requires the custom Loom JDK (28-loom).
 *
 * <p>Must be used with NIO transport (NioIoHandler), not epoll/io_uring.
 */
public class VirtualIoEventLoopGroup extends MultiThreadIoEventLoopGroup {

   // --- MethodHandles for Java 21+ virtual thread APIs and Loom-specific stickyAffinity ---

   private static final MethodHandle OF_VIRTUAL;         // Thread.ofVirtual() -> OfVirtual
   private static final MethodHandle VB_NAME;             // OfVirtual.name(String, long) -> OfVirtual
   private static final MethodHandle VB_FACTORY;          // OfVirtual.factory() -> ThreadFactory
   private static final MethodHandle VB_STICKY_AFFINITY;  // OfVirtual.stickyAffinity() -> OfVirtual (Loom-only)
   private static final MethodHandle NEW_AFFINITY_GROUP;  // Thread.Affinity.newGroup() -> Affinity.Group (Loom-only)
   private static final MethodHandle VB_AFFINITY_GROUP;   // OfVirtual.affinity(Affinity.Group) -> OfVirtual (Loom-only)

   static {
      try {
         MethodHandles.Lookup lookup = MethodHandles.publicLookup();
         Class<?> ofVirtual = Class.forName("java.lang.Thread$Builder$OfVirtual");

         OF_VIRTUAL = lookup.findStatic(Thread.class, "ofVirtual",
               MethodType.methodType(ofVirtual));
         VB_NAME = lookup.findVirtual(ofVirtual, "name",
               MethodType.methodType(ofVirtual, String.class, long.class));
         VB_FACTORY = lookup.findVirtual(ofVirtual, "factory",
               MethodType.methodType(ThreadFactory.class));

         MethodHandle sticky = null;
         try {
            sticky = lookup.findVirtual(ofVirtual, "stickyAffinity",
                  MethodType.methodType(ofVirtual));
         } catch (NoSuchMethodException e) {
            // stickyAffinity not available — plain VTs will be used
         }
         VB_STICKY_AFFINITY = sticky;

         MethodHandle newGroup = null;
         MethodHandle affinityGroup = null;
         try {
            Class<?> group = Class.forName("java.lang.Thread$Affinity$Group");
            newGroup = lookup.findStatic(Class.forName("java.lang.Thread$Affinity"), "newGroup",
                  MethodType.methodType(group));
            affinityGroup = lookup.findVirtual(ofVirtual, "affinity",
                  MethodType.methodType(ofVirtual, group));
         } catch (ClassNotFoundException | NoSuchMethodException e) {
            // Thread.Affinity not available — event loops are not spread over carriers
         }
         NEW_AFFINITY_GROUP = newGroup;
         VB_AFFINITY_GROUP = affinityGroup;
      } catch (ReflectiveOperationException e) {
         throw new ExceptionInInitializerError(
               "Virtual threads require Java 21+. Current JDK does not support Thread.ofVirtual(): " + e);
      }
   }

   private final ThreadFactory virtualThreadFactory;

   /**
    * Creates a group with {@code nThreads} event loops.
    * Each event loop runs as a virtual thread with stickyAffinity (if available).
    */
   public VirtualIoEventLoopGroup(int nThreads, IoHandlerFactory factory) {
      this(nThreads, factory, createStickyVirtualThreadFactory());
   }

   /**
    * Creates a group with {@code nThreads} event loops using the given virtual thread factory.
    */
   public VirtualIoEventLoopGroup(int nThreads, IoHandlerFactory factory, ThreadFactory threadFactory) {
      super(nThreads, (Executor) null, factory, threadFactory);
      this.virtualThreadFactory = threadFactory;
   }

   @Override
   protected IoEventLoop newChild(Executor executor, IoHandlerFactory ioHandlerFactory, Object... args) {
      ThreadFactory tf;
      if (args.length > 0 && args[0] instanceof ThreadFactory) {
         tf = (ThreadFactory) args[0];
      } else {
         tf = virtualThreadFactory;
      }
      ManualIoEventLoopTask manualTask = new ManualIoEventLoopTask(this, null, ioHandlerFactory);
      Thread newThread = tf.newThread(manualTask);
      manualTask.setOwningThread(newThread);
      newThread.start();
      return manualTask;
   }

   /**
    * Creates a ThreadFactory for virtual threads with stickyAffinity and a shared affinity group.
    * Falls back to plain virtual threads if stickyAffinity or Thread.Affinity is not available.
    */
   private static ThreadFactory createStickyVirtualThreadFactory() {
      try {
         // Thread.ofVirtual()
         Object builder = OF_VIRTUAL.invoke();
         // .name("infinispan-vio-", 0)
         builder = VB_NAME.invoke(builder, "infinispan-vio-", 0L);
         // .stickyAffinity() — if available
         if (VB_STICKY_AFFINITY != null) {
            builder = VB_STICKY_AFFINITY.invoke(builder);
         }
         // .affinity(Thread.Affinity.newGroup()) — one group for all event loops, so they spread one per carrier
         if (VB_AFFINITY_GROUP != null) {
            builder = VB_AFFINITY_GROUP.invoke(builder, NEW_AFFINITY_GROUP.invoke());
         }
         // .factory()
         return (ThreadFactory) VB_FACTORY.invoke(builder);
      } catch (Throwable e) {
         throw new IllegalStateException("Failed to create virtual thread factory: " + e, e);
      }
   }
}
