package org.infinispan.notifications.cachelistener;

import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.IntConsumer;

import org.infinispan.container.entries.CacheEntry;
import org.infinispan.notifications.cachelistener.event.Event;
import org.infinispan.notifications.impl.ListenerInvocation;
import org.infinispan.util.concurrent.CompletableFutures;

/**
 * This interface describes methods needed for a segment listener that is used when iterating over the current
 * events and be able to queue them properly
 *
 * @author wburns
 * @since 7.0
 */
public interface QueueingSegmentListener<K, V, E extends Event<K, V>> extends IntConsumer {
   // This is to be used as a placeholder when a value has been iterated and now is being processed by the caller
   // This is considered to be the completed state for the key and should never change from this
   static final Object NOTIFIED = new Object();
   // This is to be used as a placeholder for a removed value.  This is needed so that we know
   // a value is removed.  The caller will get this back when processing a key and should then ignore
   // or do it's own special processing for removed values
   static final Object REMOVED = new Object();

   /**
    * This should be invoked on a key before actually processing the data.  This way the handler knows to
    * keep any newer events have come after the iteration.
    * @param key The key being processed
    * @return The previous value that was found to be updated,
    * {@link BaseQueueingSegmentListener#NOTIFIED} if the key was
    * previously marked as processing or
    * {@link BaseQueueingSegmentListener#REMOVED} if the key was removed
    * and this value shouldn't be processed
    */
   public Object markKeyAsProcessing(K key);

   /**
    * This method is to be called just before marking the transfer as complete and after all keys have been manually
    * processed.  This will return all the entries that were raised in an event but not manually marked.  This
    * is indicative of a CREATE event occurring but not seeing the value.
    * @return
    */
   public Set<CacheEntry<K, V>> findCreatedEntries();

   /**
    * Invoked to determine if processing should be delayed or not. Will return
    * {@link CompletableFutures#completedNull()} if processing can continue immediately, otherwise should wait until this
    * is complete.
    * @return null if no notifications are required or a non null CompletionStage that when completed all notifications are done
    */
   public CompletionStage<Void> delayProcessing();

   /**
    * This should be called by any listener when an event is generated to possibly queue it.  If it is not
    * queued, then the caller should take appropriate action such as manually firing the invocation.
    * @param wrapper The event that was just raised
    * @param invocation The invocation the event would be fired on
    * @return Whether or not it was queued.  If it wasn't queued the invocation should be fired manually
    */
   public boolean handleEvent(EventWrapper<K, V, E> wrapper, ListenerInvocation<Event<K, V>> invocation);

   /**
    * This is needed to tell the handler when the complete iteration is done.  Depending on the implementation
    * this could also fire all queued events that are remaining.
    */
   public CompletionStage<Void> transferComplete();
}
