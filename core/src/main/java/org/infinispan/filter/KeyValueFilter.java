package org.infinispan.filter;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.metadata.Metadata;

import net.jcip.annotations.ThreadSafe;

/**
 * A filter for keys with their values.
 *
 * @author William Burns
 * @since 7.0
 */
@ThreadSafe
public interface KeyValueFilter<K, V> {
   /**
    * @param key key to test
    * @param value value to use (could be null for the case of removal)
    * @param metadata metadata
    * @return true if the given key is accepted by this filter.
    */
   boolean accept(K key, V value, Metadata metadata);

   /**
    * @return The desired data format to be used in the {@link #accept(Object, Object, Metadata)} operation.
    * If null, the filter will receive data as it's stored.
    */
   default MediaType format() {
      return MediaType.APPLICATION_OBJECT;
   }
}
