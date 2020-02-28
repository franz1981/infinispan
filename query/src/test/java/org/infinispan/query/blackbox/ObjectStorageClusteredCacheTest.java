package org.infinispan.query.blackbox;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT_TYPE;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.query.test.Person;
import org.infinispan.query.test.QueryTestSCI;
import org.testng.annotations.Test;

/**
 * Verify queries with configured media type as objects.
 *
 * @author Martin Gencur
 * @since 6.0
 */
@Test(groups = "functional", testName = "query.blackbox.ObjectStorageClusteredCacheTest")
public class ObjectStorageClusteredCacheTest extends ClusteredCacheTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cacheCfg = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, transactionsEnabled());
      cacheCfg
            .encoding().key().mediaType(APPLICATION_OBJECT_TYPE)
            .encoding().value().mediaType(APPLICATION_OBJECT_TYPE)
            .indexing()
            .enable()
            .addIndexedEntity(Person.class)
            .addProperty("default.directory_provider", "local-heap")
            .addProperty("error_handler", "org.infinispan.query.helper.StaticTestingErrorHandler")
            .addProperty("lucene_version", "LUCENE_CURRENT");
      enhanceConfig(cacheCfg);
      createClusteredCaches(2, QueryTestSCI.INSTANCE, cacheCfg);
      cache1 = cache(0);
      cache2 = cache(1);
   }

}
