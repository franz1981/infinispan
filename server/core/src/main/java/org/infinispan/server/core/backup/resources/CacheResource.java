package org.infinispan.server.core.backup.resources;

import static org.infinispan.server.core.BackupManager.Resources.Type.CACHES;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import javax.xml.stream.XMLStreamException;

import org.infinispan.AdvancedCache;
import org.infinispan.cache.impl.InvocationHelper;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.MarshallingException;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.reactive.RxJavaInterop;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.encoding.impl.StorageConfigurationManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.persistence.PersistenceMarshaller;
import org.infinispan.marshall.protostream.impl.SerializationContextRegistry;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.InternalMetadataImpl;
import org.infinispan.metadata.impl.PrivateMetadata;
import org.infinispan.protostream.ImmutableSerializationContext;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.reactive.publisher.PublisherTransformers;
import org.infinispan.reactive.publisher.impl.ClusterPublisherManager;
import org.infinispan.reactive.publisher.impl.DeliveryGuarantee;
import org.infinispan.registry.InternalCacheRegistry;
import org.infinispan.server.core.BackupManager;
import org.infinispan.util.concurrent.AggregateCompletionStage;
import org.infinispan.util.concurrent.BlockingManager;
import org.infinispan.util.concurrent.CompletionStages;
import org.reactivestreams.Publisher;

import io.reactivex.rxjava3.core.Flowable;

/**
 * {@link org.infinispan.server.core.backup.ContainerResource} implementation for {@link
 * BackupManager.Resources.Type#CACHES}.
 *
 * @author Ryan Emerson
 * @since 12.0
 */
public class CacheResource extends AbstractContainerResource {

   private static final String MEMCACHED_CACHE = "memcachedCache";

   private final EmbeddedCacheManager cm;
   private final ParserRegistry parserRegistry;

   CacheResource(BlockingManager blockingManager, ParserRegistry parserRegistry, EmbeddedCacheManager cm,
                 BackupManager.Resources params, Path root) {
      super(CACHES, params, blockingManager, root);
      this.cm = cm;
      this.parserRegistry = parserRegistry;
   }

   @Override
   public void prepareAndValidateBackup() {
      InternalCacheRegistry icr = cm.getGlobalComponentRegistry().getComponent(InternalCacheRegistry.class);

      Set<String> caches = wildcard ? cm.getCacheConfigurationNames() : resources;
      for (String cache : caches) {
         Configuration config = cm.getCacheConfiguration(cache);

         if (wildcard) {
            // For wildcard resources, we ignore internal caches, however explicitly requested internal caches are allowed
            if (config == null || config.isTemplate() || icr.isInternalCache(cache) || MEMCACHED_CACHE.equals(cache)) {
               continue;
            }
            resources.add(cache);
         } else if (config == null) {
            throw log.unableToFindResource(type.toString(), cache);
         } else if (config.isTemplate()) {
            throw new CacheException(String.format("Unable to backup %s '%s' as it is a template not a cache", type, cache));
         }
      }
   }

   @Override
   public CompletionStage<Void> backup() {
      AggregateCompletionStage<Void> stages = CompletionStages.aggregateCompletionStage();
      for (String cache : resources)
         stages.dependsOn(createCacheBackup(cache));
      return stages.freeze();
   }

   @Override
   public CompletionStage<Void> restore(ZipFile zip) {
      AggregateCompletionStage<Void> stages = CompletionStages.aggregateCompletionStage();
      for (String cacheName : resources) {
         stages.dependsOn(blockingManager.runBlocking(() -> {
            Path cacheRoot = root.resolve(cacheName);

            // Process .xml
            String configFile = configFile(cacheName);
            String zipPath = cacheRoot.resolve(configFile).toString();
            try (InputStream is = zip.getInputStream(zip.getEntry(zipPath))) {
               ConfigurationBuilderHolder builderHolder = parserRegistry.parse(is, null);
               Configuration cfg = builderHolder.getNamedConfigurationBuilders().get(cacheName).build();
               // GetOrCreate in the event that a default cache is defined. This also allows cache-configurations to be
               // modified prior to a restore if only the backed up data is required
               cm.administration().getOrCreateCache(cacheName, cfg);
            } catch (IOException e) {
               throw new CacheException(e);
            }

            // Process .dat
            String dataFile = dataFile(cacheName);
            String data = cacheRoot.resolve(dataFile).toString();
            ZipEntry zipEntry = zip.getEntry(data);
            if (zipEntry == null)
               return;

            AdvancedCache<Object, Object> cache = cm.getCache(cacheName).getAdvancedCache();
            ComponentRegistry cr = cache.getComponentRegistry();
            CommandsFactory commandsFactory = cr.getCommandsFactory();
            KeyPartitioner keyPartitioner = cr.getComponent(KeyPartitioner.class);
            InvocationHelper invocationHelper = cr.getComponent(InvocationHelper.class);
            StorageConfigurationManager scm = cr.getComponent(StorageConfigurationManager.class);
            PersistenceMarshaller persistenceMarshaller = cr.getPersistenceMarshaller();
            Marshaller userMarshaller = persistenceMarshaller.getUserMarshaller();

            boolean keyMarshalling = MediaType.APPLICATION_OBJECT.equals(scm.getValueStorageMediaType());
            boolean valueMarshalling = MediaType.APPLICATION_OBJECT.equals(scm.getValueStorageMediaType());

            SerializationContextRegistry ctxRegistry = cm.getGlobalComponentRegistry().getComponent(SerializationContextRegistry.class);
            ImmutableSerializationContext serCtx = ctxRegistry.getPersistenceCtx();
            try (InputStream is = zip.getInputStream(zipEntry)) {
               while (is.available() > 0) {
                  CacheBackupEntry entry = readMessageStream(serCtx, CacheBackupEntry.class, is);
                  Object key = keyMarshalling ? unmarshall(entry.key, userMarshaller) : scm.getKeyWrapper().wrap(entry.key);
                  Object value = valueMarshalling ? unmarshall(entry.value, userMarshaller) : scm.getKeyWrapper().wrap(entry.value);
                  Metadata metadata = unmarshall(entry.metadata, persistenceMarshaller);
                  Metadata internalMetadataImpl = new InternalMetadataImpl(metadata, entry.created, entry.lastUsed);

                  PutKeyValueCommand cmd = commandsFactory.buildPutKeyValueCommand(key, value, keyPartitioner.getSegment(key),
                        internalMetadataImpl, FlagBitSets.IGNORE_RETURN_VALUES);
                  cmd.setInternalMetadata(entry.internalMetadata);
                  invocationHelper.invoke(cmd, 1);
               }
            } catch (IOException e) {
               throw new CacheException(e);
            }
         }, "restore-cache-" + cacheName));
      }
      return stages.freeze();
   }

   private CompletionStage<Void> createCacheBackup(String cacheName) {
      return blockingManager.runBlocking(() -> {
         AdvancedCache<?, ?> cache = cm.getCache(cacheName).getAdvancedCache();
         Configuration configuration = cm.getCacheConfiguration(cacheName);

         Path cacheRoot = root.resolve(cacheName);
         // Create the cache backup dir and parents
         mkdirs(cacheRoot);

         // Write configuration file
         String xmlFileName = configFile(cacheName);
         Path xmlPath = cacheRoot.resolve(xmlFileName);
         try (OutputStream os = Files.newOutputStream(xmlPath)) {
            parserRegistry.serialize(os, cacheName, configuration);
         } catch (XMLStreamException | IOException e) {
            throw new CacheException(String.format("Unable to create backup file '%s'", xmlFileName), e);
         }

         ComponentRegistry cr = cache.getComponentRegistry();
         ClusterPublisherManager<Object, Object> clusterPublisherManager = cr.getClusterPublisherManager().running();
         SerializationContextRegistry ctxRegistry = cr.getGlobalComponentRegistry().getComponent(SerializationContextRegistry.class);
         ImmutableSerializationContext serCtx = ctxRegistry.getPersistenceCtx();

         String dataFileName = dataFile(cacheName);
         Path datFile = cacheRoot.resolve(dataFileName);

         int bufferSize = configuration.clustering().stateTransfer().chunkSize();

         // Create the publisher using the BlockingManager to ensure that all entries are subscribed to on a blocking thread
         Publisher<CacheEntry<Object, Object>> p = blockingManager.blockingPublisher(
               clusterPublisherManager.entryPublisher(null, null, null, true,
                     DeliveryGuarantee.EXACTLY_ONCE, bufferSize, PublisherTransformers.identity())
         );

         StorageConfigurationManager scm = cr.getComponent(StorageConfigurationManager.class);
         boolean keyMarshalling = MediaType.APPLICATION_OBJECT.equals(scm.getKeyStorageMediaType());
         boolean valueMarshalling = MediaType.APPLICATION_OBJECT.equals(scm.getValueStorageMediaType());
         PersistenceMarshaller persistenceMarshaller = cr.getPersistenceMarshaller();
         Marshaller userMarshaller = persistenceMarshaller.getUserMarshaller();

         Flowable.using(
               () -> Files.newOutputStream(datFile),
               output ->
                     Flowable.fromPublisher(p)
                           .map(e -> {
                              CacheBackupEntry be = new CacheBackupEntry();
                              be.key = keyMarshalling ? marshall(e.getKey(), userMarshaller) : (byte[]) scm.getKeyWrapper().unwrap(e.getKey());
                              be.value = valueMarshalling ? marshall(e.getValue(), userMarshaller) : (byte[]) scm.getValueWrapper().unwrap(e.getKey());
                              be.metadata = marshall(e.getMetadata(), persistenceMarshaller);
                              be.internalMetadata = e.getInternalMetadata();
                              be.created = e.getCreated();
                              be.lastUsed = e.getLastUsed();
                              return be;
                           })
                           .doOnNext(e -> writeMessageStream(e, serCtx, output))
                           .onErrorResumeNext(RxJavaInterop.cacheExceptionWrapper()),
               OutputStream::close)
               .subscribe();
      }, "backup-cache");
   }

   private String configFile(String cache) {
      return String.format("%s.xml", cache);
   }

   private String dataFile(String cache) {
      return String.format("%s.dat", cache);
   }

   private byte[] marshall(Object key, Marshaller marshaller) {
      try {
         return marshaller.objectToByteBuffer(key);
      } catch (IOException | InterruptedException e) {
         throw new MarshallingException(e);
      }
   }

   @SuppressWarnings("unchecked")
   private static <T> T unmarshall(byte[] bytes, Marshaller marshaller) {
      try {
         return (T) marshaller.objectFromByteBuffer(bytes);
      } catch (ClassNotFoundException | IOException e) {
         throw new MarshallingException(e);
      }
   }

   /**
    * ProtoStream entity used to represent individual cache entries.
    */
   @ProtoTypeId(ProtoStreamTypeIds.CACHE_BACKUP_ENTRY)
   public static class CacheBackupEntry {

      @ProtoField(number = 1)
      byte[] key;

      @ProtoField(number = 2)
      byte[] value;

      @ProtoField(number = 3)
      byte[] metadata;

      @ProtoField(number = 4)
      PrivateMetadata internalMetadata;

      @ProtoField(number = 5, defaultValue = "-1")
      long created;

      @ProtoField(number = 6, defaultValue = "-1")
      long lastUsed;
   }
}
