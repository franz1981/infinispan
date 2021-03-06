package org.infinispan.query.impl.externalizers;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.AbstractExternalizer;

public class LuceneTopDocsExternalizer extends AbstractExternalizer<TopDocs> {

   @Override
   public Set<Class<? extends TopDocs>> getTypeClasses() {
      return Collections.singleton(TopDocs.class);
   }

   @Override
   public TopDocs readObject(final ObjectInput input) throws IOException, ClassNotFoundException {
      final long totalHits = UnsignedNumeric.readUnsignedLong(input);
      final int scoreCount = UnsignedNumeric.readUnsignedInt(input);
      final ScoreDoc[] scoreDocs = new ScoreDoc[scoreCount];
      for (int i = 0; i < scoreCount; i++) {
         scoreDocs[i] = (ScoreDoc) input.readObject();
      }
      return new TopDocs(new TotalHits(totalHits, TotalHits.Relation.EQUAL_TO), scoreDocs);
   }

   @Override
   public void writeObject(final ObjectOutput output, final TopDocs topDocs) throws IOException {
      UnsignedNumeric.writeUnsignedLong(output, topDocs.totalHits.value);
      final ScoreDoc[] scoreDocs = topDocs.scoreDocs;
      final int count = scoreDocs.length;
      UnsignedNumeric.writeUnsignedInt(output, count);
      for (ScoreDoc scoreDoc : scoreDocs) {
         output.writeObject(scoreDoc);
      }
   }

   @Override
   public Integer getId() {
      return ExternalizerIds.LUCENE_TOPDOCS;
   }
}
