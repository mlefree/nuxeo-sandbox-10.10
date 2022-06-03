package com.mlefree.nuxeo.sandbox.seqgen;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gte;
import static com.mongodb.client.model.Filters.not;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.nuxeo.ecm.core.api.NuxeoException;
import org.nuxeo.runtime.mongodb.MongoDBSerializationHelper;
import org.nuxeo.ecm.core.mongodb.seqgen.MongoDBUIDSequencer;

import com.mongodb.ErrorCategory;
import com.mongodb.MongoWriteException;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.Updates;

public class MleMongoSeq extends MongoDBUIDSequencer {

    private static final Log log = LogFactory.getLog(MleMongoSeq.class);

    public static final String SEQUENCE_VALUE_FIELD = "sequence";
    public static final String MONGODB_ID = "_id";

    @Override
    public void initSequence(String key, long id) {
        Bson filter = and(eq(MONGODB_ID, key), not(gte(SEQUENCE_VALUE_FIELD, id)));
        Document sequence = new Document();
        sequence.put(MONGODB_ID, key);
        sequence.put(SEQUENCE_VALUE_FIELD, id);
        try {
            try {
                getSequencerCollection().replaceOne(filter, sequence, new ReplaceOptions().upsert(true));
            } catch (MongoWriteException e) {
                if (ErrorCategory.fromErrorCode(e.getCode()) != ErrorCategory.DUPLICATE_KEY) {
                    throw e;
                }
                // retry once, as not all server versions do server-side retries on upsert
                getSequencerCollection().replaceOne(filter, sequence, new ReplaceOptions().upsert(true));
            }
        } catch (MongoWriteException e) {
            throw new NuxeoException("Failed to update the sequence '" + key + "' with value " + id, e);
        }
    }

    @Override
    public long getNextLong(String key) {
        return incrementBy(key, 1);
    }

    @Override
    public List<Long> getNextBlock(String key, int blockSize) {
        List<Long> ret = new ArrayList<>(blockSize);
        long last = incrementBy(key, blockSize);
        for (int i = blockSize - 1; i >= 0; i--) {
            ret.add(last - i);
        }
        return ret;
    }

    protected long incrementBy(String key, int value) {
        FindOneAndUpdateOptions options = new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER);
        Bson filter = eq(MONGODB_ID, key);
        Bson update = Updates.inc(SEQUENCE_VALUE_FIELD, Long.valueOf(value));
        Document sequence = getSequencerCollection().findOneAndUpdate(filter, update, options);
        // If sequence is null, we need to create it
        if (sequence == null) {
            try {
                sequence = new Document();
                sequence.put(MONGODB_ID, key);
                sequence.put(SEQUENCE_VALUE_FIELD, Long.valueOf(value));
                getSequencerCollection().insertOne(sequence);
            } catch (MongoWriteException e) {
                // There was a race condition - just re-run getNextLong
                if (log.isTraceEnabled()) {
                    log.trace("There was a race condition during '" + key + "' sequence insertion", e);
                }
                return getNextLong(key);
            }
        }
        return ((Long) MongoDBSerializationHelper.bsonToFieldMap(sequence).get(SEQUENCE_VALUE_FIELD)).longValue();
    }

}
