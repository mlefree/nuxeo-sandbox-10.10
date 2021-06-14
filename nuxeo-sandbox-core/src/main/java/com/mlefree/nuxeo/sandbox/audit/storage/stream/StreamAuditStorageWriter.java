package com.mlefree.nuxeo.sandbox.audit.storage.stream;

import com.mlefree.nuxeo.sandbox.audit.storage.impl.DirectoryAuditStorage;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Indexes;
import com.mongodb.util.JSON;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONException;
import org.json.JSONObject;
import org.nuxeo.ecm.core.api.CursorService;
import org.nuxeo.ecm.platform.audit.service.NXAuditEventsService;
import org.nuxeo.lib.stream.computation.AbstractComputation;
import org.nuxeo.lib.stream.computation.ComputationContext;
import org.nuxeo.lib.stream.computation.Record;
import org.nuxeo.lib.stream.computation.Topology;
import org.nuxeo.runtime.api.Framework;
import org.nuxeo.runtime.stream.StreamProcessorTopology;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.nuxeo.ecm.platform.audit.api.BuiltinLogEntryData.LOG_DOC_PATH;
import static org.nuxeo.ecm.platform.audit.api.BuiltinLogEntryData.LOG_DOC_UUID;
import static org.nuxeo.ecm.platform.audit.api.BuiltinLogEntryData.LOG_EVENT_DATE;
import static org.nuxeo.ecm.platform.audit.api.BuiltinLogEntryData.LOG_EVENT_ID;
import static org.nuxeo.ecm.platform.audit.api.BuiltinLogEntryData.LOG_ID;
import static org.nuxeo.ecm.platform.audit.listener.StreamAuditEventListener.STREAM_NAME;

/**
 * Computation that consumes a stream of Json log entries and write them to the Directory Audit Storage.
 */
public class StreamAuditStorageWriter implements StreamProcessorTopology {
    private static final Log log = LogFactory.getLog(StreamAuditStorageWriter.class);

    public static final String COMPUTATION_NAME = "MLEAuditStorageLogWriter";

    public static final String BATCH_SIZE_OPT = "batchSize";

    public static final String BATCH_THRESHOLD_MS_OPT = "batchThresholdMs";

    public static final int DEFAULT_BATCH_SIZE = 10;

    public static final int DEFAULT_BATCH_THRESHOLD_MS = 200;

    @Override
    public Topology getTopology(Map<String, String> options) {
        int batchSize = getOptionAsInteger(options, BATCH_SIZE_OPT, DEFAULT_BATCH_SIZE);
        int batchThresholdMs = getOptionAsInteger(options, BATCH_THRESHOLD_MS_OPT, DEFAULT_BATCH_THRESHOLD_MS);
        return Topology.builder()
                       .addComputation(() -> new AuditStorageLogWriterComputation(COMPUTATION_NAME, batchSize,
                               batchThresholdMs), Collections.singletonList("i1:" + STREAM_NAME))
                       .build();
    }

    public class AuditStorageLogWriterComputation extends AbstractComputation {
        protected final int batchSize;

        protected final int batchThresholdMs;

        protected final List<String> jsonEntries;

        public AuditStorageLogWriterComputation(String name, int batchSize, int batchThresholdMs) {
            super(name, 1, 0);
            this.batchSize = batchSize;
            this.batchThresholdMs = batchThresholdMs;
            jsonEntries = new ArrayList<>(batchSize);
        }

        @Override
        public void init(ComputationContext context) {
            log.debug(String.format("Starting computation: %s reading on: %s, batch size: %d, threshold: %dms",
                    COMPUTATION_NAME, STREAM_NAME, batchSize, batchThresholdMs));
            context.setTimer("batch", System.currentTimeMillis() + batchThresholdMs);
        }

        @Override
        public void processTimer(ComputationContext context, String key, long timestamp) {
            writeJsonEntriesToAudit(context);
            context.setTimer("batch", System.currentTimeMillis() + batchThresholdMs);
        }

        @Override
        public void processRecord(ComputationContext context, String inputStreamName, Record record) {
            String recordAsString = new String(record.getData(), UTF_8);

            try {

                JSONObject entry = new JSONObject(recordAsString);
                String entryId = entry.getString(LOG_ID);
                entry.put(LOG_ID, context.getLastOffset().offset());
                recordAsString = entry.toString();

//                MongoDatabase database = mongoService.getDatabase(AUDIT_DATABASE_ID);
//                collection = database.getCollection(collName);
//                collection.createIndex(Indexes.ascending(LOG_DOC_UUID)); // query by doc id
//                collection.createIndex(Indexes.ascending(LOG_EVENT_DATE)); // query by date range
//                collection.createIndex(Indexes.ascending(LOG_EVENT_ID)); // query by type of event
//                collection.createIndex(Indexes.ascending(LOG_DOC_PATH)); // query by path
//                cursorService = new CursorService<>(doc -> {
//                    Object id = doc.remove(MONGODB_ID);
//                    if (id != null) {
//                        doc.put(LOG_ID, id);
//                    }
//                    return JSON.serialize(doc);
//                });

                jsonEntries.add(recordAsString );
                if (jsonEntries.size() >= batchSize) {
                    writeJsonEntriesToAudit(context);
                }
            } catch (JSONException e) {
                e.printStackTrace();
            }


        }

        @Override
        public void destroy() {
            log.debug(String.format("Destroy computation: %s, pending entries: %d", COMPUTATION_NAME,
                    jsonEntries.size()));
        }

        /**
         * Store JSON entries in the Directory Audit Storage
         */
        protected void writeJsonEntriesToAudit(ComputationContext context) {
            if (jsonEntries.isEmpty()) {
                return;
            }
            if (log.isDebugEnabled()) {
                log.debug(String.format("Writing %d log entries to the directory audit storage %s.", jsonEntries.size(),
                        DirectoryAuditStorage.NAME));
            }
            NXAuditEventsService audit = (NXAuditEventsService) Framework.getRuntime()
                                                                         .getComponent(NXAuditEventsService.NAME);
            DirectoryAuditStorage storage = (DirectoryAuditStorage) audit.getAuditStorage(DirectoryAuditStorage.NAME);
            storage.append(jsonEntries);
            jsonEntries.clear();
            context.askForCheckpoint();
        }
    }

    protected int getOptionAsInteger(Map<String, String> options, String option, int defaultValue) {
        String value = options.get(option);
        return value == null ? defaultValue : Integer.parseInt(value);
    }

}
