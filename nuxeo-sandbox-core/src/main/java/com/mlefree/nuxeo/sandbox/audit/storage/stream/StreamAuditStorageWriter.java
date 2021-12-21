package com.mlefree.nuxeo.sandbox.audit.storage.stream;

import com.mlefree.nuxeo.sandbox.audit.storage.impl.DirectoryAuditStorage;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONException;
import org.json.JSONObject;
import org.nuxeo.ecm.platform.audit.api.LogEntry;
import org.nuxeo.ecm.platform.audit.service.AuditBackend;
import org.nuxeo.ecm.platform.audit.service.NXAuditEventsService;
import org.nuxeo.ecm.platform.audit.service.extension.AuditBackendDescriptor;
import org.nuxeo.elasticsearch.audit.ESAuditBackend;
import org.nuxeo.lib.stream.computation.AbstractComputation;
import org.nuxeo.lib.stream.computation.ComputationContext;
import org.nuxeo.lib.stream.computation.Record;
import org.nuxeo.lib.stream.computation.Topology;
import org.nuxeo.runtime.api.Framework;
import org.nuxeo.runtime.stream.StreamProcessorTopology;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.nuxeo.ecm.platform.audit.api.BuiltinLogEntryData.LOG_CATEGORY;
import static org.nuxeo.ecm.platform.audit.api.BuiltinLogEntryData.LOG_DOC_UUID;
import static org.nuxeo.ecm.platform.audit.api.BuiltinLogEntryData.LOG_EVENT_DATE;
import static org.nuxeo.ecm.platform.audit.api.BuiltinLogEntryData.LOG_ID;
import static org.nuxeo.ecm.platform.audit.api.BuiltinLogEntryData.LOG_REPOSITORY_ID;
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

        protected final List<LogEntry> logEntries;

        public AuditStorageLogWriterComputation(String name, int batchSize, int batchThresholdMs) {
            super(name, 1, 0);
            this.batchSize = batchSize;
            this.batchThresholdMs = batchThresholdMs;
            jsonEntries = new ArrayList<>(batchSize);
            logEntries = new ArrayList<>(batchSize);
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
            //writeLogEntriesToAudit(context);
            context.setTimer("batch", System.currentTimeMillis() + batchThresholdMs);
        }

        //@Override
        public void processRecordLogEntries(ComputationContext context, String inputStreamName, Record record) {
            String recordAsString = new String(record.getData(), UTF_8);

            NXAuditEventsService auditService = (NXAuditEventsService) Framework.getRuntime().getComponent(
                    NXAuditEventsService.NAME);

            AuditBackend legacyBackend = auditService.getBackend();
            Map<String, Object> params = new HashMap(4);
            try {
                JSONObject entry = new JSONObject(recordAsString);
                params.put(LOG_REPOSITORY_ID, entry.getString(LOG_REPOSITORY_ID));
                params.put(LOG_CATEGORY, entry.getString(LOG_CATEGORY));
                params.put(LOG_DOC_UUID, entry.getString(LOG_DOC_UUID));
                //params.put(LOG_EVENT_DATE, entry.getString(LOG_EVENT_DATE));
                // DateFormat df = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss z", Locale.US);
                // df.setTimeZone(TimeZone.getDefault());
                // params = new Object[] { df.format(new Date(update.toMillis())) };
                // params.put(LOG_EVENT_DATE, df.parse(entry.getString(LOG_EVENT_DATE)));
                DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
                df.setTimeZone(TimeZone.getDefault());
                String eventDate = entry.getString(LOG_EVENT_DATE);
                Date eventDate1 = df.parse(eventDate);
                // String eventDate2 = //new SimpleDateFormat("'TIMESTAMP' ''yyyy-MM-dd HH:mm:ss.SSS''").format(eventDate);
                params.put(LOG_EVENT_DATE, eventDate1);
            } catch (Exception e) {
                e.printStackTrace();
            }

            String query = String.format("FROM LogEntry log WHERE log.repositoryId = :repositoryId AND log.category = :category AND log.docUUID = :docUUID AND log.eventDate = :eventDate ORDER BY log.id DESC");
            List<LogEntry> entries = (List<LogEntry>) legacyBackend.nativeQuery(query, params, 1, 1);

            logEntries.addAll(entries);
            if (logEntries.size() >= batchSize) {
                writeLogEntriesToAudit(context);
            }
        }

        @Override
        public void processRecord(ComputationContext context, String inputStreamName, Record record) {
            String recordAsString = new String(record.getData(), UTF_8);

            try {
                JSONObject entry = new JSONObject(recordAsString);
                String entryId = entry.getString(LOG_ID);
                entry.put(LOG_ID, context.getLastOffset().offset());
                recordAsString = entry.toString();
                jsonEntries.add(recordAsString);
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

        protected void writeJsonEntriesToAuditOld(ComputationContext context) {
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

        protected void writeJsonEntriesToAudit(ComputationContext context) {
            if (jsonEntries.isEmpty()) {
                return;
            }

            if (log.isDebugEnabled()) {
                log.debug(String.format("Writing %d log entries to the directory audit storage %s.", jsonEntries.size(),
                        DirectoryAuditStorage.NAME));
            }

            NXAuditEventsService auditService = (NXAuditEventsService) Framework.getRuntime()
                    .getComponent(NXAuditEventsService.NAME);
            AuditBackendDescriptor config = new AuditBackendDescriptor();
            ESAuditBackend targetBackend = new ESAuditBackend(auditService, config);

            // needs audit.elasticsearch.migration=true
            // Fail : targetBackend.onApplicationStarted();

            targetBackend.append(jsonEntries);

            jsonEntries.clear();
            context.askForCheckpoint();
        }

        protected void writeLogEntriesToAudit(ComputationContext context) {
            if (logEntries.isEmpty()) {
                return;
            }

            NXAuditEventsService auditService = (NXAuditEventsService) Framework.getRuntime().getComponent(
                    NXAuditEventsService.NAME);
            AuditBackendDescriptor config = new AuditBackendDescriptor();

            // needs audit.elasticsearch.migration=true
            AuditBackend targetBackend = new ESAuditBackend(auditService, config);
            targetBackend.onApplicationStarted();

            targetBackend.addLogEntries(logEntries);
            logEntries.clear();
            context.askForCheckpoint();
        }
    }

    protected int getOptionAsInteger(Map<String, String> options, String option, int defaultValue) {
        String value = options.get(option);
        return value == null ? defaultValue : Integer.parseInt(value);
    }

}
