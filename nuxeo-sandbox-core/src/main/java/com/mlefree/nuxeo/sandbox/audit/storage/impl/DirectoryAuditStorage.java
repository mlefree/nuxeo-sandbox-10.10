package com.mlefree.nuxeo.sandbox.audit.storage.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nuxeo.ecm.core.api.CursorService;
import org.nuxeo.ecm.core.api.DocumentModelList;
import org.nuxeo.ecm.core.api.NuxeoException;
import org.nuxeo.ecm.core.api.ScrollResult;
import org.nuxeo.ecm.core.query.sql.model.MultiExpression;
import org.nuxeo.ecm.core.query.sql.model.Operator;
import org.nuxeo.ecm.core.query.sql.model.Predicate;
import org.nuxeo.ecm.core.query.sql.model.QueryBuilder;
import org.nuxeo.ecm.core.query.sql.model.StringLiteral;
import org.nuxeo.ecm.directory.Directory;
import org.nuxeo.ecm.directory.Session;
import org.nuxeo.ecm.directory.api.DirectoryService;
import org.nuxeo.ecm.platform.audit.api.AuditStorage;
import org.nuxeo.runtime.api.Framework;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class DirectoryAuditStorage implements AuditStorage { // extends AbstractAuditBackend implements AuditBackend,

    private static final Log log = LogFactory.getLog(DirectoryAuditStorage.class);

    public static final String NAME = "MLEDirectoryAuditStorage";

    public static final String DIRECTORY_NAME = "mleAuditStorage";

    public static final String ID_COLUMN = "id";

    public static final String JSON_COLUMN = "entry";

    protected CursorService<Iterator<String>, String, String> cursorService = new CursorService<>(Function.identity());

    protected Directory getAuditDirectory() {
        return Framework.getService(DirectoryService.class).getDirectory(DIRECTORY_NAME);
    }

    /**
     * Insert entries as Json in the Audit directory.
     */
    @Override
    public void append(List<String> jsonEntries) {
        try (Session session = getAuditDirectory().getSession()) {
            for (String jsonEntry : jsonEntries) {
                Framework.doPrivileged(() -> session.createEntry(Collections.singletonMap(JSON_COLUMN, jsonEntry)));
            }
        }
    }

    /**
     * Scroll log entries in the Audit directory, given a scroll Id.
     */
    @Override
    public ScrollResult<String> scroll(String scrollId) {
        return cursorService.scroll(scrollId);
    }

    /**
     * Scroll log entries in the Audit directory, given an audit query builder.
     */
    @Override
    public ScrollResult<String> scroll(QueryBuilder queryBuilder, int batchSize, int keepAlive) {
        cursorService.checkForTimedOutScroll();
        List<String> logEntries = queryLogsAsString(queryBuilder);
        String scrollId = cursorService.registerCursor(logEntries.iterator(), batchSize, keepAlive);
        return scroll(scrollId);
    }

    /**
     * Query log entries in the Audit directory, given an audit query builder. Does not support literals other than
     * StringLiteral: see {@link Session#query(Map, Set, Map, boolean, int, int)}.
     */
    protected List<String> queryLogsAsString(QueryBuilder queryBuilder) {
        // Get the predicates filter map from the query builder.
        Map<String, Serializable> filter = new HashMap<>();
        Set<String> fulltext = null;
        MultiExpression multiExpr = queryBuilder.predicate();
        if (multiExpr.operator != Operator.AND) {
            throw new NuxeoException("Operator not supported: " + multiExpr.operator);
        }
        List<Predicate> predicateList = multiExpr.predicates;
        for (Predicate p : predicateList) {
            String rvalue;
            if (p.rvalue instanceof StringLiteral) {
                rvalue = ((StringLiteral) p.rvalue).asString();
            } else {
                rvalue = p.rvalue.toString();
                log.warn(String.format(
                        "Scrolling audit logs with a query builder containing non-string literals is not supported: %s.",
                        rvalue));
            }
            filter.put(p.lvalue.toString(), rvalue);

            if (fulltext == null && Arrays.asList(Operator.LIKE, Operator.ILIKE).contains(p.operator)) {
                fulltext = Collections.singleton(JSON_COLUMN);
            }
        }

        // Get the orderBy map from the query builder.
        Map<String, String> orderBy = queryBuilder.orders()
                                                  .stream()
                                                  .collect(Collectors.toMap(o -> o.reference.name,
                                                          o -> o.isDescending ? "desc" : "asc"));

        // Get the limit and offset from the query builder.
        int limit = (int) queryBuilder.limit();
        int offset = (int) queryBuilder.offset();

        // Query the Json Entries via the directory session.
        Directory directory = getAuditDirectory();
        try (Session session = directory.getSession()) {
            DocumentModelList jsonEntriesDocs = session.query(filter, fulltext, orderBy, false, limit, offset);

            // Build a list of Json Entries from the document model list.
            String auditPropertyName = directory.getSchema() + ":" + JSON_COLUMN;
            return jsonEntriesDocs.stream()
                                  .map(doc -> doc.getPropertyValue(auditPropertyName))
                                  .map(String::valueOf)
                                  .collect(Collectors.toList());
        }
    }

/*
    @Override
    public void append(List<String> jsonEntries) {
        BulkRequest bulkRequest = new BulkRequest();
        for (String json : jsonEntries) {
            try {
                String entryId = new JSONObject(json).getString(LOG_ID);
                if (StringUtils.isBlank(entryId)) {
                    throw new NuxeoException("A json entry has an empty id. entry=" + json);
                }
                IndexRequest request = new IndexRequest(getESIndexName(), ElasticSearchConstants.ENTRY_TYPE, entryId);
                request.source(json, XContentType.JSON);
                bulkRequest.add(request);
            } catch (JSONException e) {
                throw new NuxeoException("Unable to deserialize json entry=" + json, e);
            }
        }
        esClient.bulk(bulkRequest);
    }



    @Override
    public ExtendedInfo newExtendedInfo(Serializable serializable) {
        return null;
    }

    @Override
    public void addLogEntries(List<LogEntry> list) {

    }

    @Override
    public int getApplicationStartedOrder() {
        return 0;
    }

    @Override
    public void onApplicationStarted() {

    }

    @Override
    public long syncLogCreationEntries(String s, String s1, Boolean aBoolean) {
        return 0;
    }

    @Override
    public Long getEventsCount(String s) {
        return null;
    }

    @Override
    public LogEntry getLogEntryByID(long l) {
        return null;
    }

    @Override
    public List<LogEntry> queryLogs(QueryBuilder queryBuilder) {
        return null;
    }

    @Override
    public List<?> nativeQuery(String s, Map<String, Object> map, int i, int i1) {
        return null;
    }

 */
}
