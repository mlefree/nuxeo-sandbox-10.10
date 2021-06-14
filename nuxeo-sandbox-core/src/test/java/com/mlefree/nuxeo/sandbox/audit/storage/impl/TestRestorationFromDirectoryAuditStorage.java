
package com.mlefree.nuxeo.sandbox.audit.storage.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.nuxeo.directory.test.DirectoryFeature;
import org.nuxeo.ecm.core.query.sql.model.Predicates;
import org.nuxeo.ecm.core.query.sql.model.QueryBuilder;
import org.nuxeo.ecm.directory.Session;
import org.nuxeo.ecm.platform.audit.AuditFeature;
import org.nuxeo.ecm.platform.audit.api.AuditQueryBuilder;
import org.nuxeo.ecm.platform.audit.api.LogEntry;
import org.nuxeo.ecm.platform.audit.service.AuditBackend;
import org.nuxeo.ecm.platform.audit.service.NXAuditEventsService;
import org.nuxeo.runtime.api.Framework;
import org.nuxeo.runtime.test.runner.Deploy;
import org.nuxeo.runtime.test.runner.Features;
import org.nuxeo.runtime.test.runner.FeaturesRunner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.nuxeo.ecm.platform.audit.api.BuiltinLogEntryData.LOG_EVENT_ID;
import static org.nuxeo.ecm.platform.audit.api.BuiltinLogEntryData.LOG_ID;

@RunWith(FeaturesRunner.class)
@Features({ DirectoryFeature.class, AuditFeature.class })
// @Features({ DirectoryFeature.class, MongoDBAuditFeature.class })
@Deploy("com.mlefree.nuxeo.sandbox.nuxeo-sandbox-core")
public class TestRestorationFromDirectoryAuditStorage {

    @Test
    public void testRestoration() throws Exception {

        String testEventId = "testEventId";
        int nbEntries = 50;

        QueryBuilder queryBuilder = new AuditQueryBuilder().predicate(Predicates.eq(LOG_EVENT_ID, testEventId));

        ObjectMapper mapper = new ObjectMapper();
        List<String> jsonEntries = new ArrayList<>();
        for (long i = 1; i <= nbEntries; i++) {
            ObjectNode logEntryJson = mapper.createObjectNode();
            logEntryJson.put(LOG_ID, i);
            logEntryJson.put(LOG_EVENT_ID, testEventId);
            jsonEntries.add(mapper.writeValueAsString(logEntryJson));
        }

        NXAuditEventsService audit = (NXAuditEventsService) Framework.getRuntime()
                                                                     .getComponent(NXAuditEventsService.NAME);
        DirectoryAuditStorage storage = (DirectoryAuditStorage) audit.getAuditStorage(DirectoryAuditStorage.NAME);

        storage.append(jsonEntries);
        try (Session storageSession = storage.getAuditDirectory().getSession()) {
            assertEquals(nbEntries + 6, storageSession.query(Collections.emptyMap()).size());
        }

        AuditBackend backend = audit.getBackend();
        assertEquals(0, backend.queryLogs(queryBuilder).size());

        backend.restore(storage, 500, 10);

        List<LogEntry> logs = backend.queryLogs(queryBuilder);
        assertEquals(nbEntries, logs.size());
    }

}
