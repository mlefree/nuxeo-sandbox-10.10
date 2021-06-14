
package com.mlefree.nuxeo.sandbox.operations;

import com.mlefree.nuxeo.sandbox.audit.storage.impl.DirectoryAuditStorage;
import org.nuxeo.ecm.automation.core.Constants;
import org.nuxeo.ecm.automation.core.annotations.Context;
import org.nuxeo.ecm.automation.core.annotations.Operation;
import org.nuxeo.ecm.automation.core.annotations.OperationMethod;
import org.nuxeo.ecm.automation.core.annotations.Param;
import org.nuxeo.ecm.core.api.ScrollResult;
import org.nuxeo.ecm.platform.audit.api.AuditQueryBuilder;
import org.nuxeo.ecm.platform.audit.api.LogEntry;
import org.nuxeo.ecm.platform.audit.service.AuditBackend;
import org.nuxeo.ecm.platform.audit.service.NXAuditEventsService;
import org.nuxeo.runtime.api.Framework;
import org.nuxeo.elasticsearch.api.ElasticSearchAdmin;
import org.nuxeo.elasticsearch.audit.ESAuditBackend;

import java.util.List;

@Operation(id = AuditMleRestore.ID, category = Constants.CAT_SERVICES, label = "Restore log entries", description = "Restore log entries from an audit storage implementation to the audit backend.")
public class AuditMleRestore {

    @Context
    protected AuditBackend auditBackend;

    public static final String ID = "Audit.MLERestore";

    public static final int DEFAULT_BATCH_SIZE = 100;

    public static final int DEFAULT_KEEP_ALIVE = 10;

    @Param(name = "auditStorage")
    protected String auditStorageId;

    @Param(name = "batchSize", required = false)
    protected int batchSize = DEFAULT_BATCH_SIZE;

    @Param(name = "keepAlive", required = false)
    protected int keepAlive = DEFAULT_KEEP_ALIVE;

    @OperationMethod
    public void run() {
        NXAuditEventsService audit = (NXAuditEventsService) Framework.getRuntime()
                                                                     .getComponent(NXAuditEventsService.NAME);
        ESAuditBackend esBackend = (ESAuditBackend) Framework.getService(AuditBackend.class);

        auditBackend.restore(audit.getAuditStorage(auditStorageId), batchSize, keepAlive);

        List<LogEntry> entries = esBackend.queryLogs(new AuditQueryBuilder());
        System.out.println(entries.size());

        DirectoryAuditStorage storage = (DirectoryAuditStorage) audit.getAuditStorage(DirectoryAuditStorage.NAME);
        ScrollResult<String> scrollResult = storage.scroll(new AuditQueryBuilder(),batchSize, keepAlive);
        List<String> results = scrollResult.getResults();

        // esBackend.addLogEntries(results);
        esBackend.append(results);

        entries = esBackend.queryLogs(new AuditQueryBuilder());
        System.out.println(entries.size());
    }

}
