
package com.mlefree.nuxeo.sandbox.audit.storage.stream;

import com.mlefree.nuxeo.sandbox.audit.storage.impl.DirectoryAuditStorage;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.nuxeo.ecm.core.api.ScrollResult;
import org.nuxeo.ecm.core.query.sql.model.QueryBuilder;
import org.nuxeo.ecm.core.test.DefaultRepositoryInit;
import org.nuxeo.ecm.core.test.annotations.Granularity;
import org.nuxeo.ecm.core.test.annotations.RepositoryConfig;
import org.nuxeo.ecm.platform.audit.AuditFeature;
import org.nuxeo.ecm.platform.audit.api.AuditQueryBuilder;
import org.nuxeo.ecm.platform.audit.service.NXAuditEventsService;
import org.nuxeo.runtime.api.Framework;
import org.nuxeo.runtime.test.runner.Deploy;
import org.nuxeo.runtime.test.runner.Features;
import org.nuxeo.runtime.test.runner.FeaturesRunner;

import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

@RunWith(FeaturesRunner.class)
@Features(AuditFeature.class)
@RepositoryConfig(init = DefaultRepositoryInit.class, cleanup = Granularity.METHOD)
@Deploy("org.nuxeo.runtime.stream")
@Deploy("com.mlefree.nuxeo.sandbox.nuxeo-sandbox-core")
//@Deploy("com.mlefree.nuxeo.sandbox.nuxeo-sandbox-core:OSGI-INF/test-stream-audit-storage-contrib.xml")
public class TestStreamAuditStorageWriter {

    @Test
    public void testWriteJsonEntriesToAudit() {
        NXAuditEventsService audit = (NXAuditEventsService) Framework.getRuntime()
                                                                     .getComponent(NXAuditEventsService.NAME);
        DirectoryAuditStorage storage = (DirectoryAuditStorage) audit.getAuditStorage(DirectoryAuditStorage.NAME);

        QueryBuilder queryBuilder = new AuditQueryBuilder();
        ScrollResult<String> scrollResult = storage.scroll(queryBuilder, 20, 1);
        assertNotNull(scrollResult.getScrollId());
        List<String> results = scrollResult.getResults();
        assertFalse(results.isEmpty());
    }

}
