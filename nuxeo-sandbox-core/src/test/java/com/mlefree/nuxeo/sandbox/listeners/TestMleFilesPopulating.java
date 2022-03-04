package com.mlefree.nuxeo.sandbox.listeners;

import static com.mlefree.nuxeo.sandbox.MleRepositoryInit.ASSET_PATH;
import static com.mlefree.nuxeo.sandbox.constants.Constants.BINARY_TEXT;
import static com.mlefree.nuxeo.sandbox.constants.Constants.MLE_FILES_FULLTEXT;
import static com.mlefree.nuxeo.sandbox.constants.Constants.MLE_FILES_SIZE;
import static com.mlefree.nuxeo.sandbox.constants.StudioConstant.MLE_FILES_SCHEMA_FILES_PROPERTY;
import static com.mlefree.nuxeo.sandbox.constants.StudioConstant.MLE_FILES_SCHEMA_PAGE_COUNT_PROPERTY;
import static com.mlefree.nuxeo.sandbox.constants.StudioConstant.MLE_FILE_DOC_TYPE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.nuxeo.ecm.automation.test.AutomationFeature;
import org.nuxeo.ecm.collections.core.test.CollectionFeature;
import org.nuxeo.ecm.core.api.CloseableCoreSession;
import org.nuxeo.ecm.core.api.CoreInstance;
import org.nuxeo.ecm.core.api.CoreSession;
import org.nuxeo.ecm.core.api.DocumentModel;
import org.nuxeo.ecm.core.api.DocumentRef;
import org.nuxeo.ecm.core.api.impl.blob.FileBlob;
import org.nuxeo.ecm.core.event.EventService;
import org.nuxeo.ecm.core.work.api.WorkManager;
import org.nuxeo.runtime.api.Framework;
import org.nuxeo.runtime.test.runner.Deploy;
import org.nuxeo.runtime.test.runner.Features;
import org.nuxeo.runtime.test.runner.FeaturesRunner;
import org.nuxeo.runtime.transaction.TransactionHelper;

import com.mlefree.nuxeo.sandbox.MleFeature;

@RunWith(FeaturesRunner.class)
@Features({ MleFeature.class, AutomationFeature.class, CollectionFeature.class })
@Deploy("com.mlefree.nuxeo.sandbox.test:test-override-indexing-contrib.xml")
public class TestMleFilesPopulating {

    @Inject
    protected CoreSession session;

    @Inject
    protected EventService s;

    protected void waitForAsyncIndexing() throws Exception {
        TransactionHelper.commitOrRollbackTransaction();
        Framework.getService(WorkManager.class).awaitCompletion(20, TimeUnit.SECONDS);
        Framework.getService(WorkManager.class).awaitCompletion(1, TimeUnit.MINUTES);
        TransactionHelper.startTransaction();
    }

    @Test
    public void shouldNotPopulateFileWithoutBinaryFullText() throws Exception {
        DocumentRef ref;
        try (CloseableCoreSession adminSession = CoreInstance.openCoreSession(session.getRepositoryName(),
                "Administrator")) {

            DocumentModel mleFileWithoutBinary = session.createDocumentModel("/", "mleFileWithoutBinary",
                    MLE_FILE_DOC_TYPE);
            mleFileWithoutBinary.setPropertyValue("dc:title", "mleFileWithoutBinary");
            mleFileWithoutBinary.setPropertyValue("dc:description", "test folder");
            mleFileWithoutBinary = adminSession.createDocument(mleFileWithoutBinary);
            adminSession.saveDocument(mleFileWithoutBinary);
            adminSession.save();
            ref = mleFileWithoutBinary.getRef();
            adminSession.saveDocument(mleFileWithoutBinary);
            adminSession.save();
        }

        waitForAsyncIndexing();

        try (CloseableCoreSession adminSession = CoreInstance.openCoreSession(session.getRepositoryName(),
                "Administrator")) {
            DocumentModel doc = adminSession.getDocument(ref);
            assertNull(doc.getPropertyValue(MLE_FILES_SCHEMA_PAGE_COUNT_PROPERTY));
            // assertEquals(0, ((List<?>) doc.getPropertyValue(MLE_FILES_SCHEMA_FILES_PROPERTY)).size());
        }
    }

    @Test
    public void shouldPopulateFileWithBinaryFullText() throws Exception {
        DocumentRef ref;
        try (CloseableCoreSession adminSession = CoreInstance.openCoreSession(session.getRepositoryName(),
                "Administrator")) {

            DocumentModel mleFileWithBinary = adminSession.createDocumentModel("/", "mleFileWithBinary",
                    MLE_FILE_DOC_TYPE);
            mleFileWithBinary.setPropertyValue("dc:title", "mleFileWithBinary");
            mleFileWithBinary.setPropertyValue("dc:description", "test folder");

            List<Map<String, Serializable>> files = new ArrayList<>();
            Map<String, Serializable> file = new HashMap<>();
            file.put("file",
                    new FileBlob(new File(
                            Objects.requireNonNull(TestMleFilesPopulating.class.getResource(ASSET_PATH)).getPath()),
                            "application/pdf"));
            files.add(file);
            mleFileWithBinary.setPropertyValue("files:files", (Serializable) files);

            mleFileWithBinary = adminSession.createDocument(mleFileWithBinary);
            ref = mleFileWithBinary.getRef();
            adminSession.saveDocument(mleFileWithBinary);
            adminSession.save();
        }

        waitForAsyncIndexing();

        try (CloseableCoreSession adminSession = CoreInstance.openCoreSession(session.getRepositoryName(),
                "Administrator")) {
            DocumentModel doc = adminSession.getDocument(ref);
            assertEquals(1, Math.toIntExact((Long) doc.getPropertyValue(MLE_FILES_SCHEMA_PAGE_COUNT_PROPERTY)));
            assertEquals(1, ((List<?>) doc.getPropertyValue(MLE_FILES_SCHEMA_FILES_PROPERTY)).size());
            Map<String, Serializable> mleFile = (Map<String, Serializable>) ((List<?>) doc.getPropertyValue(
                    MLE_FILES_SCHEMA_FILES_PROPERTY)).get(0);
            assertEquals(" exp", doc.getBinaryFulltext().get(BINARY_TEXT));
            assertEquals(2900,((String) mleFile.get(MLE_FILES_FULLTEXT)).length());
            assertTrue(((String) mleFile.get(MLE_FILES_FULLTEXT)).contains("explicabo."));
            assertEquals(29401, Math.toIntExact((Long) mleFile.get(MLE_FILES_SIZE)));
        }
    }
}
