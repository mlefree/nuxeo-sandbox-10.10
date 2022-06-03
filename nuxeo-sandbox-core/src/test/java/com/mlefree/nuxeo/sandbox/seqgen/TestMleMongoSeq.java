package com.mlefree.nuxeo.sandbox.seqgen;

import static com.mlefree.nuxeo.sandbox.MleFeature.waitForAsyncExec;
import static com.mlefree.nuxeo.sandbox.constants.StudioConstant.MLE_FILES_SCHEMA_PAGE_COUNT_PROPERTY;
import static com.mlefree.nuxeo.sandbox.constants.StudioConstant.MLE_FILE_DOC_TYPE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import javax.inject.Inject;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.nuxeo.ecm.automation.test.AutomationFeature;
import org.nuxeo.ecm.collections.core.test.CollectionFeature;
import org.nuxeo.ecm.core.api.CloseableCoreSession;
import org.nuxeo.ecm.core.api.CoreInstance;
import org.nuxeo.ecm.core.api.CoreSession;
import org.nuxeo.ecm.core.api.DocumentModel;
import org.nuxeo.ecm.core.api.DocumentRef;
import org.nuxeo.ecm.core.uidgen.UIDGeneratorService;
import org.nuxeo.ecm.core.uidgen.UIDSequencer;
import org.nuxeo.runtime.test.runner.Features;
import org.nuxeo.runtime.test.runner.FeaturesRunner;

import com.mlefree.nuxeo.sandbox.MleFeature;

@RunWith(FeaturesRunner.class)
@Features({ MleFeature.class, AutomationFeature.class, CollectionFeature.class })
public class TestMleMongoSeq {

    @Inject
    protected CoreSession session;

    @Inject
    protected UIDGeneratorService service;

    @Test
    public void testRegistration() {
        UIDSequencer seq = service.getSequencer();
        assertNotNull(seq);
        assertTrue(seq.getClass().getName(), seq instanceof MleMongoSeq);
    }

    @Test
    @Ignore
    public void shouldCreateFile() throws Exception {
        DocumentRef ref;
        try (CloseableCoreSession adminSession = CoreInstance.openCoreSession(session.getRepositoryName(),
                "Administrator")) {

            DocumentModel mleFileWithoutBinary = session.createDocumentModel("/", "file",
                    "File");
            mleFileWithoutBinary.setPropertyValue("dc:title", "file");
            mleFileWithoutBinary.setPropertyValue("dc:description", "test folder");
            mleFileWithoutBinary = adminSession.createDocument(mleFileWithoutBinary);
            adminSession.saveDocument(mleFileWithoutBinary);
            adminSession.save();
            ref = mleFileWithoutBinary.getRef();
            adminSession.saveDocument(mleFileWithoutBinary);
            adminSession.save();
        }

        waitForAsyncExec();

        try (CloseableCoreSession adminSession = CoreInstance.openCoreSession(session.getRepositoryName(),
                "Administrator")) {
            DocumentModel doc = adminSession.getDocument(ref);
            assertEquals("???", doc.getPropertyValue("file_schema:mleSeq"));
        }
    }

}
