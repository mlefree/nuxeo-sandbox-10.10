package com.mlefree.nuxeo.sandbox.listeners;

import static com.mlefree.nuxeo.sandbox.constants.Constants.FILES_FILE;
import static com.mlefree.nuxeo.sandbox.constants.Constants.FULLTEXT_BINARY;
import static com.mlefree.nuxeo.sandbox.constants.Constants.MLE_FILES_BINARY;
import static com.mlefree.nuxeo.sandbox.constants.Constants.MLE_FILES_FULLTEXT;
import static com.mlefree.nuxeo.sandbox.constants.Constants.MLE_FILES_SIZE;
import static com.mlefree.nuxeo.sandbox.constants.StudioConstant.MLE_FILES_SCHEMA;
import static com.mlefree.nuxeo.sandbox.constants.StudioConstant.MLE_FILES_SCHEMA_FILES_PROPERTY;
import static com.mlefree.nuxeo.sandbox.constants.StudioConstant.MLE_FILES_SCHEMA_PAGE_COUNT_PROPERTY;
import static java.lang.Boolean.TRUE;
import static org.nuxeo.ecm.collections.api.CollectionConstants.DISABLE_NOTIFICATION_SERVICE;
import static org.nuxeo.ecm.core.api.CoreSession.ALLOW_VERSION_WRITE;
import static org.nuxeo.ecm.core.api.versioning.VersioningService.DISABLE_AUTO_CHECKOUT;
import static org.nuxeo.ecm.core.bulk.action.SetPropertiesAction.PARAM_DISABLE_AUDIT;
import static org.nuxeo.ecm.platform.dublincore.listener.DublinCoreListener.DISABLE_DUBLINCORE_LISTENER;
import static org.nuxeo.ecm.platform.htmlsanitizer.HtmlSanitizerListener.DISABLE_HTMLSANITIZER_LISTENER;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nuxeo.ecm.core.api.Blob;
import org.nuxeo.ecm.core.api.CoreSession;
import org.nuxeo.ecm.core.api.DocumentModel;
import org.nuxeo.ecm.core.api.blobholder.BlobHolder;
import org.nuxeo.ecm.core.api.blobholder.SimpleBlobHolder;
import org.nuxeo.ecm.core.convert.api.ConversionException;
import org.nuxeo.ecm.core.convert.api.ConversionService;
import org.nuxeo.ecm.core.event.Event;
import org.nuxeo.ecm.core.event.EventContext;
import org.nuxeo.ecm.core.event.EventListener;
import org.nuxeo.ecm.core.event.impl.DocumentEventContext;
import org.nuxeo.runtime.api.Framework;

public class MleFilesPopulating implements EventListener {

    public static final String MLE_FILES_POPULATING = "mleFilesPopulating";

    private static final Log log = LogFactory.getLog(MleFilesPopulating.class);

    @Override
    public void handleEvent(Event event) {
        EventContext ctx = event.getContext();
        if (!(ctx instanceof DocumentEventContext)) {
            return;
        }

        DocumentEventContext docCtx = (DocumentEventContext) ctx;
        CoreSession session = docCtx.getCoreSession();
        DocumentModel doc = docCtx.getSourceDocument();

        if (doc == null) {
            return;
        }

        if (!doc.hasSchema(MLE_FILES_SCHEMA) || ((List<?>) doc.getPropertyValue("files:files")).isEmpty()
                || !event.getContext().getProperties().get("systemProperty").equals(FULLTEXT_BINARY)) {
            return;
        }

        List<Map<String, Serializable>> files = (List<Map<String, Serializable>>) doc.getPropertyValue("files:files");
        List<Map<String, Serializable>> mleFiles = new ArrayList<>();
        for (Map<String, Serializable> attachment : files) {
            Blob attBlob = (Blob) attachment.get(FILES_FILE);

            Map<String, Serializable> mleFile = new HashMap<>();
            mleFile.put(MLE_FILES_FULLTEXT, blobToText(attBlob));
            mleFile.put(MLE_FILES_SIZE, attBlob.getLength());
            mleFile.put(MLE_FILES_BINARY, (Serializable) attBlob);
            mleFiles.add(mleFile);
        }

        if (!isIdentical(doc, mleFiles)) {
            doc.setPropertyValue(MLE_FILES_SCHEMA_PAGE_COUNT_PROPERTY, mleFiles.size());
            doc.setPropertyValue(MLE_FILES_SCHEMA_FILES_PROPERTY, (Serializable) mleFiles);
            saveDocument(session, doc);
        }
    }

    private void saveDocument(CoreSession session, DocumentModel doc) {
        doc.putContextData(ALLOW_VERSION_WRITE, TRUE);
        doc.putContextData(DISABLE_AUTO_CHECKOUT, TRUE);
        doc.putContextData(DISABLE_DUBLINCORE_LISTENER, TRUE);
        doc.putContextData(DISABLE_NOTIFICATION_SERVICE, TRUE);
        doc.putContextData(DISABLE_HTMLSANITIZER_LISTENER, TRUE);
        doc.putContextData(PARAM_DISABLE_AUDIT, TRUE);
        session.saveDocument(doc);
    }

    protected String blobToText(Blob blob) {
        try {
            ConversionService conversionService = Framework.getService(ConversionService.class);
            if (conversionService == null) {
                log.debug("No ConversionService available");
                return "";
            }
            BlobHolder blobHolder = conversionService.convert("any2text", new SimpleBlobHolder(blob), null);
            if (blobHolder == null) {
                return "";
            }
            Blob resultBlob = blobHolder.getBlob();
            if (resultBlob == null) {
                return "";
            }
            String string = resultBlob.getString();
            // strip '\0 chars from text
            if (string.indexOf('\0') >= 0) {
                string = string.replace("\0", " ");
            }
            return string;
        } catch (ConversionException | IOException e) {
            String msg = "Could not extract fulltext of file '" + blob.getFilename() + "' : " + e;
            log.warn(msg);
            return "";
        }
    }

    protected boolean isIdentical(DocumentModel doc, List<Map<String, Serializable>> files) {
        try {
            return files.size() == Math.toIntExact((Long) doc.getPropertyValue(MLE_FILES_SCHEMA_PAGE_COUNT_PROPERTY));
        } catch (Exception e) {
            return false;
        }
    }
}
