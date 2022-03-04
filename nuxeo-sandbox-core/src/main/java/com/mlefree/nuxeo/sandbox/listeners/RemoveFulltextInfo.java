package com.mlefree.nuxeo.sandbox.listeners;

import static com.mlefree.nuxeo.sandbox.constants.Constants.BINARY_TEXT;
import static com.mlefree.nuxeo.sandbox.constants.Constants.FULLTEXT_BINARY;
import static com.mlefree.nuxeo.sandbox.constants.Constants.KEY_FULLTEXT;
import static com.mlefree.nuxeo.sandbox.constants.Constants.KEY_FULLTEXT_BINARY;
import static com.mlefree.nuxeo.sandbox.constants.Constants.KEY_FULLTEXT_SIMPLE;
import static com.mlefree.nuxeo.sandbox.listeners.MleFilesPopulating.MLE_FILES_POPULATING;
import static java.lang.Boolean.TRUE;
import static org.nuxeo.ecm.core.storage.BaseDocument.FULLTEXT_BINARYTEXT_PROP;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nuxeo.ecm.core.api.CoreSession;
import org.nuxeo.ecm.core.api.DocumentModel;
import org.nuxeo.ecm.core.event.Event;
import org.nuxeo.ecm.core.event.EventContext;
import org.nuxeo.ecm.core.event.EventListener;
import org.nuxeo.ecm.core.event.impl.DocumentEventContext;

public class RemoveFulltextInfo implements EventListener {

    private static final Log log = LogFactory.getLog(RemoveFulltextInfo.class);

    @Override
    public void handleEvent(Event event) {
        EventContext ctx = event.getContext();
        if (!(ctx instanceof DocumentEventContext)) {
            return;
        }

        DocumentEventContext docCtx = (DocumentEventContext) ctx;
        DocumentModel doc = docCtx.getSourceDocument();
        CoreSession session = docCtx.getCoreSession();

        if (doc == null) {
            return;
        }

        /*
        logEventFullText(event, doc);
        logEventProp(event, doc, BINARY_TEXT);
        logEventProp(event, doc, KEY_FULLTEXT_SIMPLE);
        logEventProp(event, doc, KEY_FULLTEXT_BINARY);
        logEventProp(event, doc, KEY_FULLTEXT);
        logEventProp(event, doc, FULLTEXT_BINARYTEXT_PROP);
        logEventSystemProp(event, doc, BINARY_TEXT);
        logEventSystemProp(event, doc, FULLTEXT_BINARY);
        logEventSystemProp(event, doc, "fulltextSimple");
         */
    }

    protected void logEventFullText(Event event, DocumentModel doc) {
        try {
            log.warn("#f#" + event.getName() + " > " + doc.getTitle() + " > : "
                    + doc.getBinaryFulltext() // .get(BINARY_TEXT)
            // + doc.getPropertyValue(prop)
            );
        } catch (Exception ignored) {
            log.error("#f#" + event.getName() + " > " + doc.getTitle() + " error with FT");
        }
    }

    protected void logEventProp(Event event, DocumentModel doc, String prop) {
        try {
            log.warn("#p#" + event.getName() + " > " + doc.getTitle() + " > " + prop + " : "
            // + doc.getBinaryFulltext().get(BINARY_TEXT)
                    + doc.getPropertyValue(prop));
        } catch (Exception ignored) {
            log.error("#p#" + event.getName() + " > " + doc.getTitle() + " error with " + prop);
        }
    }

    protected void logEventSystemProp(Event event, DocumentModel doc, String prop) {
        try {
            log.warn("#s#" + event.getName() + " > " + doc.getTitle() + " > " + prop + " : "
                    + doc.getSystemProp(prop, String.class));
        } catch (Exception ignored) {
            log.error("#s#" + event.getName() + " > " + doc.getTitle() + " error with " + prop);
        }
    }
}
