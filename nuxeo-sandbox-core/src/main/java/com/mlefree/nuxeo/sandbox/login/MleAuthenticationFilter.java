
package com.mlefree.nuxeo.sandbox.login;

import java.io.IOException;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nuxeo.ecm.core.api.NuxeoPrincipal;
import org.nuxeo.ecm.platform.ui.web.auth.CachableUserIdentificationInfo;
import org.nuxeo.ecm.platform.ui.web.auth.NuxeoAuthenticationFilter;

public class MleAuthenticationFilter extends NuxeoAuthenticationFilter {

    private static final Log log = LogFactory.getLog(MleAuthenticationFilter.class);

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {

        HttpServletRequest httpRequest = (HttpServletRequest) request;
        CachableUserIdentificationInfo cachableUserIdent = retrieveIdentityFromCache(httpRequest);

        NuxeoPrincipal currentPrincipal = (NuxeoPrincipal) cachableUserIdent.getPrincipal();
        if (currentPrincipal != null && currentPrincipal.getGroups().contains("test")) {
            this.handleLogout(request, response, cachableUserIdent);
            return;
        }

        super.doFilter(request, response, chain);
    }

}
