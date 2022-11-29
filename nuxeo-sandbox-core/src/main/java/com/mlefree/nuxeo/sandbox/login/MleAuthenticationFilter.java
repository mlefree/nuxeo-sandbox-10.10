
package com.mlefree.nuxeo.sandbox.login;

import static org.nuxeo.ecm.platform.ui.web.auth.NXAuthConstants.USERIDENT_KEY;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import org.nuxeo.ecm.core.api.NuxeoPrincipal;
import org.nuxeo.ecm.core.api.impl.UserPrincipal;
import org.nuxeo.ecm.core.event.EventContext;
import org.nuxeo.ecm.core.event.EventProducer;
import org.nuxeo.ecm.core.event.impl.UnboundEventContext;
import org.nuxeo.ecm.platform.api.login.UserIdentificationInfo;
import org.nuxeo.ecm.platform.ui.web.auth.CachableUserIdentificationInfo;
import org.nuxeo.runtime.api.Framework;

public class MleAuthenticationFilter implements Filter {

    protected static final String LOGIN_CATEGORY = "NuxeoAuthentication";

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {

        HttpServletRequest httpRequest = (HttpServletRequest) request;
        HttpServletResponse httpResponse = (HttpServletResponse) response;

        // 1) Remove all uri before Login
        String uri = httpRequest.getRequestURI();
        boolean whitelist = uri.equals("/nuxeo/") || uri.equals("/nuxeo/ui") || uri.equals("/nuxeo/jsf")
                || uri.equals("/nuxeo/nuxeo_error.jsp") || uri.equals("/nuxeo/login.jsp");
        boolean blacklist = !uri.contains("api/v1") && !uri.contains("site/automation") && !uri.contains("/ui")
                && !uri.contains(".faces");
        if (whitelist || blacklist) {
            // no extra filter, just following the chain
            chain.doFilter(request, response);
            return;
        }

        // 2) Login has been done, you can add extra filter = verify user and break chain if needed
        CachableUserIdentificationInfo cachableUserIdent = retrieveIdentityFromCache(httpRequest);
        assert cachableUserIdent != null;
        NuxeoPrincipal currentPrincipal = (NuxeoPrincipal) cachableUserIdent.getPrincipal();
        if (currentPrincipal != null && currentPrincipal.getGroups().contains("test")) {
            httpResponse.setStatus(HttpServletResponse.SC_FORBIDDEN);
            try {
                logout(cachableUserIdent, httpRequest, httpResponse);
            } catch (LoginException ignored) {
            }
            return;
        }

        // 3) Else: follow the filters chain
        chain.doFilter(request, response);
    }

    public static CachableUserIdentificationInfo retrieveIdentityFromCache(HttpServletRequest httpRequest) {

        HttpSession session = httpRequest.getSession(false);
        if (session != null) {
            return (CachableUserIdentificationInfo) session.getAttribute(
                    USERIDENT_KEY);
        }

        return null;
    }

    public static void logout(CachableUserIdentificationInfo cachableUserIdent, HttpServletRequest httpRequest,
            HttpServletResponse httpResponse) throws LoginException, IOException {

        // Notify plateform
        String eventId = "logout";
        String comment = " logged out";
        UserIdentificationInfo userInfo = cachableUserIdent.getUserInfo();
        sendAuthenticationEvent(userInfo, eventId, comment);

        // Reset JSESSIONID Cookie
        Cookie cookie = new Cookie("JSESSIONID", null);
        cookie.setMaxAge(0);
        cookie.setPath("/");
        httpResponse.addCookie(cookie);
        HttpSession session = httpRequest.getSession(false);
        if (session != null) {
            session.setAttribute(USERIDENT_KEY, null);
        }

        // Logout from context
        cachableUserIdent.getLoginContext().logout();
    }

    public static boolean sendAuthenticationEvent(UserIdentificationInfo userInfo, String eventId, String comment)
            throws LoginException {

        LoginContext loginContext = null;
        try {
            loginContext = Framework.login();

            EventProducer evtProducer = Framework.getService(EventProducer.class);
            NuxeoPrincipal principal = new UserPrincipal(userInfo.getUserName(), null, false, false);

            Map<String, Serializable> props = new HashMap<>();
            props.put("AuthenticationPlugin", userInfo.getAuthPluginName());
            props.put("LoginPlugin", userInfo.getLoginPluginName());
            props.put("category", LOGIN_CATEGORY);
            props.put("comment", comment);

            EventContext ctx = new UnboundEventContext(principal, props);
            evtProducer.fireEvent(ctx.newEvent(eventId));
            return true;
        } finally {
            if (loginContext != null) {
                loginContext.logout();
            }
        }
    }

}
