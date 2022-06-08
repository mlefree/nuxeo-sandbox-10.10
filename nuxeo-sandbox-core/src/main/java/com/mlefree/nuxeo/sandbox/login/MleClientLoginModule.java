
package com.mlefree.nuxeo.sandbox.login;

import static org.nuxeo.ecm.core.api.security.SecurityConstants.ADMINISTRATOR;

import java.security.Principal;
import java.util.Map;
import java.util.Set;

import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;

import org.nuxeo.ecm.core.api.NuxeoPrincipal;
import org.nuxeo.ecm.core.api.SystemPrincipal;
import org.nuxeo.ecm.core.api.local.ClientLoginModule;
import org.nuxeo.ecm.core.api.local.LoginStack;
import org.nuxeo.runtime.api.Framework;
import org.nuxeo.runtime.api.login.LoginComponent;

public class MleClientLoginModule extends ClientLoginModule {


    @Override
    public void initialize(Subject subject, CallbackHandler callbackHandler, Map sharedState, Map options) {
        super.initialize(subject, callbackHandler, sharedState, options);
    }

    @Override
    public boolean login() throws LoginException {
        return super.login();
    }

    @Override
    public boolean logout() throws LoginException {
        return super.logout();
    }

}
