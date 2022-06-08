
package com.mlefree.nuxeo.sandbox.login;

import java.util.Map;

import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.LoginException;

import org.nuxeo.ecm.core.api.local.ClientLoginModule;

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
