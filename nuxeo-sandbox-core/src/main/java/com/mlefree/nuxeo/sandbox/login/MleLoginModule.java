
package com.mlefree.nuxeo.sandbox.login;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.LoginException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nuxeo.ecm.core.api.NuxeoPrincipal;
import org.nuxeo.ecm.platform.login.NuxeoLoginModule;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

public class MleLoginModule extends NuxeoLoginModule {

    private static final Log log = LogFactory.getLog(MleLoginModule.class);

    @Override
    public void initialize(Subject subject, CallbackHandler callbackHandler, Map<String, ?> sharedState,
            Map<String, ?> options) {

        super.initialize(subject, callbackHandler, sharedState, options);
    }


    @Override
    public boolean login() throws LoginException {

        boolean ok = super.login();
        if (!ok) {
            return false;
        }

        NuxeoPrincipal user = getPrincipal();
        String company = getCompanyFromApiCall();
        user.setCompany(company);
        return true;
    }

    @Override
    public NuxeoPrincipal createIdentity(String username) throws LoginException {
        log.debug("createIdentity: " + username);
        return super.createIdentity(username);
    }

    @Override
    public boolean logout() throws LoginException {

        return super.logout();
    }

    private String getCompanyFromApiCall() {

        Request request = new Request.Builder().url("https://catfact.ninja/fact")
                .get()
                .build();

        try (Response response = getHttpClient().newCall(request).execute()) {


            return response.message();

        } catch (IOException e) {
            log.error("API call error:", e);
        }
        return "Not found";
    }

    private OkHttpClient getHttpClient() {
        int timeoutInSeconds = 10;
        return new OkHttpClient.Builder().connectTimeout(timeoutInSeconds, TimeUnit.SECONDS)
                .readTimeout(timeoutInSeconds, TimeUnit.SECONDS)
                .writeTimeout(timeoutInSeconds, TimeUnit.SECONDS)
                .build();
    }


}
