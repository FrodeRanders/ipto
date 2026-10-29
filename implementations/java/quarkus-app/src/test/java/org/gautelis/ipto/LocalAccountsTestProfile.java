package org.gautelis.ipto;

import io.quarkus.elytron.security.common.BcryptUtil;
import io.quarkus.test.junit.QuarkusTestProfile;

import java.util.Map;

public class LocalAccountsTestProfile implements QuarkusTestProfile {
    static final String ADMIN_PASSWORD = "test-admin-password";
    static final String READER_PASSWORD = "test-reader-password";

    @Override
    public Map<String, String> getConfigOverrides() {
        return Map.of("ipto-accounts.admin-name", "admin",
                "ipto-accounts.admin-password-hash", BcryptUtil.bcryptHash(ADMIN_PASSWORD, 4),
                "ipto-accounts.reader-name", "reader",
                "ipto-accounts.reader-password-hash", BcryptUtil.bcryptHash(READER_PASSWORD, 4));
    }
}
