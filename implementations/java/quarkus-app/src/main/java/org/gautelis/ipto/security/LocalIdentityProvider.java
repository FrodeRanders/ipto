package org.gautelis.ipto.security;

import io.quarkus.elytron.security.common.BcryptUtil;
import io.quarkus.security.AuthenticationFailedException;
import io.quarkus.security.identity.AuthenticationRequestContext;
import io.quarkus.security.identity.IdentityProvider;
import io.quarkus.security.identity.SecurityIdentity;
import io.quarkus.security.identity.request.UsernamePasswordAuthenticationRequest;
import io.quarkus.security.runtime.QuarkusSecurityIdentity;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.Optional;
import java.nio.charset.StandardCharsets;

/** Local accounts are disabled until a bcrypt password hash is configured. */
@ApplicationScoped
public class LocalIdentityProvider implements IdentityProvider<UsernamePasswordAuthenticationRequest> {
    @ConfigProperty(name = "ipto-accounts.admin-name", defaultValue = "admin")
    String adminName;

    @ConfigProperty(name = "ipto-accounts.admin-password-hash")
    Optional<String> adminHash;

    @ConfigProperty(name = "ipto-accounts.reader-name", defaultValue = "reader")
    String readerName;

    @ConfigProperty(name = "ipto-accounts.reader-password-hash")
    Optional<String> readerHash;

    @Override
    public Class<UsernamePasswordAuthenticationRequest> getRequestType() {
        return UsernamePasswordAuthenticationRequest.class;
    }

    @Override
    public Uni<SecurityIdentity> authenticate(UsernamePasswordAuthenticationRequest request,
                                               AuthenticationRequestContext context) {
        // Bcrypt is intentionally expensive; never run it on the event loop.
        return context.runBlocking(() -> {
            String name = request.getUsername();
            Optional<String> hash = name.equals(adminName) ? adminHash
                    : name.equals(readerName) ? readerHash : Optional.empty();
            boolean matches = false;
            String password = new String(request.getPassword().getPassword());
            if (hash.isPresent() && !password.isEmpty()
                    && password.getBytes(StandardCharsets.UTF_8).length <= 72) {
                try {
                    matches = BcryptUtil.matches(password, hash.get());
                } catch (RuntimeException ignored) {
                    // Invalid configuration must fail closed, without revealing the hash.
                }
            }
            if (!matches) {
                throw new AuthenticationFailedException();
            }
            return QuarkusSecurityIdentity.builder()
                    .setPrincipal(() -> name)
                    .addRole(name.equals(adminName) ? "administrator" : "reader")
                    .build();
        });
    }
}
