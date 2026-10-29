package org.gautelis.ipto.security;

import io.quarkus.elytron.security.common.BcryptUtil;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/** Interactive helper; never accepts passwords in command-line arguments. */
public final class PasswordHash {
    public static void main(String[] args) {
        var console = System.console();
        if (console == null) {
            throw new IllegalStateException("Run this command in an interactive terminal");
        }
        char[] password = console.readPassword("Password: ");
        char[] confirmation = console.readPassword("Confirm password: ");
        try {
            if (password == null || confirmation == null || !Arrays.equals(password, confirmation)) {
                throw new IllegalArgumentException("Passwords do not match");
            }
            String value = new String(password);
            if (value.isEmpty() || value.getBytes(StandardCharsets.UTF_8).length > 72) {
                throw new IllegalArgumentException("Password must contain 1–72 UTF-8 bytes");
            }
            console.printf("%s%n", BcryptUtil.bcryptHash(value, 12));
        } finally {
            if (password != null) Arrays.fill(password, '\0');
            if (confirmation != null) Arrays.fill(confirmation, '\0');
        }
    }
}
