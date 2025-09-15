package org.gautelis.repo.udf;

import javax.sql.rowset.serial.SerialBlob;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;
import java.util.Base64;

public class B64 {
    private static final Base64.Encoder ENC = Base64.getEncoder();
    private static final Base64.Decoder DEC = Base64.getDecoder();

    //public static String encode(byte[] b) { return b == null ? null : ENC.encodeToString(b); }

    public static Blob decode(Clob s) throws SQLException {
        if (s == null) return null;
        byte[] bytes = DEC.decode(s.toString());
        return new SerialBlob(bytes);
    }
}
