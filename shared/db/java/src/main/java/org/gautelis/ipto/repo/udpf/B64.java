/*
 * Copyright (C) 2025-2026 Frode Randers
 * All rights reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gautelis.ipto.repo.udpf;

import javax.sql.rowset.serial.SerialBlob;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;
import java.util.Base64;

/**
 * User defined function (UDF)
 */
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
