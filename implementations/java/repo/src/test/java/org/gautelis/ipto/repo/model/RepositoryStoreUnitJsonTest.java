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
package org.gautelis.ipto.repo.model;

import org.junit.jupiter.api.Test;
import org.gautelis.ipto.repo.model.attributes.AttributeValueRunnable;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RepositoryStoreUnitJsonTest {

    @Test
    void storeAttributesParsesNestedRecords() throws Exception {
        String json = """
                {
                  "attributes" : [ {
                    "@type" : "ipto:record-scalar",
                    "alias" : "producerade_resultat",
                    "attrtype" : "RECORD",
                    "attrname" : "ffa:producerade_resultat",
                    "attributes" : [ {
                      "@type" : "ipto:record-vector",
                      "alias" : "ratten_till_period",
                      "attrtype" : "RECORD",
                      "attrname" : "ffa:ratten_till_period",
                      "attributes" : [ {
                        "@type" : "ipto:string-scalar",
                        "alias" : "ersattningstyp",
                        "attrtype" : "STRING",
                        "attrname" : "ffa:ersattningstyp",
                        "value" : [ "HUNDBIDRAG" ]
                      }, {
                        "@type" : "ipto:string-scalar",
                        "alias" : "omfattning",
                        "attrtype" : "STRING",
                        "attrname" : "ffa:omfattning",
                        "value" : [ "HEL" ]
                      } ]
                    } ]
                  }, {
                    "@type" : "ipto:string-vector",
                    "alias" : "title",
                    "attrtype" : "STRING",
                    "attrname" : "dcterms:title",
                    "value" : [ "Replaced value", "019b290c-7c4b-7592-8298-ced722f531c1" ]
                  }, {
                    "@type" : "ipto:time-scalar",
                    "alias" : "date",
                    "attrtype" : "TIME",
                    "attrname" : "dcterms:date",
                    "value" : [ "2025-12-16T21:24:02.780162" ]
                  } ]
                }
                """;

        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(json);
        JsonNode attributes = root.path("attributes");
        assertTrue(attributes.isArray());

        Repository repo = new Repository(null, 0, new HashMap<>());
        Map<String, Object> stored = new LinkedHashMap<>();
        repo.storeAttributes(new RecordingWriter(stored), attributes);

        @SuppressWarnings("unchecked")
        List<String> title = (List<String>) stored.get("dcterms:title");
        assertEquals(List.of("Replaced value", "019b290c-7c4b-7592-8298-ced722f531c1"), title);

        @SuppressWarnings("unchecked")
        List<Instant> date = (List<Instant>) stored.get("dcterms:date");
        assertEquals(List.of(Instant.parse("2025-12-16T21:24:02.780162Z")), date);

        @SuppressWarnings("unchecked")
        Map<String, Object> resultat = (Map<String, Object>) stored.get("ffa:producerade_resultat");
        @SuppressWarnings("unchecked")
        Map<String, Object> rattenTill = (Map<String, Object>) resultat.get("ffa:ratten_till_period");
        @SuppressWarnings("unchecked")
        List<String> ersattning = (List<String>) rattenTill.get("ffa:ersattningstyp");
        assertEquals(List.of("HUNDBIDRAG"), ersattning);
    }

    private static final class RecordingWriter implements Repository.AttributeWriter {
        private final Map<String, Object> attributes;

        private RecordingWriter(Map<String, Object> attributes) {
            this.attributes = attributes;
        }

        @Override
        public <T> void writeValue(String attrName, Class<T> type, AttributeValueRunnable<T> updater) {
            ArrayList<T> values = new ArrayList<>();
            updater.run(values);
            attributes.put(attrName, values);
        }

        @Override
        public void writeRecord(String attrName, java.util.function.Consumer<Repository.AttributeWriter> updater) {
            Map<String, Object> nested = new LinkedHashMap<>();
            attributes.put(attrName, nested);
            updater.accept(new RecordingWriter(nested));
        }
    }
}
