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
package org.gautelis.ipto.it;

import org.gautelis.ipto.repo.model.Repository;
import org.gautelis.ipto.repo.model.Unit;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.io.PrintWriter;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@Tag("units")
@IptoIT
class StoreRawUnitIT {

    @Test
    void storeRawUnitPersistsNestedRecordAttributes(Repository repo) throws Exception {
        String json = """
                {
                  "@type" : "ipto:unit",
                  "@version" : 2,
                  "tenantid" : 1,
                  "unitid" : null,
                  "unitver" : null,
                  "status" : 30,
                  "unitname" : "raw-unit-test",
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
        Unit stored = repo.storeUnit(root);
        assertNotNull(stored);

        Unit reloaded = repo.getUnit(1, stored.getUnitId()).orElseThrow();
        JsonNode rawNode = mapper.readTree(reloaded.asJson(false));

        JsonNode attrs = rawNode.path("attributes");
        JsonNode title = findAttribute(attrs, "dcterms:title");
        assertNotNull(title);
        assertEquals(List.of("Replaced value", "019b290c-7c4b-7592-8298-ced722f531c1"),
                mapper.convertValue(title.path("value"), List.class));

        JsonNode date = findAttribute(attrs, "dcterms:date");
        assertNotNull(date);
        assertEquals(List.of("2025-12-16T21:24:02.780162Z"),
                mapper.convertValue(date.path("value"), List.class));

        JsonNode producerade = findAttribute(attrs, "ffa:producerade_resultat");
        assertNotNull(producerade);
        JsonNode rattenTill = findAttribute(producerade.path("attributes"), "ffa:ratten_till_period");
        assertNotNull(rattenTill);
        JsonNode ersattningstyp = findAttribute(rattenTill.path("attributes"), "ffa:ersattningstyp");
        assertNotNull(ersattningstyp);
        assertEquals(List.of("HUNDBIDRAG"),
                mapper.convertValue(ersattningstyp.path("value"), List.class));

        JsonNode omfattning = findAttribute(rattenTill.path("attributes"), "ffa:omfattning");
        assertNotNull(omfattning);
        assertEquals(List.of("HEL"),
                mapper.convertValue(omfattning.path("value"), List.class));

        reloaded.requestStatusTransition(Unit.Status.PENDING_DISPOSITION);
        repo.dispose(reloaded.getTenantId(), new PrintWriter(System.out));
    }

    private JsonNode findAttribute(JsonNode attributes, String name) {
        for (JsonNode attribute : attributes) {
            if (name.equals(attribute.path("attrname").asString())) {
                return attribute;
            }
        }
        return null;
    }
}
