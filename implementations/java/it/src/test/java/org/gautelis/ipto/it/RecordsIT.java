/*
 * Copyright (C) 2024-2026 Frode Randers
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

import com.fasterxml.uuid.Generators;
import org.gautelis.ipto.repo.model.Repository;
import org.gautelis.ipto.repo.model.Unit;
import org.gautelis.ipto.repo.model.attributes.Attribute;
import org.gautelis.ipto.repo.model.attributes.RecordAttribute;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.fail;

/**
 *
 */
@Tag("records")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@IptoIT
public class RecordsIT {
    private static final Logger log = LoggerFactory.getLogger(RecordsIT.class);

    @Test
    public void test(Repository repo) {
        final int tenantId = 1;
        UUID processId = Generators.timeBasedEpochGenerator().generate(); // UUID v7
        final String processDescription = "Yrkan om vård av husdjur";

        // ------------------ Steg 1 i process, etablera Yrkan -------------------
        Unit yrkan = repo.createUnit(tenantId, processId);

        /*
         * type Yrkan @record(attribute: yrkan) {
         *     person : Person
         *     beskrivning : String @use(attribute: description)
         *     beslut : Beslut
         *     producerade_resultat : [ProduceratResultat]
         * }
         *
         * type FysiskPerson @record(attribute: fysisk_person) {
         *     personnummer : String
         * }
         */
        yrkan.withRecordAttribute("ffa:fysisk_person", person -> person.withNestedAttributeValue("ffa:personnummer", String.class, value -> value.add("19121212-1212")));

        yrkan.withAttributeValue("dcterms:description", String.class, value -> value.add(processDescription));

        repo.storeUnit(yrkan);

        // ------------------ Steg 2 i process, pröva rätten till -------------------
        yrkan.withAttributeValue("ffa:producerade_resultat", Attribute.class, resultat -> {
            /*
             * enum RattenTillPeriodOmfattning {
             *     HEL
             *     EN_ATTONDEL
             *     OCHSAAVIDARE
             * }
             *
             * enum ErsattningsTyp {
             *     SJUKPENNING # ersattningstyp:SJUKPENNING
             *     FORALDRAPENNING # ersattningstyp:FORALDRAPENNING
             *     HUNDBIDRAG # ersattningstyp:HUNDBIDRAG
             * }
             *
             * type RattenTillPeriod @record(attribute: ratten_till_period) {
             *     ersattningstyp : ErsattningsTyp
             *     omfattning : RattenTillPeriodOmfattning
             * }
             */
            Optional<Attribute<?>> rattenTill = repo.instantiateAttribute("ffa:ratten_till_period");
            if (rattenTill.isPresent()) {
                RecordAttribute rattenTillRecord = RecordAttribute.from(yrkan, rattenTill.get());

                rattenTillRecord.withNestedAttributeValue("ffa:ersattningstyp", String.class, ersattningstyp -> ersattningstyp.add("HUNDBIDRAG"));

                rattenTillRecord.withNestedAttributeValue("ffa:omfattning", String.class, omfattning -> omfattning.add("HEL"));

                Attribute<?> attribute = rattenTill.get();
                resultat.add(attribute);
            }
        });

        repo.storeUnit(yrkan);

        // ------------------ Steg 3 i process, beräkna -------------------
        yrkan.withAttributeValue("ffa:producerade_resultat", Attribute.class, resultat -> {
            /*
             * enum BeloppsPeriod {
             *     PER_TIMME
             *     PER_DAG
             *     PER_MAANAD
             *     PER_AAR
             * }
             *
             * enum Valuta {
             *     SEK
             *     EUR
             * }
             *
             * enum Skattestatus {
             *     SKATTEFRI
             *     SKATTEPLIKTIG
             * }
             *
             * type Belopp @record(attribute: belopp) {
             *     varde : Float
             *     beloppsperiod : BeloppsPeriod
             *     skattestatus : Skattestatus
             *     valuta : Valuta
             * }
             *
             * type Period @record(attribute: period){
             *     from : DateTime
             *     tom : DateTime
             * }
             *
             * enum ErsattningsTyp {
             *     SJUKPENNING # ersattningstyp:SJUKPENNING
             *     FORALDRAPENNING # ersattningstyp:FORALDRAPENNING
             *     HUNDBIDRAG # ersattningstyp:HUNDBIDRAG
             * }
             *
             * type Ersattning @record(attribute: ersattning) {
             *     typ : ErsattningsTyp
             *     belopp : Belopp
             *     period : Period
             * }
             */
            Optional<Attribute<?>> ersattning = repo.instantiateAttribute("ffa:ersattning");
            if (ersattning.isPresent()) {
                RecordAttribute ersattningRecord = RecordAttribute.from(yrkan, ersattning.get());

                ersattningRecord.withNestedAttributeValue("ffa:ersattningstyp", String.class, value -> value.add("HUNDBIDRAG"));

                ersattningRecord.withNestedAttribute("ffa:belopp", Attribute.class, belopp -> {
                    RecordAttribute beloppRecord = RecordAttribute.wrap(yrkan, belopp);

                    beloppRecord.withNestedAttributeValue("ffa:beloppsvarde", Double.class, beloppsvarde -> beloppsvarde.add(1000.0));
                    beloppRecord.withNestedAttributeValue("ffa:beloppsperiodisering", String.class, beloppsperiodisering -> beloppsperiodisering.add("PER_DAG"));
                    beloppRecord.withNestedAttributeValue("ffa:valuta", String.class, valuta -> valuta.add("SEK"));
                    beloppRecord.withNestedAttributeValue("ffa:skattestatus", String.class, skattestatus -> skattestatus.add("SKATTEPLIKTIG"));
                });

                ersattningRecord.withNestedAttribute("ffa:period", Attribute.class, period -> {
                    RecordAttribute periodRecord = RecordAttribute.wrap(yrkan, period);

                    periodRecord.withNestedAttributeValue("ffa:from", Instant.class, value -> value.add(Instant.now()));
                    periodRecord.withNestedAttributeValue("ffa:tom", Instant.class, value -> value.add(Instant.now()));
                });

                resultat.add(ersattningRecord.getDelegate());
            }
        });

        repo.storeUnit(yrkan);

        // ------------------ Steg 4 i process, besluta -------------------
        yrkan.withAttributeValue("ffa:producerade_resultat", Attribute.class, resultat -> {
            /*
             * enum BeslutsTyp {
             *     INTERRIMISTISK
             *     STALLNINGSTAGANDE
             *     SLUTLIGT
             * }
             *
             * enum BeslutsUtfall {
             *     BEVILJAT
             *     AVSLAG
             *     DELVIS_BEVILJANDE
             *     AVVISNING
             *     AVSKRIVNING
             * }
             *
             * enum BeslutsLagrum {
             *     SFB_K112_P2a # SFB Kap. 112 § 2a
             *     SFB_K112_P3  # SFB Kap. 112 § 3
             *     SFB_K112_P4  # SFB Kap. 112 § 4
             *     SFB_K113_P3_S1 # SFB Kap. 113 § 3 p. 1
             *     SFB_K113_P3_S2 # SFB Kap. 113 § 3 p. 2
             *     SFB_K113_P3_S3 # SFB Kap. 113 § 3 p. 3
             *     FL_P36 # FL § 36
             *     FL_P37 # FL § 37
             *     FL_P38 # FL § 38
             * }
             *
             * type Beslut @record(attribute: beslut) {
             *     datum : DateTime @use(attribute: date)
             *     beslutsfattare : String
             *     typ : BeslutsTyp
             *     utfall : BeslutsUtfall
             *     organisation : String
             *     lagrum : BeslutsLagrum
             *     avslagsanledning : String
             * }
             */
            Optional<Attribute<?>> beslut = repo.instantiateAttribute("ffa:beslut");
            if (beslut.isPresent()) {
                RecordAttribute beslutsRecord = RecordAttribute.from(yrkan, beslut.get());

                beslutsRecord.withNestedAttributeValue("dcterms:date", Instant.class, datum -> datum.add(Instant.now()));
                beslutsRecord.withNestedAttributeValue("ffa:beslutsfattare", String.class, beslutsfattare -> beslutsfattare.add("Beslut Person"));
                beslutsRecord.withNestedAttributeValue("ffa:beslutstyp", String.class, beslutstyp -> beslutstyp.add("SLUTLIGT"));
                beslutsRecord.withNestedAttributeValue("ffa:beslutsutfall", String.class, beslutsutfall -> beslutsutfall.add("BEVILJAT"));
                beslutsRecord.withNestedAttributeValue("ffa:organisation", String.class, organisation -> organisation.add("Myndigheten"));
                beslutsRecord.withNestedAttributeValue("ffa:lagrum", String.class, lagrum -> lagrum.add("FL_P38"));
                beslutsRecord.withNestedAttributeValue("ffa:avslagsanledning", String.class, avslagsanledning -> {
                    // Ingen
                });

                resultat.add(beslutsRecord.getDelegate());
            }
        });

        repo.storeUnit(yrkan);

        // ------------------ Process avslutad -------------------

        // Test
        Optional<Unit> _sameUnit = repo.getUnit(yrkan.getTenantId(), yrkan.getUnitId());
        if (_sameUnit.isEmpty()) {
            fail("Unit " + yrkan.getReference() + " exists, but is not retrievable");
        }

        Unit sameUnit = _sameUnit.get();
        Optional<Attribute<String>> _description = sameUnit.getStringAttribute("dcterms:description");
        if (_description.isEmpty()) {
            fail("Attribute 'dcterms:description' not found in unit " + yrkan.getReference() + " but is known to exist");
        }
        Attribute<String> description = _description.get();
        ArrayList<String> valueVector = description.getValueVector();

        if (valueVector.size() != 1) {
            fail("Attribute 'dcterms:description' must have exactly one value");
        }

        if (!processDescription.equals(valueVector.getFirst())) {
            fail("Attribute 'dcterms:description' does not have expected value: \"" + processDescription + "\" != \"" + valueVector.getFirst() + "\"");
        }
    }
}
