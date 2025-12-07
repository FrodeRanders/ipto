### Creating and retrieving data using the Java API

For testing and demo purposes, we ship this [GraphQL SDL](../repo/src/test/resources/org/gautelis/repo/schema2.graphqls).

```java
Repository repo = RepositoryFactory.getRepository();

final int tenantId = 1;
UUID processId = Generators.timeBasedEpochGenerator().generate(); // UUID v7
```

The idea here is to represent the total ```process state``` as a single ```unit```.
The process has a unique ID, the ```processId```. The comments make references to 
definitions in the GraphQL SDL.

```java
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
yrkan.withRecordAttribute("ffa:fysisk_person", person -> {
    person.withNestedAttributeValue("ffa:personnummer", String.class, value -> {
        value.add("19121212-1212");
    });
});

yrkan.withAttributeValue("dce:description", String.class, value -> {
    value.add("Yrkan om vård av husdjur");
});

repo.storeUnit(yrkan);
```

```java
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

        rattenTillRecord.withNestedAttributeValue("ffa:ersattningstyp", String.class, ersattningstyp -> {
            ersattningstyp.add("HUNDBIDRAG");
        });

        rattenTillRecord.withNestedAttributeValue("ffa:omfattning", String.class, omfattning -> {
            omfattning.add("HEL");
        });

        Attribute<?> attribute = rattenTill.get();
        resultat.add(attribute);
    }
});

repo.storeUnit(yrkan);
```

```java
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

        ersattningRecord.withNestedAttributeValue("ffa:ersattningstyp", String.class, value -> {
            value.add("HUNDBIDRAG");
        });

        ersattningRecord.withNestedAttribute("ffa:belopp", Attribute.class, belopp -> {
            RecordAttribute beloppRecord = RecordAttribute.wrap(yrkan, belopp);

            beloppRecord.withNestedAttributeValue("ffa:beloppsvarde", Double.class, beloppsvarde -> {
                beloppsvarde.add(1000.0);
            });
            beloppRecord.withNestedAttributeValue("ffa:beloppsperiodisering", String.class, beloppsperiodisering -> {
                beloppsperiodisering.add("PER_DAG");
            });
            beloppRecord.withNestedAttributeValue("ffa:valuta", String.class, valuta -> {
                valuta.add("SEK");
            });
            beloppRecord.withNestedAttributeValue("ffa:skattestatus", String.class, skattestatus -> {
                skattestatus.add("SKATTEPLIKTIG");
            });
        });

        ersattningRecord.withNestedAttribute("ffa:period", Attribute.class, period -> {
            RecordAttribute periodRecord = RecordAttribute.wrap(yrkan, period);

            periodRecord.withNestedAttributeValue("ffa:from", Instant.class, value -> {
                value.add(Instant.now());
            });
            periodRecord.withNestedAttributeValue("ffa:tom", Instant.class, value -> {
                value.add(Instant.now());
            });
        });

        resultat.add(ersattningRecord.getDelegate());
    }
});

repo.storeUnit(yrkan);
```

```java
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

        beslutsRecord.withNestedAttributeValue("dce:date", Instant.class, datum -> {
            datum.add(Instant.now());
        });

        beslutsRecord.withNestedAttributeValue("ffa:beslutsfattare", String.class, beslutsfattare -> {
            beslutsfattare.add("Beslut Person");
        });

        beslutsRecord.withNestedAttributeValue("ffa:beslutstyp", String.class, beslutstyp -> {
            beslutstyp.add("SLUTLIGT");
        });

        beslutsRecord.withNestedAttributeValue("ffa:beslutsutfall", String.class, beslutsutfall -> {
            beslutsutfall.add("BEVILJAT");
        });

        beslutsRecord.withNestedAttributeValue("ffa:organisation", String.class, organisation -> {
            organisation.add("Myndigheten");
        });

        beslutsRecord.withNestedAttributeValue("ffa:lagrum", String.class, lagrum -> {
            lagrum.add("FL_P38");
        });

        beslutsRecord.withNestedAttributeValue("ffa:avslagsanledning", String.class, avslagsanledning -> {
            // Ingen
        });

        resultat.add(beslutsRecord.getDelegate());
    }
});

repo.storeUnit(yrkan);
```

The result is something like:
```json
{
  "@type": "ipto:unit",
  "@version": 4,
  "tenantid": 1,
  "unitid": 1560,
  "unitver": 1,
  "corrid": "019afa74-8f53-71f7-9550-211ec1646ce6",
  "status": 30,
  "unitname": null,
  "created": "2025-12-07T21:15:34.227929Z",
  "modified": "2025-12-07T21:15:34.227947Z",
  "attributes": [
    {
      "@type": "record-vector",
      "attrname": "ffa:producerade_resultat",
      "alias": "producerade_resultat",
      "attrtype": "RECORD",
      "attrid": 22,
      "attributes": [
        {
          "@type": "record-vector",
          "attrname": "ffa:ratten_till_period",
          "alias": "ratten_till_period",
          "attrtype": "RECORD",
          "attrid": 18,
          "attributes": [
            {
              "@type": "string-scalar",
              "attrname": "ffa:ersattningstyp",
              "alias": "ersattningstyp",
              "attrtype": "STRING",
              "attrid": 24,
              "value": [
                "HUNDBIDRAG"
              ]
            },
            {
              "@type": "string-scalar",
              "attrname": "ffa:omfattning",
              "alias": "omfattning",
              "attrtype": "STRING",
              "attrid": 44,
              "value": [
                "HEL"
              ]
            }
          ]
        },
        {
          "@type": "record-vector",
          "attrname": "ffa:ersattning",
          "alias": "ersattning",
          "attrtype": "RECORD",
          "attrid": 42,
          "attributes": [
            {
              "@type": "string-scalar",
              "attrname": "ffa:ersattningstyp",
              "alias": "ersattningstyp",
              "attrtype": "STRING",
              "attrid": 24,
              "value": [
                "HUNDBIDRAG"
              ]
            },
            {
              "@type": "record-vector",
              "attrname": "ffa:belopp",
              "alias": "belopp",
              "attrtype": "RECORD",
              "attrid": 46,
              "attributes": [
                {
                  "@type": "double-scalar",
                  "attrname": "ffa:beloppsvarde",
                  "alias": "beloppsvarde",
                  "attrtype": "DOUBLE",
                  "attrid": 31,
                  "value": [
                    1000.0
                  ]
                },
                {
                  "@type": "string-scalar",
                  "attrname": "ffa:beloppsperiodisering",
                  "alias": "beloppsperiodisering",
                  "attrtype": "STRING",
                  "attrid": 49,
                  "value": [
                    "PER_DAG"
                  ]
                },
                {
                  "@type": "string-scalar",
                  "attrname": "ffa:valuta",
                  "alias": "valuta",
                  "attrtype": "STRING",
                  "attrid": 2,
                  "value": [
                    "SEK"
                  ]
                },
                {
                  "@type": "string-scalar",
                  "attrname": "ffa:skattestatus",
                  "alias": "skattestatus",
                  "attrtype": "STRING",
                  "attrid": 28,
                  "value": [
                    "SKATTEPLIKTIG"
                  ]
                }
              ]
            },
            {
              "@type": "record-vector",
              "attrname": "ffa:period",
              "alias": "period",
              "attrtype": "RECORD",
              "attrid": 16,
              "attributes": [
                {
                  "@type": "time-scalar",
                  "attrname": "ffa:from",
                  "alias": "from",
                  "attrtype": "TIME",
                  "attrid": 13,
                  "value": [
                    "2025-12-07T20:15:34.227985Z"
                  ]
                },
                {
                  "@type": "time-scalar",
                  "attrname": "ffa:tom",
                  "alias": "tom",
                  "attrtype": "TIME",
                  "attrid": 33,
                  "value": [
                    "2025-12-07T20:15:34.227986Z"
                  ]
                }
              ]
            }
          ]
        }
      ]
    },
    {
      "@type": "record-vector",
      "attrname": "ffa:beslut",
      "alias": "beslut",
      "attrtype": "RECORD",
      "attrid": 48,
      "attributes": [
        {
          "@type": "time-scalar",
          "attrname": "dce:date",
          "alias": "date",
          "attrtype": "TIME",
          "attrid": 1,
          "value": [
            "2025-12-07T20:15:34.227958Z"
          ]
        },
        {
          "@type": "string-scalar",
          "attrname": "ffa:beslutsfattare",
          "alias": "beslutsfattare",
          "attrtype": "STRING",
          "attrid": 35,
          "value": [
            "019afa74-8f53-7b6f-bb77-c01ec60bd52f"
          ]
        },
        {
          "@type": "string-scalar",
          "attrname": "ffa:beslutstyp",
          "alias": "beslutstyp",
          "attrtype": "STRING",
          "attrid": 11,
          "value": [
            "SLUTLIGT"
          ]
        },
        {
          "@type": "string-scalar",
          "attrname": "ffa:beslutsutfall",
          "alias": "beslutsutfall",
          "attrtype": "STRING",
          "attrid": 12,
          "value": [
            "BEVILJAT"
          ]
        },
        {
          "@type": "string-scalar",
          "attrname": "ffa:organisation",
          "alias": "organisation",
          "attrtype": "STRING",
          "attrid": 4,
          "value": [
            "Myndigheten"
          ]
        },
        {
          "@type": "string-scalar",
          "attrname": "ffa:lagrum",
          "alias": "lagrum",
          "attrtype": "STRING",
          "attrid": 40,
          "value": [
            "FL_P38"
          ]
        },
        {
          "@type": "string-scalar",
          "attrname": "ffa:avslagsanledning",
          "alias": "avslagsanledning",
          "attrtype": "STRING",
          "attrid": 32,
          "value": []
        }
      ]
    },
    {
      "@type": "string-scalar",
      "attrname": "dce:description",
      "alias": "description",
      "attrtype": "STRING",
      "attrid": 27,
      "value": [
        "Yrkan om vård av husdjur"
      ]
    },
    {
      "@type": "record-scalar",
      "attrname": "ffa:fysisk_person",
      "alias": "fysisk_person",
      "attrtype": "RECORD",
      "attrid": 38,
      "attributes": [
        {
          "@type": "string-scalar",
          "attrname": "ffa:personnummer",
          "alias": "personnummer",
          "attrtype": "STRING",
          "attrid": 39,
          "value": [
            "19121212-1212"
          ]
        }
      ]
    }
  ]
}
```
