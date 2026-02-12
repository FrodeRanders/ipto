### Retrieving data using GraphQL

#### Retrieve two units
```graphql
query Unit {
  yrkan1: yrkan(id: { tenantId: 1, unitId: 1553 }) {
    person {
      ... on FysiskPerson {
        personnummer
      }
      ... on JuridiskPerson {
        orgnummer
      }
    }
    producerade_resultat {
      ... on Ersattning {
        ersattningstyp
        belopp {
          beloppsvarde
        }
      }
    }
  }

  yrkan2: yrkan(id: { tenantId: 1, unitId: 1554 }) {
    person {
      ... on FysiskPerson {
        personnummer
      }
      ... on JuridiskPerson {
        orgnummer
      }
    }
    beslut {
      datum
      beslutsfattare
    }
  }
}
```
&darr;
```json
 {
  "yrkan1" : {
    "person" : {
      "personnummer" : "19121212-1212"
    },
    "producerade_resultat" : [ { }, {
      "ersattningstyp" : "HUNDBIDRAG",
      "belopp" : {
        "beloppsvarde" : 1000.0
      }
    } ]
  },
  "yrkan2" : {
    "person" : {
      "personnummer" : "19121212-1212"
    },
    "beslut" : {
      "datum" : "2025-12-07T20:15:34.116578Z",
      "beslutsfattare" : "019afa74-8ee0-7bfc-80e1-1dd37ff9ae0e"
    }
  }
}
```
#### Retrieve a unit
```graphql
query Unit($id: UnitIdentification!) {
  yrkan(id: $id) {
    person {
      ... on FysiskPerson {
        personnummer
      }
      ... on JuridiskPerson {
        orgnummer
      }
    }
    producerade_resultat {
      ... on Ersattning {
         ersattningstyp
         belopp {
           beloppsvarde
         }
      }
    }
  }
}
```
```java
int tenantId = 1;
long unitId = 42;

ExecutionResult result = graphQL.execute(
        ExecutionInput.newExecutionInput()
                .query(query)
                .variables(Map.of(
                    "id", Map.of(
                         "tenantId", tenantId,
                         "unitId",   unitId
                         )
                    )
                )
                .build());
```
&darr;
```json
{
  "yrkan" : {
    "person" : {
      "personnummer" : "19121212-1212"
    },
    "producerade_resultat" : [ { }, {
      "ersattningstyp" : "HUNDBIDRAG",
      "belopp" : {
        "beloppsvarde" : 1000.0
      }
    } ]
  }
}
```
#### Retrieve a unit as JSON

```graphql
query Unit($id: UnitIdentification!) {
  yrkanRaw(id: $id)
}
```
```java
int tenantId = 1;
long unitId = 42;

ExecutionResult result = graphQL.execute(
        ExecutionInput.newExecutionInput()
                .query(query)
                .variables(Map.of(
                    "id", Map.of(
                         "tenantId", tenantId,
                         "unitId",   unitId
                         )
                    )
                )
                .build());
```
&darr;
```json
{"@type":"ipto:unit","@version":2,"tenantid":1,"unitid":42,"unitver":1,"corrid":"019afa74-8f33-7e8f-8f97-83c192afba8e","status":30,"unitname":null,"created":"2025-12-07T21:15:34.195568Z","modified":"2025-12-07T21:15:34.195568Z","attributes":[{"@type":"record-vector","attrname":"ffa:producerade_resultat","alias":"producerade_resultat","attrtype":"RECORD","attrid":22,"attributes":[{"@type":"record-vector","attrname":"ffa:ratten_till_period","alias":"ratten_till_period","attrtype":"RECORD","attrid":18,"attributes":[{"@type":"string-scalar","attrname":"ffa:ersattningstyp","alias":"ersattningstyp","attrtype":"STRING","attrid":24,"value":["HUNDBIDRAG"]},{"@type":"string-scalar","attrname":"ffa:omfattning","alias":"omfattning","attrtype":"STRING","attrid":44,"value":["HEL"]}]},{"@type":"record-vector","attrname":"ffa:ersattning","alias":"ersattning","attrtype":"RECORD","attrid":42,"attributes":[{"@type":"string-scalar","attrname":"ffa:ersattningstyp","alias":"ersattningstyp","attrtype":"STRING","attrid":24,"value":["HUNDBIDRAG"]},{"@type":"record-vector","attrname":"ffa:belopp","alias":"belopp","attrtype":"RECORD","attrid":46,"attributes":[{"@type":"double-scalar","attrname":"ffa:beloppsvarde","alias":"beloppsvarde","attrtype":"DOUBLE","attrid":31,"value":[1000.0]},{"@type":"string-scalar","attrname":"ffa:beloppsperiodisering","alias":"beloppsperiodisering","attrtype":"STRING","attrid":49,"value":["PER_DAG"]},{"@type":"string-scalar","attrname":"ffa:valuta","alias":"valuta","attrtype":"STRING","attrid":2,"value":["SEK"]},{"@type":"string-scalar","attrname":"ffa:skattestatus","alias":"skattestatus","attrtype":"STRING","attrid":28,"value":["SKATTEPLIKTIG"]}]},{"@type":"record-vector","attrname":"ffa:period","alias":"period","attrtype":"RECORD","attrid":16,"attributes":[{"@type":"time-scalar","attrname":"ffa:from","alias":"from","attrtype":"TIME","attrid":13,"value":["2025-12-07T20:15:34.195656Z"]},{"@type":"time-scalar","attrname":"ffa:tom","alias":"tom","attrtype":"TIME","attrid":33,"value":["2025-12-07T20:15:34.195657Z"]}]}]}]},{"@type":"record-vector","attrname":"ffa:beslut","alias":"beslut","attrtype":"RECORD","attrid":48,"attributes":[{"@type":"time-scalar","attrname":":date","alias":"date","attrtype":"TIME","attrid":1,"value":["2025-12-07T20:15:34.195619Z"]},{"@type":"string-scalar","attrname":"ffa:beslutsfattare","alias":"beslutsfattare","attrtype":"STRING","attrid":35,"value":["019afa74-8f33-76a9-a5ee-4552201ee333"]},{"@type":"string-scalar","attrname":"ffa:beslutstyp","alias":"beslutstyp","attrtype":"STRING","attrid":11,"value":["SLUTLIGT"]},{"@type":"string-scalar","attrname":"ffa:beslutsutfall","alias":"beslutsutfall","attrtype":"STRING","attrid":12,"value":["BEVILJAT"]},{"@type":"string-scalar","attrname":"ffa:organisation","alias":"organisation","attrtype":"STRING","attrid":4,"value":["Myndigheten"]},{"@type":"string-scalar","attrname":"ffa:lagrum","alias":"lagrum","attrtype":"STRING","attrid":40,"value":["FL_P38"]},{"@type":"string-scalar","attrname":"ffa:avslagsanledning","alias":"avslagsanledning","attrtype":"STRING","attrid":32,"value":[]}]},{"@type":"string-scalar","attrname":":description","alias":"description","attrtype":"STRING","attrid":27,"value":["Yrkan om vård av husdjur"]},{"@type":"record-scalar","attrname":"ffa:fysisk_person","alias":"fysisk_person","attrtype":"RECORD","attrid":38,"attributes":[{"@type":"string-scalar","attrname":"ffa:personnummer","alias":"personnummer","attrtype":"STRING","attrid":39,"value":["19121212-1212"]}]}]}
```

#### Perform a search

```graphql
query Unit($filter: Filter!) {
  yrkanden(filter: $filter) {
    person {
      ... on FysiskPerson {
        personnummer
      }
      ... on JuridiskPerson {
        orgnummer
      }
    }
    producerade_resultat {
      ... on Ersattning {
         ersattningstyp
         belopp {
           beloppsvarde
         }
      }
    }
    beslut {
      datum
      beslutsfattare
    }
  }
}
```
```java
int tenantId = 1;

String expr = "beslutsfattare == \"" + beslutsfattare + "\"";

ExecutionResult result = graphQL.execute(
        ExecutionInput.newExecutionInput()
                .query(query)
                .variables(
                        Map.of(
                                "filter", Map.of(
                                        "tenantId", tenantId,
                                        "where", expr
                                )
                        )
                )                .build());
```
&darr;
```json
{
  "yrkanden" : [ {
    "person" : {
      "personnummer" : "19121212-1212"
    },
    "producerade_resultat" : [ { }, {
      "ersattningstyp" : "HUNDBIDRAG",
      "belopp" : {
        "beloppsvarde" : 1000.0
      }
    } ],
    "beslut" : {
      "datum" : "2025-12-07T20:15:34.201759Z",
      "beslutsfattare" : "019afa74-8f39-76f7-9b81-d3280d6852f9"
    }
  } ]
}
```
#### Perform a search, inlined

```graphql
query Unit {
  yrkanden1: yrkanden(filter: {tenantId: 1, where: "beslutsfattare == \"019bb927-85f4-77e5-9eb6-630a0e23f207\""}) {
    person {
      ... on FysiskPerson {
        personnummer
      }
      ... on JuridiskPerson {
        orgnummer
      }
    }
  }
  yrkanden2: yrkanden(filter: {tenantId: 1, where: "beslutsfattare == \"019afa74-8f49-7c6b-96d6-0d867dab7664\""}) {
    person {
      ... on FysiskPerson {
        personnummer
      }
      ... on JuridiskPerson {
        orgnummer
      }
    }
  }
}
```
&darr; 
```json
{
  "yrkanden1" : [ {
    "person" : {
      "personnummer" : "19121212-1212"
    }
  } ],
  "yrkanden2" : [ {
    "person" : {
      "personnummer" : "19121212-1212"
    }
  } ]
}
```
#### Perform a search, results as JSON
```graphql
query Unit($filter: Filter!) {
  yrkandenRaw(filter: $filter)
}
```
```java
String beslutsfattare = "019afa74-8f53-7b6f-bb77-c01ec60bd52f";
int tenantId = 1;

String expr = "beslutsfattare == \"" + beslutsfattare + "\"";

ExecutionResult result = graphQL.execute(
        ExecutionInput.newExecutionInput()
                .query(query)
                .variables(
                        Map.of(
                                "filter", Map.of(
                                        "tenantId", tenantId,
                                        "where", expr
                                )
                        )
                )
                .build());

```
&darr;
```json
[{"@type":"ipto:unit","@version":2,"tenantid":1,"unitid":1560,"unitver":1,"corrid":"019afa74-8f53-71f7-9550-211ec1646ce6","status":30,"unitname":null,"created":"2025-12-07T21:15:34.227929Z","modified":"2025-12-07T21:15:34.227929Z","attributes":[{"@type":"record-vector","attrname":"ffa:producerade_resultat","alias":"producerade_resultat","attrtype":"RECORD","attrid":22,"attributes":[{"@type":"record-vector","attrname":"ffa:ratten_till_period","alias":"ratten_till_period","attrtype":"RECORD","attrid":18,"attributes":[{"@type":"string-scalar","attrname":"ffa:ersattningstyp","alias":"ersattningstyp","attrtype":"STRING","attrid":24,"value":["HUNDBIDRAG"]},{"@type":"string-scalar","attrname":"ffa:omfattning","alias":"omfattning","attrtype":"STRING","attrid":44,"value":["HEL"]}]},{"@type":"record-vector","attrname":"ffa:ersattning","alias":"ersattning","attrtype":"RECORD","attrid":42,"attributes":[{"@type":"string-scalar","attrname":"ffa:ersattningstyp","alias":"ersattningstyp","attrtype":"STRING","attrid":24,"value":["HUNDBIDRAG"]},{"@type":"record-vector","attrname":"ffa:belopp","alias":"belopp","attrtype":"RECORD","attrid":46,"attributes":[{"@type":"double-scalar","attrname":"ffa:beloppsvarde","alias":"beloppsvarde","attrtype":"DOUBLE","attrid":31,"value":[1000.0]},{"@type":"string-scalar","attrname":"ffa:beloppsperiodisering","alias":"beloppsperiodisering","attrtype":"STRING","attrid":49,"value":["PER_DAG"]},{"@type":"string-scalar","attrname":"ffa:valuta","alias":"valuta","attrtype":"STRING","attrid":2,"value":["SEK"]},{"@type":"string-scalar","attrname":"ffa:skattestatus","alias":"skattestatus","attrtype":"STRING","attrid":28,"value":["SKATTEPLIKTIG"]}]},{"@type":"record-vector","attrname":"ffa:period","alias":"period","attrtype":"RECORD","attrid":16,"attributes":[{"@type":"time-scalar","attrname":"ffa:from","alias":"from","attrtype":"TIME","attrid":13,"value":["2025-12-07T20:15:34.227985Z"]},{"@type":"time-scalar","attrname":"ffa:tom","alias":"tom","attrtype":"TIME","attrid":33,"value":["2025-12-07T20:15:34.227986Z"]}]}]}]},{"@type":"record-vector","attrname":"ffa:beslut","alias":"beslut","attrtype":"RECORD","attrid":48,"attributes":[{"@type":"time-scalar","attrname":":date","alias":"date","attrtype":"TIME","attrid":1,"value":["2025-12-07T20:15:34.227958Z"]},{"@type":"string-scalar","attrname":"ffa:beslutsfattare","alias":"beslutsfattare","attrtype":"STRING","attrid":35,"value":["019afa74-8f53-7b6f-bb77-c01ec60bd52f"]},{"@type":"string-scalar","attrname":"ffa:beslutstyp","alias":"beslutstyp","attrtype":"STRING","attrid":11,"value":["SLUTLIGT"]},{"@type":"string-scalar","attrname":"ffa:beslutsutfall","alias":"beslutsutfall","attrtype":"STRING","attrid":12,"value":["BEVILJAT"]},{"@type":"string-scalar","attrname":"ffa:organisation","alias":"organisation","attrtype":"STRING","attrid":4,"value":["Myndigheten"]},{"@type":"string-scalar","attrname":"ffa:lagrum","alias":"lagrum","attrtype":"STRING","attrid":40,"value":["FL_P38"]},{"@type":"string-scalar","attrname":"ffa:avslagsanledning","alias":"avslagsanledning","attrtype":"STRING","attrid":32,"value":[]}]},{"@type":"string-scalar","attrname":":description","alias":"description","attrtype":"STRING","attrid":27,"value":["Yrkan om vård av husdjur"]},{"@type":"record-scalar","attrname":"ffa:fysisk_person","alias":"fysisk_person","attrtype":"RECORD","attrid":38,"attributes":[{"@type":"string-scalar","attrname":"ffa:personnummer","alias":"personnummer","attrtype":"STRING","attrid":39,"value":["19121212-1212"]}]}]}]
```

### Operations wiring contract

Root `Query`/`Mutation` operations are wired from SDL automatically.  
Use the `@ipto` directive on operation fields to define explicit runtime mapping:

```graphql
directive @ipto(operation: String!) on FIELD_DEFINITION
```

Supported values for `operation`:

- `LOAD_UNIT`: load a structured unit by `id` (`tenantId`, `unitId`).
- `LOAD_UNIT_RAW`: load full unit JSON as `Bytes` by `id` (`tenantId`, `unitId`).
- `LOAD_BY_CORRID`: load a structured unit by `id` (`tenantId`, `corrId`).
- `LOAD_RAW_PAYLOAD_BY_CORRID`: load raw payload as `Bytes` by `id` (`tenantId`, `corrId`).
- `SEARCH`: search and return structured results using `filter`.
- `SEARCH_RAW`: search and return full unit JSON as `Bytes` using `filter`.
- `SEARCH_RAW_PAYLOAD`: search and return payload JSON as `Bytes` using `filter`.
- `STORE_RAW_UNIT`: store native IPTO unit JSON from `data: Bytes`.
- `CUSTOM` or `MANUAL`: do not auto-wire this field; wire it in bootstrap code.

Notes:

- Ambiguous operations without `@ipto(operation: ...)` fail startup with a configuration error.
- Non-ambiguous operations may still use inference, but explicit `@ipto` is recommended.
- `CUSTOM` is intended for domain-specific transforms (for example, branded payload to native unit JSON) before repository storage.
