# IPTO - A data management framework

A data management system around 'units', which encapsulate dynamic sets of data.

See end of README for instructions on how to setup test.

## Configuration
Configuration of attributes (using GraphQL SDL):
```graphql
schema {
    query: Query
}

##############  directive definitions ############################
directive @datatypeRegistry on ENUM
directive @datatype(id: Int!) on ENUM_VALUE
directive @attributeRegistry on ENUM
directive @attribute(id: Int!, datatype: DataTypes!, array: Boolean = true, alias: String = null, uri: String = null, description: String = null) on ENUM_VALUE
directive @use(attribute: Attributes!) on FIELD_DEFINITION
directive @unit(id: Int, name: String) on OBJECT # either id or name
directive @record(attribute: Attributes!) on OBJECT

##############  basic data types ############################
enum DataTypes @datatypeRegistry {
    STRING    @datatype(id: 1)
    TIME      @datatype(id: 2)
    INTEGER   @datatype(id: 3)
    LONG      @datatype(id: 4)
    DOUBLE    @datatype(id: 5)
    BOOLEAN   @datatype(id: 6)
    DATA      @datatype(id: 7)
    RECORD    @datatype(id: 99)
}

############## attributes ############################
enum Attributes @attributeRegistry {
    "The name given to the resource. It''s a human-readable identifier that provides a concise representation of the resource''s content."
    title @attribute(id: 1, datatype: STRING, array: false,
        alias: "dce:title", uri: "http://purl.org/dc/elements/1.1/title"   
    ...

    # Some domain specific attributes (used in demo)
    orderId    @attribute(id: 1001, datatype: STRING, name: "dmo:orderId", array: false)
    deadline   @attribute(id: 1002, datatype: TIME,   name: "dmo:deadline", array: false)
    reading    @attribute(id: 1003, datatype: DOUBLE, name: "dmo:reading", array: true) # i.e. capable of handling multiple values
    shipment   @attribute(id: 1099, datatype: RECORD, name: "dmo:shipment", array: false)
    shipmentId @attribute(id: 2000, datatype: STRING, name: "dmo:shipmentId")
}

##############  object & unit types ##############################
scalar Long
scalar DateTime
scalar Bytes    # Base-64 strings (JSON) on the wire

type Shipment @record(attribute: shipment) {
    shipmentId  : String  @use(attribute: shipmentId)
    deadline : DateTime   @use(attribute: deadline)
    reading  : [Float]    @use(attribute: reading)
}

type PurchaseOrder @unit(id: 42) {
    orderId  : String    @use(attribute: orderId)
    shipment : Shipment! @use(attribute: shipment)
}

##############  Query related ####################################

input UnitIdentification {
    tenantId : Int!
    unitId : Long!
}

#
enum Operator { GT, GEQ, EQ, LEQ, LT, LIKE, NEQ }

input AttributeExpression {
    attr  : Attributes!
    op : Operator!
    value : String!
}

enum Logical { AND, OR }

input TreeExpression {
    op : Logical!
    left : Node!
    right : Node!
}

input Node {
    attrExpr : AttributeExpression,
    treeExpr : TreeExpression
}

input Filter {
    tenantId: Int!
    where: Node!
    offset: Int = 0
    size: Int = 20
}

##############  Queries ###########################################
type Query {
    order(id : UnitIdentification!) : PurchaseOrder
    orderRaw(id : UnitIdentification!) : Bytes
    orders(filter: Filter!) : [PurchaseOrder]
    ordersRaw(filter: Filter!) : Bytes
}
```

## Example of usage (in Java on top of GraphQL):
```java
String query = """
    query Units($filter: Filter!) {
      orders(filter: $filter) {
        edges {
          shipment {
            orderId
          }
        }
      }
    }
    """;

Map<String, Object> filter = Map.of(
        "filter", Map.of(
                "tenantId", 1,
                "where", Map.of(
                        "attrExpr", Map.of(
                                "attr", "orderId", // Uses GraphQL SDL naming
                                "op", "EQ",
                                "value", "<order id>"
                        )
                )
        )
);
        
ExecutionResult result = graphQL.execute(
        ExecutionInput.newExecutionInput()
                .query(query)
                .variables(filter)
                .build());

System.out.println((Object) result.getData());
```
Result:
```
{orders={edges=[{shipment={orderId=*order id 2*}}, {shipment={orderId=*order id 2*}}, {shipment={orderId=*order id 2*}}, {shipment={orderId=*order id 2*}}, {shipment={orderId=*order id 2*}}, {shipment={orderId=*order id 2*}}, {shipment={orderId=*order id 2*}}, {shipment={orderId=*order id 2*}}, {shipment={orderId=*order id 2*}}, {shipment={orderId=*order id 2*}}, {shipment={orderId=*order id 2*}}, {shipment={orderId=*order id 2*}}, {shipment={orderId=*order id 2*}}, {shipment={orderId=*order id 2*}}, {shipment={orderId=*order id 2*}}, {shipment={orderId=*order id 2*}}, {shipment={orderId=*order id 2*}}, {shipment={orderId=*order id 2*}}, {shipment={orderId=*order id 2*}}, {shipment={orderId=*order id 2*}}, {shipment={orderId=*order id 2*}}], pageInfo={hasNextPage=false, hasPreviousPage=false, startCursor=0, endCursor=0}}}
```

## Example in Java of creating objects and querying:
```java
    public void createAUnitAndAssignAttributes() {
        Repository repo = RepositoryFactory.getRepository();
        int tenantId = getTenantId("SCRATCH", repo); // SCRATCH is the default space

        // Create a unit, with some random name. Names does not have to be unique
        // as they are not used to identify units.
        Unit unit = repo.createUnit(tenantId, "a shipment instance");

        unit.withAttributeValue("dce:title", String.class, value -> {
            value.add("Shipment information for <order id>");
        });

        unit.withAttributeValue("dmo:orderId", String.class, value -> {
            value.add("<order id>");
        });
    
        unit.withAttribute("dmo:shipment", Attribute.class, attr -> {
            RecordAttribute recrd = new RecordAttribute(attr);

            recrd.withNestedAttributeValue(unit, "dmo:shipmentId", String.class, value -> {
                value.add("<shipment id>");
            });

            recrd.withNestedAttributeValue(unit, "dmo:deadline", Instant.class, value -> {
                value.add(Instant.now()); // brutal :)
            });

            recrd.withNestedAttributeValue(unit, "dmo:reading", Double.class, value -> {
                value.add(Math.PI);
                value.add(Math.E);
            });
        });

        // Store this new unit to database
        repo.storeUnit(unit);
    }

    public void searchForAUnitBasedOnAttributes() {
        Repository repo = RepositoryFactory.getRepository();
        int tenantId = getTenantId("SCRATCH", repo); // SCRATCH is the default space

        // Constraints specified for the unit itself (such as being 'EFFECTIVE')
        SearchExpression expr = new SearchExpression(SearchItem.constrainToSpecificTenant(tenantId));
        expr = SearchExpression.assembleAnd(expr, SearchItem.constrainToSpecificStatus(Unit.Status.EFFECTIVE));

        // Constrain to specific string attribute, in this case 'order ID'
        attributeId = getAttributeId("dmo:orderId", repo);
        SearchItem<String> stringSearchItem = new StringAttributeSearchItem(attributeId, Operator.EQ, "<order id>");
        expr = SearchExpression.assembleAnd(expr, stringSearchItem);

        // 
        SearchOrder order = SearchOrder.getDefaultOrder(); // descending on creation time
 
        // Now we can *either* use canned search (that produces instantiated Unit:s) 
        // *or* search without instantiating Unit objects.
        if (/* use canned search? */ true) {
            SearchResult result = repo.searchUnit(
                    /* paging stuff */ 0, 5, 100,
                    expr, order
            );

            Collection<Unit> units = result.results();
            for (Unit unit : units) {
                System.out.println("Found: " + unit);
            }
            
        } else {
            // Searching without instantiating Unit:s.
            DatabaseAdapter searchAdapter = repo.getDatabaseAdapter();
            UnitSearchData usd = new UnitSearchData(expr, order, /* selectionSize */ 5);

            try {
                repo.withConnection(conn -> searchAdapter.search(conn, usd, repo.getTimingData(), rs -> {
                    while (rs.next()) {
                        int i = 0;
                        int _tenantId = rs.getInt(++i);
                        long _unitId = rs.getLong(++i);
                        Timestamp _created = rs.getTimestamp(++i);

                        System.out.println("Found: tenantId=" + _tenantId + " unitId=" + _unitId + " created=" + _created);
                    }
                }));
            } catch (SQLException sqle) {
                throw new RuntimeException("Could not search: " + Database.squeeze(sqle), sqle);
            }
        }
    }

    /* Utility function, tenant name -> tenant id */
    private int getTenantId(String tenantName, Repository repo) {
        Optional<Integer> tenantId = repo.tenantNameToId(tenantName);
        if (tenantId.isEmpty()) {
            throw new RuntimeException("Unknown tenant: " + tenantName);
        }
        return tenantId.get();
    }

    /* Utility function, attribute name -> attribute id */
    private int getAttributeId(String attributeName, Repository repo) {
        Optional<Integer> attributeId = repo.attributeNameToId(attributeName);
        if (attributeId.isEmpty()) {
            throw new RuntimeException("Unknown attribute: " + attributeName);
        }
        return attributeId.get();
    }
```

## Database
The data model governed by this framework:
![Image](doc/system/images/datamodel.png)

## Setup
The steps needed in order to fetch, start and configure PostgreSQL to run in a Docker container are 
automated in ```db/postgresql/setup-for-test.py```, but you can do this manually (as described in [this README](db/postgresql/README.md)).

```
➜  python db/postgresql/setup-for-test.py
Pulling the latest 'postgres' image from Docker Hub...
Using default tag: latest
latest: Pulling from library/postgres
254e724d7786: Pull complete
...

Image pulled successfully.

Removing existing container named 'repo-postgres' (if any)...
Removed old container 'repo-postgres'.

Starting new container 'repo-postgres' on port 1402 ...
2e8b41855a8976901ed256cba75c6541c3dda188a4cb617f911688c26057212b
Container started.

Waiting 10 seconds for the database to initialize...
Proceeding...

Entering container to run psql commands...
...

Setup completed.  
```
