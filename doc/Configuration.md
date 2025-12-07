## Configuration


### Base configuration

This system comes with a base setup of attribute types, which in various combinations are used to model information.

Since we chose to base the configuration language on GraphQL SDL (a choice among several possible alternatives), 
we provide a set of directives and enumerations that are used later when formulating the domain specific attributes
and types for your application.

These are the directives we provide:
```graphql
#
# Directives used and understood in this configuration setting, as annotations to
# GraphQL SDL.
#
directive @datatypeRegistry on ENUM
directive @datatype(id: Int!) on ENUM_VALUE
directive @attributeRegistry on ENUM
directive @attribute(datatype: DataTypes!, array: Boolean = true, name: String = null, uri: String = null, description: String = null) on ENUM_VALUE
directive @use(attribute: Attributes!) on FIELD_DEFINITION
directive @unit(name: String) on OBJECT # either id or name
directive @record(attribute: Attributes!) on OBJECT
```

These are the attribute types we provide:
```graphql
#
# These are the currently implemented attribute types, in terms of primitives (STRING, TIME, INTEGER, ...)
# but also in terms of records of primitives or nested records (RECORD)
#
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

scalar Long
scalar DateTime
scalar Bytes    # Base-64 strings (JSON) on the wire
```
The ```RECORD``` attribute type refers to a nested structure of attributes within a type (or attribute), 
so that you for instance can refer to an address as a whole as related to a Person or Company.

Examples will follow.

### Domain specific configuration

Within a specific domain, any number of attributes has to be defined in o order to model relevant information.
Attributes are combined into types, so you may choose to work top down or bottom up: defining types first and picking
attributes later, or defining attributes first and combining them into types respectively.

Attributes are defined as an enumeration, but we also need to annotate this enumeration with relevant
metadata for the backing management system.

Attributes are identified with different names at different levels. At the backing management level, by
a namespaced name (and the corresponding qualified name). Take for instance the attribute ```title``` from
the Dublic Core Elements namespace ```http://purl.org/dc/elements/1.1/```. This attribute is identified
by the name ```dce:title```, when operating through the Java API on top of the management system, and as
such globally understood as "The name given to a resource, as a human-readable identifier that provides a 
concise representation of the resource's content." 

Choosing to use this attribute, identifies both the attribute value itself as well as the function of the 
attribute value within your application domain.

You can choose to add other well known vocabularies as a base for your data models.

```graphql
#
# This enumeration lists attributes available in the system, and when used in 
# later type definitions will be validated against this enumeration.
#
enum Attributes @attributeRegistry {
    "The name given to the resource. It''s a human-readable identifier that provides a concise representation of the resource''s content."
    title @attribute(datatype: STRING, array: false,
        name: "dce:title", uri: "http://purl.org/dc/elements/1.1/title"   
    ...

    # Domain specific attributes (e.g. related to orders and shipments)
    orderId    @attribute(datatype: STRING, name: "biz:orderId", uri: "http://biz.com/namespace/order-id")
    deadline   @attribute(datatype: TIME,   name: "biz:deadline", uri: "http://biz.com/namespace/deadline")
    shipment   @attribute(datatype: RECORD, name: "biz:shipment", uri: "http://biz.com/namespace/shipment")
    shipmentId @attribute(datatype: STRING, name: "biz:shipmentId", uri: "http://biz.com/namespace/shipment-id")
}
```
These attributes are then used to define domain models, by the way of ```type```. We annotate the type-definition
to indicate whether the object has a life-cycle of its own (i.e. it is a ```unit```) or whether it is a
compoundable record, that can be aggregated into other records or units.

```graphql
#
# This represents a sample 'shipment'...
#
type Shipment @record(attribute: shipment) {
    shipmentId  : String  @use(attribute: shipmentId)
    deadline : DateTime   @use(attribute: deadline)
}

#
# ...and shipments are related to purchase orders
#
type PurchaseOrder @unit {
    orderId  : String    @use(attribute: orderId)
    shipment : Shipment! @use(attribute: shipment)
}
```
On top of these, GraphQL queries and mutations can be used to retrieve or store objects using these
defined types.

Units are identified by ```UnitIdentification```, so by using a query that takes that parameter objects
of the relevant type will be retrieved. 

It is also possible to provide a ```Filter``` for searching on individual attributes, using an expression 
tree ```TreeExpression``` with a combination of ```AttributeExpression```, ```Operator``` and string values. 

```graphql
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
```
The declared queries are automatically handled, based on declared parameters and return types.

```graphql
#
# These queries alternates between a GraphQL type of return or Bytes (effectively JSON)
#
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
            shipmentId
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
Prints:
```
{orders={edges=[{shipment={shipmentId=<order id>}}]}}
```

### Test and demo configuration

For testing and demo purposes, we ship this [GraphQL SDL](../repo/src/test/resources/org/gautelis/repo/schema2.graphqls).
