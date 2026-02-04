-module(erepo_graphql_schema).

-export([schema/0]).

schema() ->
    <<
        "schema {\n"
        "  query: Query\n"
        "  mutation: Mutation\n"
        "}\n\n"
        "type Query {\n"
        "  unit(tenantid: Int!, unitid: Int!): Unit\n"
        "  units(query: String, limit: Int, offset: Int): SearchResult!\n"
        "}\n\n"
        "type Mutation {\n"
        "  createUnit(tenantid: Int!, name: String): Unit!\n"
        "  inactivateUnit(tenantid: Int!, unitid: Int!): Boolean!\n"
        "  activateUnit(tenantid: Int!, unitid: Int!): Boolean!\n"
        "}\n\n"
        "type SearchResult {\n"
        "  total: Int!\n"
        "  results: [Unit!]!\n"
        "}\n\n"
        "type Unit {\n"
        "  tenantid: Int!\n"
        "  unitid: Int!\n"
        "  unitver: Int!\n"
        "  status: Int!\n"
        "  unitname: String\n"
        "  corrid: String\n"
        "}\n"
    >>.
