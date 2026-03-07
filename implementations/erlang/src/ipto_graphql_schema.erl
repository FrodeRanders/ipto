%%% Copyright (C) 2026 Frode Randers
%%% All rights reserved
%%%
%%% This file is part of IPTO.
%%%
%%% Licensed under the Apache License, Version 2.0 (the "License");
%%% you may not use this file except in compliance with the License.
%%% You may obtain a copy of the License at
%%%
%%%    http://www.apache.org/licenses/LICENSE-2.0
%%%
%%% Unless required by applicable law or agreed to in writing, software
%%% distributed under the License is distributed on an "AS IS" BASIS,
%%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%% See the License for the specific language governing permissions and
%%% limitations under the License.
%%%
-module(ipto_graphql_schema).

-export([schema/0]).

-spec schema() -> binary().
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
