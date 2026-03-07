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
-module(ipto_graphql_sdl_tests).

-include_lib("eunit/include/eunit.hrl").

parse_valid_sdl_catalog_test() ->
    Sdl = <<
        "directive @attributeRegistry on ENUM\n"
        "directive @attribute(datatype: String) on ENUM_VALUE\n"
        "directive @record(attribute: Attributes!) on OBJECT\n"
        "directive @template(name: String) on OBJECT\n"
        "directive @use(attribute: Attributes!) on FIELD_DEFINITION\n"
        "enum Attributes @attributeRegistry {\n"
        "  person @attribute(datatype: \"RECORD\")\n"
        "  name @attribute(datatype: \"STRING\")\n"
        "  age @attribute(datatype: \"INTEGER\")\n"
        "}\n"
        "type Person @record(attribute: person) @template(name: \"person-template\") {\n"
        "  fullName: String @use(attribute: name)\n"
        "  years: Int @use(attribute: age)\n"
        "}\n"
    >>,
    {ok, Catalog} = ipto_graphql_sdl:parse(Sdl),
    ?assertEqual(3, maps:get(attributes, maps:get(counts, Catalog))),
    ?assertEqual(1, maps:get(records, maps:get(counts, Catalog))),
    ?assertEqual(1, maps:get(templates, maps:get(counts, Catalog))),
    ?assertEqual([], maps:get(errors, Catalog)).

parse_duplicate_attribute_fails_test() ->
    Sdl = <<
        "enum Attributes @attributeRegistry {\n"
        "  person @attribute(datatype: \"RECORD\")\n"
        "  person @attribute(datatype: \"RECORD\")\n"
        "}\n"
    >>,
    {error, {invalid_sdl, Errors, _Catalog}} = ipto_graphql_sdl:parse(Sdl),
    true = has_error_code(Errors, duplicate_attribute).

parse_unresolved_record_attribute_fails_test() ->
    Sdl = <<
        "enum Attributes @attributeRegistry {\n"
        "  name @attribute(datatype: \"STRING\")\n"
        "}\n"
        "type Person @record(attribute: person) {\n"
        "  fullName: String @use(attribute: name)\n"
        "}\n"
    >>,
    {error, {invalid_sdl, Errors, _Catalog}} = ipto_graphql_sdl:parse(Sdl),
    true = has_error_code(Errors, unresolved_record_attribute).

parse_unresolved_use_attribute_fails_test() ->
    Sdl = <<
        "enum Attributes @attributeRegistry {\n"
        "  person @attribute(datatype: \"RECORD\")\n"
        "}\n"
        "type Person @record(attribute: person) {\n"
        "  fullName: String @use(attribute: name)\n"
        "}\n"
    >>,
    {error, {invalid_sdl, Errors, _Catalog}} = ipto_graphql_sdl:parse(Sdl),
    true = has_error_code(Errors, unresolved_use_attribute).

inspect_returns_catalog_even_when_invalid_test() ->
    Sdl = <<"type Person @record(attribute: person) { fullName: String @use(attribute: name) }">>,
    Catalog = ipto_graphql_sdl:inspect(Sdl),
    Errors = maps:get(errors, Catalog),
    true = has_error_code(Errors, missing_attribute_registry),
    ?assertEqual(0, maps:get(attributes, maps:get(counts, Catalog))).

configure_graphql_sdl_creates_and_reuses_attributes_test() ->
    application:set_env(ipto, backend, memory),
    {ok, _} = ipto:start_link(),
    Sdl = <<
        "enum Attributes @attributeRegistry {\n"
        "  person @attribute(datatype: RECORD, array: false, name: \"demo:person\")\n"
        "  name @attribute(datatype: STRING, array: false, name: \"demo:name\")\n"
        "}\n"
        "type Person @record(attribute: person) @template(name: \"person-template\") {\n"
        "  fullName: String @use(attribute: name)\n"
        "}\n"
    >>,
    {ok, First} = ipto:configure_graphql_sdl(Sdl),
    AttrSummary1 = maps:get(attributes, First),
    ?assertEqual(2, maps:get(created, AttrSummary1)),
    ?assertEqual(0, maps:get(existing, AttrSummary1)),
    {ok, _NameAttr} = ipto:get_attribute_info(<<"demo:name">>),

    {ok, Second} = ipto:configure_graphql_sdl(Sdl),
    AttrSummary2 = maps:get(attributes, Second),
    ?assertEqual(0, maps:get(created, AttrSummary2)),
    ?assertEqual(2, maps:get(existing, AttrSummary2)),
    ?assertEqual(false, maps:get(supported, maps:get(records, Second))),
    ?assertEqual(false, maps:get(supported, maps:get(templates, Second))).

configure_graphql_sdl_invalid_returns_error_test() ->
    application:set_env(ipto, backend, memory),
    {ok, _} = ipto:start_link(),
    Sdl = <<
        "enum Attributes @attributeRegistry {\n"
        "  person @attribute(datatype: RECORD, array: false, name: \"demo:person\")\n"
        "}\n"
        "type Person @record(attribute: person) {\n"
        "  fullName: String @use(attribute: missing)\n"
        "}\n"
    >>,
    {error, {invalid_sdl, Errors}} = ipto:configure_graphql_sdl(Sdl),
    true = has_error_code(Errors, unresolved_use_attribute).

configure_graphql_sdl_file_test() ->
    application:set_env(ipto, backend, memory),
    {ok, _} = ipto:start_link(),
    Tmp = filename:join([os:getenv("TMPDIR", "/tmp"), "ipto-configure-sdl.graphql"]),
    Sdl = <<
        "enum Attributes @attributeRegistry {\n"
        "  thing @attribute(datatype: STRING, array: false, name: \"demo:file:thing\")\n"
        "}\n"
    >>,
    ok = file:write_file(Tmp, Sdl),
    try
        {ok, Summary} = ipto:configure_graphql_sdl_file(Tmp),
        AttrSummary = maps:get(attributes, Summary),
        ?assertEqual(1, maps:get(created, AttrSummary))
    after
        _ = file:delete(Tmp)
    end.

-spec has_error_code([map()], atom()) -> boolean().
has_error_code(Errors, Code) ->
    lists:any(fun(E) -> maps:get(code, E, undefined) =:= Code end, Errors).
