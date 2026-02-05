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
-module(erepo_graphql_config_tests).

-include_lib("eunit/include/eunit.hrl").

schema_override_from_sdl_test() ->
    ok = erepo_graphql_config:set_schema_sdl("type Query { ping: String }"),
    Bin = erepo_graphql_config:get_schema(),
    true = binary:match(Bin, <<"ping">>) =/= nomatch,
    ok = erepo_graphql_config:clear_schema_overrides().

schema_override_from_file_test() ->
    Tmp = filename:join([os:getenv("TMPDIR", "/tmp"), "erepo-graphql-schema.graphql"]),
    ok = file:write_file(Tmp, <<"type Query { pong: String }">>),
    ok = erepo_graphql_config:set_schema_file(Tmp),
    Bin = erepo_graphql_config:get_schema(),
    true = binary:match(Bin, <<"pong">>) =/= nomatch,
    ok = erepo_graphql_config:clear_schema_overrides(),
    _ = file:delete(Tmp),
    ok.
