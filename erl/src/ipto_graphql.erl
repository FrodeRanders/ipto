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
-module(ipto_graphql).

-include("ipto.hrl").

-export([
    schema/0,
    build_context/1,
    execute/2,
    execute/3,
    reload_schema/0
]).

-spec schema() -> binary().
schema() ->
    ipto_graphql_schema:schema().

-spec build_context(map()) -> map().
build_context(Options) when is_map(Options) ->
    #{
        tenantid => maps:get(tenantid, Options, undefined),
        authz => maps:get(authz, Options, undefined),
        meta => maps:get(meta, Options, #{})
    };
build_context(_Options) ->
    #{tenantid => undefined, authz => undefined, meta => #{}}.

-spec execute(binary() | string(), map()) -> ipto_result(map()).
execute(Query, Variables) ->
    Context = build_context(#{}),
    execute(Query, Variables, Context).

-spec execute(binary() | string(), map(), map()) -> ipto_result(map()).
execute(Query, Variables, Context) ->
    ipto_graphql_adapter:execute(Query, Variables, Context).

-spec reload_schema() -> ok.
reload_schema() ->
    ipto_graphql_adapter:reload_schema().
