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
-module(ipto_graphql_object_resource).

-include("ipto.hrl").

-export([execute/4]).

-spec execute(map(), map(), binary(), map()) -> {ok, graphql_value()}.
execute(_Context, Source, Field, _Args) when is_map(Source), is_binary(Field) ->
    Value =
        case maps:find(Field, Source) of
            {ok, V1} -> V1;
            error ->
                case maps:find(binary_to_atom(Field, utf8), Source) of
                    {ok, V2} -> V2;
                    error -> null
                end
        end,
    {ok, Value};
execute(_Context, _Source, _Field, _Args) ->
    {ok, null}.
