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
-module(ipto_graphql_adapter).

-include("ipto.hrl").

-export([execute/3, reload_schema/0]).

-define(STATE_KEY, {?MODULE, state}).

-spec execute(binary() | string(), map(), map()) -> ipto_result(map()).
execute(Query, Variables, Context) ->
    case code:ensure_loaded(graphql) of
        {module, graphql} ->
            execute_with_graphql_erl(Query, Variables, Context);
        _ ->
            {error, {missing_dependency, graphql_erl}}
    end.

-spec reload_schema() -> ok.
reload_schema() ->
    ok = ipto_graphql_config:reload(),
    persistent_term:erase(?STATE_KEY),
    ok.

execute_with_graphql_erl(Query, Variables, Context0) ->
    case ensure_graphql_started() of
        ok -> continue_execute(Query, Variables, Context0);
        Error -> Error
    end.

continue_execute(Query, Variables, Context0) ->
    case ensure_schema_loaded() of
        ok ->
            QueryText = to_binary(Query),
            case graphql:parse(QueryText) of
                {ok, AST} ->
                    {ok, #{fun_env := FunEnv, ast := AST2}} = graphql:type_check(AST),
                    ok = graphql:validate(AST2),
                    {OpName, Vars} = extract_operation_and_vars(Variables),
                    Coerced = graphql:type_check_params(FunEnv, OpName, Vars),
                    Context = Context0#{params => Coerced, operation_name => OpName},
                    {ok, graphql:execute(Context, AST2)};
                ParseError ->
                    ParseError
            end;
        Error ->
            Error
    end.

ensure_graphql_started() ->
    case application:ensure_all_started(graphql) of
        {ok, _} -> ok;
        {error, {already_started, graphql}} -> ok;
        {error, {already_started, _Other}} -> ok;
        Error -> {error, {graphql_start_failed, Error}}
    end.

ensure_schema_loaded() ->
    SchemaData = ipto_graphql_config:get_schema(),
    Mapping = ipto_graphql_config:get_mapping(),
    Digest = erlang:phash2({SchemaData, Mapping}),

    case persistent_term:get(?STATE_KEY, undefined) of
        #{digest := Digest} ->
            ok;
        _ ->
            load_schema(SchemaData, Mapping, Digest)
    end.

load_schema(SchemaData, Mapping, Digest) ->
    ok = graphql_schema:reset(),
    case graphql:load_schema(Mapping, SchemaData) of
        ok ->
            Root = {root, #{query => 'Query', mutation => 'Mutation', interfaces => []}},
            _ = graphql:insert_schema_definition(Root),
            ok = graphql:validate_schema(),
            persistent_term:put(?STATE_KEY, #{digest => Digest}),
            ok;
        Error ->
            {error, {graphql_schema_load_failed, Error}}
    end.

extract_operation_and_vars(Variables) when is_map(Variables) ->
    OpName = maps:get(operation_name, Variables, maps:get(<<"operationName">>, Variables, undefined)),
    Params =
        case maps:get(params, Variables, maps:get(<<"variables">>, Variables, Variables)) of
            V when is_map(V) -> V;
            _ -> #{}
        end,
    {OpName, Params};
extract_operation_and_vars(_Variables) ->
    {undefined, #{}}.

to_binary(Value) when is_binary(Value) ->
    Value;
to_binary(Value) when is_list(Value) ->
    unicode:characters_to_binary(Value);
to_binary(Value) ->
    unicode:characters_to_binary(io_lib:format("~p", [Value])).
