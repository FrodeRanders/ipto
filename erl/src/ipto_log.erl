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
-module(ipto_log).

-export([configure/0, debug/3, info/3, notice/3, warning/3, error/3]).

-type log_level() :: debug | info | notice | warning | error.

-spec configure() -> ok.
configure() ->
    maybe_set_primary_level(),
    maybe_set_file_handler(),
    ok.

-spec debug(atom(), io:format(), [term()]) -> ok.
debug(Component, Format, Args) ->
    log(debug, Component, Format, Args).

-spec info(atom(), io:format(), [term()]) -> ok.
info(Component, Format, Args) ->
    log(info, Component, Format, Args).

-spec notice(atom(), io:format(), [term()]) -> ok.
notice(Component, Format, Args) ->
    log(notice, Component, Format, Args).

-spec warning(atom(), io:format(), [term()]) -> ok.
warning(Component, Format, Args) ->
    log(warning, Component, Format, Args).

-spec error(atom(), io:format(), [term()]) -> ok.
error(Component, Format, Args) ->
    log(error, Component, Format, Args).

-spec log(log_level(), atom(), io:format(), [term()]) -> ok.
log(Level, Component, Format, Args) ->
    _ = logger:log(Level, Format, Args, #{app => ipto, component => Component}),
    ok.

-spec maybe_set_primary_level() -> ok.
maybe_set_primary_level() ->
    case configured_level() of
        undefined ->
            ok;
        Level ->
            _ = logger:set_primary_config(level, Level),
            ok
    end.

-spec maybe_set_file_handler() -> ok.
maybe_set_file_handler() ->
    case configured_file() of
        undefined ->
            ok;
        Path ->
            FileLevel = configured_file_level(),
            HandlerConfig = # {
                level => FileLevel,
                sync_mode_qlen => 0,
                formatter => {logger_formatter, #{single_line => true}},
                config => # {
                    type => {file, Path},
                    modes => [append]
                }
            },
            case logger:add_handler(ipto_file, logger_std_h, HandlerConfig) of
                ok ->
                    ok;
                {error, {already_exist, ipto_file}} ->
                    _ = logger:update_handler_config(ipto_file, HandlerConfig),
                    ok;
                {error, _} ->
                    ok
            end
    end.

-spec configured_level() -> log_level() | undefined.
configured_level() ->
    case os:getenv("IPTO_LOG_LEVEL") of
        false ->
            case application:get_env(ipto, log_level) of
                {ok, Value} -> parse_level(Value, undefined);
                undefined -> undefined
            end;
        Value ->
            parse_level(Value, undefined)
    end.

-spec configured_file() -> string() | undefined.
configured_file() ->
    case os:getenv("IPTO_LOG_FILE") of
        false ->
            case application:get_env(ipto, log_file) of
                {ok, undefined} -> undefined;
                {ok, Value} -> to_string(Value);
                undefined -> undefined
            end;
        Value ->
            to_string(Value)
    end.

-spec configured_file_level() -> log_level().
configured_file_level() ->
    case os:getenv("IPTO_LOG_FILE_LEVEL") of
        false ->
            case application:get_env(ipto, log_file_level) of
                {ok, Value} -> parse_level(Value, info);
                undefined -> info
            end;
        Value ->
            parse_level(Value, info)
    end.

-spec parse_level(term(), log_level() | undefined) -> log_level() | undefined.
parse_level(Value, Default) when is_atom(Value) ->
    case Value of
        debug -> debug;
        info -> info;
        notice -> notice;
        warning -> warning;
        error -> error;
        _ -> Default
    end;
parse_level(Value, Default) ->
    Normalized = string:lowercase(string:trim(to_string(Value))),
    case Normalized of
        "debug" -> debug;
        "info" -> info;
        "notice" -> notice;
        "warning" -> warning;
        "error" -> error;
        _ -> Default
    end.

-spec to_string(term()) -> string().
to_string(Value) when is_binary(Value) ->
    binary_to_list(Value);
to_string(Value) when is_list(Value) ->
    Value;
to_string(Value) when is_atom(Value) ->
    atom_to_list(Value);
to_string(Value) ->
    lists:flatten(io_lib:format("~p", [Value])).
