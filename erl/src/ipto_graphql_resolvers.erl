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
-module(ipto_graphql_resolvers).

-include("ipto.hrl").

-export([resolve/4]).

-spec resolve(query | mutation, atom(), map(), map()) ->
    {ok, unit_map() | search_result() | boolean() | null} | {error, ipto_reason()}.
resolve(query, unit, Args, _Context) ->
    TenantId = maps:get(tenantid, Args),
    UnitId = maps:get(unitid, Args),
    case ipto:get_unit(TenantId, UnitId) of
        {ok, Unit} -> {ok, Unit};
        not_found -> {ok, null};
        Error -> Error
    end;
resolve(query, units, Args, _Context) ->
    Query = maps:get(query, Args, <<>>),
    Limit = maps:get(limit, Args, 50),
    Offset = maps:get(offset, Args, 0),
    ipto:search_units(Query, {created, desc}, #{limit => Limit, offset => Offset});
resolve(mutation, createUnit, Args, _Context) ->
    TenantId = maps:get(tenantid, Args),
    Name = maps:get(name, Args, undefined),
    case Name of
        undefined ->
            case ipto:create_unit(TenantId) of
                {ok, Unit} -> ipto:store_unit(Unit);
                Error -> Error
            end;
        _ ->
            case ipto:create_unit(TenantId, Name) of
                {ok, Unit} -> ipto:store_unit(Unit);
                Error -> Error
            end
    end;
resolve(mutation, inactivateUnit, Args, _Context) ->
    UnitRef = #{tenantid => maps:get(tenantid, Args), unitid => maps:get(unitid, Args)},
    case ipto:inactivate_unit(UnitRef) of
        ok -> {ok, true};
        Error -> Error
    end;
resolve(mutation, activateUnit, Args, _Context) ->
    UnitRef = #{tenantid => maps:get(tenantid, Args), unitid => maps:get(unitid, Args)},
    case ipto:activate_unit(UnitRef) of
        ok -> {ok, true};
        Error -> Error
    end;
resolve(Type, Field, _Args, _Context) ->
    {error, {unsupported_resolver, {Type, Field}}}.
