-module(erepo_graphql_resolvers).

-export([resolve/4]).

resolve(query, unit, Args, _Context) ->
    TenantId = maps:get(tenantid, Args),
    UnitId = maps:get(unitid, Args),
    case erepo:get_unit(TenantId, UnitId) of
        {ok, Unit} -> {ok, Unit};
        not_found -> {ok, null};
        Error -> Error
    end;
resolve(query, units, Args, _Context) ->
    Query = maps:get(query, Args, <<>>),
    Limit = maps:get(limit, Args, 50),
    Offset = maps:get(offset, Args, 0),
    erepo:search_units(Query, {created, desc}, #{limit => Limit, offset => Offset});
resolve(mutation, createUnit, Args, _Context) ->
    TenantId = maps:get(tenantid, Args),
    Name = maps:get(name, Args, undefined),
    case Name of
        undefined ->
            case erepo:create_unit(TenantId) of
                {ok, Unit} -> erepo:store_unit(Unit);
                Error -> Error
            end;
        _ ->
            case erepo:create_unit(TenantId, Name) of
                {ok, Unit} -> erepo:store_unit(Unit);
                Error -> Error
            end
    end;
resolve(mutation, inactivateUnit, Args, _Context) ->
    UnitRef = #{tenantid => maps:get(tenantid, Args), unitid => maps:get(unitid, Args)},
    case erepo:inactivate_unit(UnitRef) of
        ok -> {ok, true};
        Error -> Error
    end;
resolve(mutation, activateUnit, Args, _Context) ->
    UnitRef = #{tenantid => maps:get(tenantid, Args), unitid => maps:get(unitid, Args)},
    case erepo:activate_unit(UnitRef) of
        ok -> {ok, true};
        Error -> Error
    end;
resolve(Type, Field, _Args, _Context) ->
    {error, {unsupported_resolver, {Type, Field}}}.
