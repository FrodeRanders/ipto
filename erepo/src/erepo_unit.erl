-module(erepo_unit).

-include("erepo.hrl").

-export([new/3, to_persist_map/1, make_corrid/0]).

new(TenantId, UnitName, CorrId) ->
    #unit{
        tenantid = TenantId,
        unitname = UnitName,
        corrid = CorrId
    }.

to_persist_map(#unit{
    tenantid = TenantId,
    unitid = UnitId,
    unitver = UnitVer,
    corrid = CorrId,
    status = Status,
    unitname = UnitName,
    attributes = Attributes
}) ->
    Base = #{
        tenantid => TenantId,
        unitver => UnitVer,
        corrid => CorrId,
        status => Status,
        attributes => Attributes
    },
    WithUnitId =
        case UnitId of
            undefined -> Base;
            _ -> Base#{unitid => UnitId}
        end,
    case UnitName of
        undefined -> WithUnitId;
        _ -> WithUnitId#{unitname => UnitName}
    end.

make_corrid() ->
    %% UUIDv4 textual representation. PG expects valid UUID input.
    <<A:32, B:16, C0:16, D0:16, E:48>> = crypto:strong_rand_bytes(16),
    C = (C0 band 16#0FFF) bor 16#4000,  %% version 4
    D = (D0 band 16#3FFF) bor 16#8000,  %% RFC4122 variant
    iolist_to_binary(
      io_lib:format("~8.16.0b-~4.16.0b-~4.16.0b-~4.16.0b-~12.16.0b", [A, B, C, D, E])
    ).
