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
-module(ipto_unit).

-include("ipto.hrl").

-export([new/3, to_persist_map/1, make_corrid/0]).

%% --------------------------------------------------------------------
%% new/3
%%
%% Creates a transient unit record.
%% --------------------------------------------------------------------
-spec new(tenantid(), unit_name() | undefined, corrid()) -> #unit{}.
new(TenantId, UnitName, CorrId) ->
    #unit{
        tenantid = TenantId,
        unitname = UnitName,
        corrid = CorrId
    }.

-spec to_persist_map(#unit{}) -> unit_map().
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

-spec make_corrid() -> corrid().
make_corrid() ->
    %% UUIDv7 textual representation: sortable by unix epoch milliseconds.
    TsMs0 = erlang:system_time(millisecond),
    TsMs = TsMs0 band 16#FFFFFFFFFFFF,
    <<R0:80>> = crypto:strong_rand_bytes(10),
    RandA = (R0 bsr 68) band 16#FFF,
    RandB = (R0 bsr 6) band 16#3FFFFFFFFFFFFFFF,
    <<A:32, B:16, C:16, D:16, E:48>> = <<TsMs:48, 7:4, RandA:12, 2:2, RandB:62>>,
    iolist_to_binary(
      io_lib:format("~8.16.0b-~4.16.0b-~4.16.0b-~4.16.0b-~12.16.0b", [A, B, C, D, E])
    ).
