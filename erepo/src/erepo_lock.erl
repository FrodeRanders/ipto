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
-module(erepo_lock).

-include("erepo.hrl").

-export([lock/3, unlock/1]).

-spec lock(unit_ref_value(), lock_type(), ref_string()) -> ok | already_locked | {error, erepo_reason()}.
lock(UnitRef, LockType, Purpose) ->
    erepo_db:lock_unit(UnitRef, LockType, Purpose).

-spec unlock(unit_ref_value()) -> ok | {error, erepo_reason()}.
unlock(UnitRef) ->
    erepo_db:unlock_unit(UnitRef).
