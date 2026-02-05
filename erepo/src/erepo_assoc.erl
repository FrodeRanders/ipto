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
-module(erepo_assoc).

-include("erepo.hrl").

-export([add/3, remove/3]).

-spec add(unit_ref_value(), association_type(), ref_string()) -> ok | {error, erepo_reason()}.
add(UnitRef, AssocType, RefString) ->
    erepo_db:add_association(UnitRef, AssocType, RefString).

-spec remove(unit_ref_value(), association_type(), ref_string()) -> ok | {error, erepo_reason()}.
remove(UnitRef, AssocType, RefString) ->
    erepo_db:remove_association(UnitRef, AssocType, RefString).
