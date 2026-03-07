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
-module(ipto_rel).

-include("ipto.hrl").

-export([add/3, remove/3, get_right/2, get_left/2, get_right_one/2, count_right/2, count_left/2]).

-spec add(unit_ref_value(), relation_type(), unit_ref_value()) -> ok | {error, ipto_reason()}.
add(UnitRef, RelType, OtherUnitRef) ->
    ipto_db:add_relation(UnitRef, RelType, OtherUnitRef).

-spec remove(unit_ref_value(), relation_type(), unit_ref_value()) -> ok | {error, ipto_reason()}.
remove(UnitRef, RelType, OtherUnitRef) ->
    ipto_db:remove_relation(UnitRef, RelType, OtherUnitRef).

-spec get_right(unit_ref_value(), relation_type()) -> ipto_result([relation()]).
get_right(UnitRef, RelType) ->
    ipto_db:get_right_relations(UnitRef, RelType).

-spec get_left(unit_ref_value(), relation_type()) -> ipto_result([relation()]).
get_left(UnitRef, RelType) ->
    ipto_db:get_left_relations(UnitRef, RelType).

-spec get_right_one(unit_ref_value(), relation_type()) -> relation_lookup_result().
get_right_one(UnitRef, RelType) ->
    ipto_db:get_right_relation(UnitRef, RelType).

-spec count_right(unit_ref_value(), relation_type()) -> ipto_result(non_neg_integer()).
count_right(UnitRef, RelType) ->
    ipto_db:count_right_relations(UnitRef, RelType).

-spec count_left(unit_ref_value(), relation_type()) -> ipto_result(non_neg_integer()).
count_left(UnitRef, RelType) ->
    ipto_db:count_left_relations(UnitRef, RelType).
