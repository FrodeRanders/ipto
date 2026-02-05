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

-export([add/3, remove/3, get_right/2, get_left/2, get_right_one/2, count_right/2, count_left/2]).

-spec add(unit_ref_value(), association_type(), ref_string()) -> ok | {error, erepo_reason()}.
add(UnitRef, AssocType, RefString) ->
    erepo_db:add_association(UnitRef, AssocType, RefString).

-spec remove(unit_ref_value(), association_type(), ref_string()) -> ok | {error, erepo_reason()}.
remove(UnitRef, AssocType, RefString) ->
    erepo_db:remove_association(UnitRef, AssocType, RefString).

-spec get_right(unit_ref_value(), association_type()) -> erepo_result([association()]).
get_right(UnitRef, AssocType) ->
    erepo_db:get_right_associations(UnitRef, AssocType).

-spec get_left(association_type(), ref_string()) -> erepo_result([association()]).
get_left(AssocType, RefString) ->
    erepo_db:get_left_associations(AssocType, RefString).

-spec get_right_one(unit_ref_value(), association_type()) -> association_lookup_result().
get_right_one(UnitRef, AssocType) ->
    erepo_db:get_right_association(UnitRef, AssocType).

-spec count_right(unit_ref_value(), association_type()) -> erepo_result(non_neg_integer()).
count_right(UnitRef, AssocType) ->
    erepo_db:count_right_associations(UnitRef, AssocType).

-spec count_left(association_type(), ref_string()) -> erepo_result(non_neg_integer()).
count_left(AssocType, RefString) ->
    erepo_db:count_left_associations(AssocType, RefString).
