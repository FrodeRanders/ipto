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
-module(ipto_backend).

-include("ipto.hrl").

-callback get_unit_json(tenantid(), unitid(), version_selector()) -> unit_lookup_result().
-callback unit_exists(tenantid(), unitid()) -> boolean().
-callback store_unit_json(unit_map()) -> ipto_result(unit_map()).
-callback search_units(search_expression() | map(), search_order(), search_paging()) -> ipto_result(search_result()).
-callback add_relation(unit_ref_value(), relation_type(), unit_ref_value()) -> ok | {error, ipto_reason()}.
-callback remove_relation(unit_ref_value(), relation_type(), unit_ref_value()) -> ok | {error, ipto_reason()}.
-callback get_right_relation(unit_ref_value(), relation_type()) -> relation_lookup_result().
-callback get_right_relations(unit_ref_value(), relation_type()) -> ipto_result([relation()]).
-callback get_left_relations(unit_ref_value(), relation_type()) -> ipto_result([relation()]).
-callback count_right_relations(unit_ref_value(), relation_type()) -> ipto_result(non_neg_integer()).
-callback count_left_relations(unit_ref_value(), relation_type()) -> ipto_result(non_neg_integer()).
-callback add_association(unit_ref_value(), association_type(), ref_string()) -> ok | {error, ipto_reason()}.
-callback remove_association(unit_ref_value(), association_type(), ref_string()) -> ok | {error, ipto_reason()}.
-callback get_right_association(unit_ref_value(), association_type()) -> association_lookup_result().
-callback get_right_associations(unit_ref_value(), association_type()) -> ipto_result([association()]).
-callback get_left_associations(association_type(), ref_string()) -> ipto_result([association()]).
-callback count_right_associations(unit_ref_value(), association_type()) -> ipto_result(non_neg_integer()).
-callback count_left_associations(association_type(), ref_string()) -> ipto_result(non_neg_integer()).
-callback lock_unit(unit_ref_value(), lock_type(), ref_string()) -> ok | already_locked | {error, ipto_reason()}.
-callback unlock_unit(unit_ref_value()) -> ok | {error, ipto_reason()}.
-callback set_status(unit_ref_value(), unit_status()) -> ok | {error, ipto_reason()}.
-callback create_attribute(attribute_alias(), attribute_name(), attribute_qualname(), attribute_type(), boolean()) ->
    ipto_result(attribute_info()).
-callback get_attribute_info(name_or_id()) -> {ok, attribute_info()} | not_found | {error, ipto_reason()}.
-callback get_tenant_info(name_or_id()) -> {ok, tenant_info()} | not_found | {error, ipto_reason()}.
