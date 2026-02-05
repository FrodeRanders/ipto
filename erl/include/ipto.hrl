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
-ifndef(IPTO_HRL).
-define(IPTO_HRL, true).

%%-------------------------------------------------------------------
%% Unit lifecycle status values
%%-------------------------------------------------------------------
-define(STATUS_PENDING_DISPOSITION, 1).
-define(STATUS_PENDING_DELETION, 10).
-define(STATUS_OBLITERATED, 20).
-define(STATUS_EFFECTIVE, 30).
-define(STATUS_ARCHIVED, 40).

%%-------------------------------------------------------------------
%% Shared types
%%-------------------------------------------------------------------
-type tenantid() :: pos_integer().
-type unitid() :: pos_integer().
-type unitver() :: pos_integer().
-type unit_status() :: integer().
-type version_selector() :: latest | integer().
-type corrid() :: binary().
-type unit_name() :: binary().
-type unit_attribute() :: map() | tuple().
-type unit_attributes() :: [unit_attribute()].
-type unit_ref_tuple() :: {tenantid(), unitid()}.
-type unit_ref_map() :: #{tenantid := tenantid(), unitid := unitid()}.
-type unit_map() :: map().
-type name_or_id() :: binary() | pos_integer().
-type relation_type() :: integer().
-type association_type() :: integer().
-type lock_type() :: integer().
-type ref_string() :: binary() | string().
-type relation() :: #{
    tenantid := tenantid(),
    unitid := unitid(),
    reltype := relation_type(),
    reltenantid := tenantid(),
    relunitid := unitid()
}.
-type association() :: #{
    tenantid := tenantid(),
    unitid := unitid(),
    assoctype := association_type(),
    assocstring := binary()
}.
-type relation_lookup_result() :: {ok, relation()} | not_found | {error, ipto_reason()}.
-type association_lookup_result() :: {ok, association()} | not_found | {error, ipto_reason()}.
-type search_expression() :: map() | binary() | string().
-type search_order() :: {atom(), asc | desc} | map().
-type search_paging() :: pos_integer() | #{limit => non_neg_integer(), offset => non_neg_integer()}.
-type search_result() :: #{results := [unit_map()], total := non_neg_integer()}.
-type attribute_alias() :: binary() | undefined.
-type attribute_name() :: binary().
-type attribute_qualname() :: binary().
-type attribute_type() :: integer().
-type attribute_info() :: map().
-type tenant_info() :: map().
-type ipto_reason() :: atom() | tuple().
-type ipto_result(T) :: {ok, T} | {error, ipto_reason()}.
-type unit_lookup_result() :: {ok, unit_map()} | not_found | {error, ipto_reason()}.
-type graphql_scalar() :: binary() | string() | integer() | float() | boolean() | null.
-type graphql_value() :: graphql_scalar() | [graphql_value()] | #{atom() | binary() => graphql_value()}.

%%-------------------------------------------------------------------
%% Public data structures
%%-------------------------------------------------------------------
-record(unit_ref, {
    tenantid :: integer(),
    unitid :: integer(),
    unitver :: integer() | undefined
}).

-record(unit, {
    tenantid :: integer(),
    unitid :: integer() | undefined,
    unitver = 1 :: integer(),
    corrid :: binary(),
    status = ?STATUS_EFFECTIVE :: integer(),
    unitname = undefined :: binary() | undefined,
    attributes = [] :: list(),
    created = undefined :: undefined | binary(),
    modified = undefined :: undefined | binary(),
    is_new = true :: boolean(),
    is_read_only = false :: boolean()
}).

-type unit_ref_value() :: #unit_ref{} | unit_ref_tuple() | unit_ref_map().

-endif.
