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
-module(ipto_db_memory).
-behaviour(ipto_backend).

-compile(nowarn_unused_function).

-include("ipto.hrl").

-export([
    get_unit_json/3,
    unit_exists/2,
    store_unit_json/1,
    search_units/3,
    add_relation/3,
    remove_relation/3,
    get_right_relation/2,
    get_right_relations/2,
    get_left_relations/2,
    count_right_relations/2,
    count_left_relations/2,
    add_association/3,
    remove_association/3,
    get_right_association/2,
    get_right_associations/2,
    get_left_associations/2,
    count_right_associations/2,
    count_left_associations/2,
    lock_unit/3,
    unlock_unit/1,
    is_unit_locked/1,
    set_status/2,
    create_attribute/5,
    get_attribute_info/1,
    get_tenant_info/1,
    upsert_record_template/3,
    upsert_unit_template/2
]).

-define(ATTR_SEQ_KEY, {meta, attr_seq}).
-define(ATTR_BY_NAME_KEY, {meta, attrs_by_name}).
-define(ATTR_BY_ID_KEY, {meta, attrs_by_id}).
-define(REL_KEY, {meta, relations}).
-define(ASSOC_KEY, {meta, associations}).
-define(LOCK_KEY, {meta, locks}).

%% Backend API

-spec get_unit_json(tenantid(), unitid(), version_selector()) -> unit_lookup_result().
get_unit_json(TenantId, UnitId, latest) ->
    get_latest_version(TenantId, UnitId);
get_unit_json(TenantId, UnitId, Version) when is_integer(Version), Version > 0 ->
    Key = {unit, TenantId, UnitId, Version},
    case ipto_cache:get(Key) of
        undefined -> not_found;
        Unit -> {ok, Unit}
    end;
get_unit_json(_TenantId, _UnitId, _Version) ->
    {error, invalid_version}.

-spec unit_exists(tenantid(), unitid()) -> boolean().
unit_exists(TenantId, UnitId) ->
    case get_latest_version(TenantId, UnitId) of
        {ok, _} -> true;
        not_found -> false;
        _ -> false
    end.

-spec store_unit_json(unit_map()) -> ipto_result(unit_map()).
store_unit_json(UnitMap0) ->
    TenantId = maps:get(tenantid, UnitMap0),
    UnitId = maps:get(unitid, UnitMap0, undefined),
    AssignedUnitId = case UnitId of
        undefined -> erlang:unique_integer([monotonic, positive]);
        _ -> UnitId
    end,
    CurrentVersion =
        case get_latest_version(TenantId, AssignedUnitId) of
            {ok, Existing} -> maps:get(unitver, Existing, 1);
            not_found -> 0
        end,
    NextVersion = CurrentVersion + 1,
    Now = erlang:system_time(second),
    Created = case CurrentVersion of
        0 -> Now;
        _ -> maps:get(created, UnitMap0, Now)
    end,
    UnitMap = UnitMap0#{
        unitid => AssignedUnitId,
        unitver => NextVersion,
        created => Created,
        modified => Now,
        isreadonly => false
    },
    Key = {unit, TenantId, AssignedUnitId, NextVersion},
    LatestKey = {latest, TenantId, AssignedUnitId},
    ipto_cache:put(Key, UnitMap),
    ipto_cache:put(LatestKey, NextVersion),
    {ok, UnitMap}.

-spec add_relation(unit_ref_value(), relation_type(), unit_ref_value()) -> ok | {error, ipto_reason()}.
add_relation(UnitRef, RelType, OtherUnitRef) ->
    Relations0 = ipto_db_utils:get_meta_map(?REL_KEY),
    RelKey = {ipto_db_utils:normalize_ref(UnitRef), RelType, ipto_db_utils:normalize_ref(OtherUnitRef)},
    ipto_cache:put(?REL_KEY, Relations0#{RelKey => true}),
    ok.

-spec remove_relation(unit_ref_value(), relation_type(), unit_ref_value()) -> ok | {error, ipto_reason()}.
remove_relation(UnitRef, RelType, OtherUnitRef) ->
    Relations0 = ipto_db_utils:get_meta_map(?REL_KEY),
    RelKey = {ipto_db_utils:normalize_ref(UnitRef), RelType, ipto_db_utils:normalize_ref(OtherUnitRef)},
    ipto_cache:put(?REL_KEY, maps:remove(RelKey, Relations0)),
    ok.

-spec get_right_relation(unit_ref_value(), relation_type()) -> relation_lookup_result().
get_right_relation(UnitRef, RelType) when is_integer(RelType) ->
    case get_right_relations(UnitRef, RelType) of
        {ok, [Rel | _]} -> {ok, Rel};
        {ok, []} -> not_found;
        Error -> Error
    end;
get_right_relation(_UnitRef, _RelType) ->
    {error, invalid_relation_type}.

-spec get_right_relations(unit_ref_value(), relation_type()) -> ipto_result([relation()]).
get_right_relations(UnitRef, RelType) when is_integer(RelType) ->
    case ipto_db_utils:normalize_ref(UnitRef) of
        {_, _} = Ref ->
            Relations0 = ipto_db_utils:get_meta_map(?REL_KEY),
            Matches = [
                relation_from_key(RelKey)
                || {RelKey = {Ref0, RelType0, _OtherRef}, true} <- maps:to_list(Relations0),
                   Ref0 =:= Ref,
                   RelType0 =:= RelType
            ],
            {ok, [R || R <- Matches, R =/= #{}]};
        _ ->
            {error, invalid_unit_ref}
    end;
get_right_relations(_UnitRef, _RelType) ->
    {error, invalid_relation_type}.

-spec get_left_relations(unit_ref_value(), relation_type()) -> ipto_result([relation()]).
get_left_relations(UnitRef, RelType) when is_integer(RelType) ->
    case ipto_db_utils:normalize_ref(UnitRef) of
        {_, _} = Ref ->
            Relations0 = ipto_db_utils:get_meta_map(?REL_KEY),
            Matches = [
                relation_from_key(RelKey)
                || {RelKey = {_Ref0, RelType0, OtherRef}, true} <- maps:to_list(Relations0),
                   OtherRef =:= Ref,
                   RelType0 =:= RelType
            ],
            {ok, [R || R <- Matches, R =/= #{}]};
        _ ->
            {error, invalid_unit_ref}
    end;
get_left_relations(_UnitRef, _RelType) ->
    {error, invalid_relation_type}.

-spec count_right_relations(unit_ref_value(), relation_type()) -> ipto_result(non_neg_integer()).
count_right_relations(UnitRef, RelType) when is_integer(RelType) ->
    case get_right_relations(UnitRef, RelType) of
        {ok, Relations} -> {ok, length(Relations)};
        Error -> Error
    end;
count_right_relations(_UnitRef, _RelType) ->
    {error, invalid_relation_type}.

-spec count_left_relations(unit_ref_value(), relation_type()) -> ipto_result(non_neg_integer()).
count_left_relations(UnitRef, RelType) when is_integer(RelType) ->
    case get_left_relations(UnitRef, RelType) of
        {ok, Relations} -> {ok, length(Relations)};
        Error -> Error
    end;
count_left_relations(_UnitRef, _RelType) ->
    {error, invalid_relation_type}.

-spec add_association(unit_ref_value(), association_type(), ref_string()) -> ok | {error, ipto_reason()}.
add_association(UnitRef, AssocType, RefString) ->
    Assocs0 = ipto_db_utils:get_meta_map(?ASSOC_KEY),
    AssocKey = {ipto_db_utils:normalize_ref(UnitRef), AssocType, RefString},
    ipto_cache:put(?ASSOC_KEY, Assocs0#{AssocKey => true}),
    ok.

-spec remove_association(unit_ref_value(), association_type(), ref_string()) -> ok | {error, ipto_reason()}.
remove_association(UnitRef, AssocType, RefString) ->
    Assocs0 = ipto_db_utils:get_meta_map(?ASSOC_KEY),
    AssocKey = {ipto_db_utils:normalize_ref(UnitRef), AssocType, RefString},
    ipto_cache:put(?ASSOC_KEY, maps:remove(AssocKey, Assocs0)),
    ok.

-spec get_right_association(unit_ref_value(), association_type()) -> association_lookup_result().
get_right_association(UnitRef, AssocType) when is_integer(AssocType) ->
    case get_right_associations(UnitRef, AssocType) of
        {ok, [Assoc | _]} -> {ok, Assoc};
        {ok, []} -> not_found;
        Error -> Error
    end;
get_right_association(_UnitRef, _AssocType) ->
    {error, invalid_association}.

-spec get_right_associations(unit_ref_value(), association_type()) -> ipto_result([association()]).
get_right_associations(UnitRef, AssocType) when is_integer(AssocType) ->
    case ipto_db_utils:normalize_ref(UnitRef) of
        {_, _} = Ref ->
            Assocs0 = ipto_db_utils:get_meta_map(?ASSOC_KEY),
            Matches = [
                association_from_key(AssocKey)
                || {AssocKey = {Ref0, AssocType0, _RefString}, true} <- maps:to_list(Assocs0),
                   Ref0 =:= Ref,
                   AssocType0 =:= AssocType
            ],
            {ok, [A || A <- Matches, A =/= #{}]};
        _ ->
            {error, invalid_unit_ref}
    end;
get_right_associations(_UnitRef, _AssocType) ->
    {error, invalid_association}.

-spec get_left_associations(association_type(), ref_string()) -> ipto_result([association()]).
get_left_associations(AssocType, RefString) when is_integer(AssocType), (is_binary(RefString) orelse is_list(RefString)) ->
    RefStringNorm = ipto_db_utils:normalize_string(RefString),
    Assocs0 = ipto_db_utils:get_meta_map(?ASSOC_KEY),
    Matches = [
        association_from_key(AssocKey)
        || {AssocKey = {_Ref0, AssocType0, AssocString0}, true} <- maps:to_list(Assocs0),
           AssocType0 =:= AssocType,
           ipto_db_utils:normalize_string(AssocString0) =:= RefStringNorm
    ],
    {ok, [A || A <- Matches, A =/= #{}]};
get_left_associations(_AssocType, _RefString) ->
    {error, invalid_association}.

-spec count_right_associations(unit_ref_value(), association_type()) -> ipto_result(non_neg_integer()).
count_right_associations(UnitRef, AssocType) when is_integer(AssocType) ->
    case get_right_associations(UnitRef, AssocType) of
        {ok, Assocs} -> {ok, length(Assocs)};
        Error -> Error
    end;
count_right_associations(_UnitRef, _AssocType) ->
    {error, invalid_association}.

-spec count_left_associations(association_type(), ref_string()) -> ipto_result(non_neg_integer()).
count_left_associations(AssocType, RefString) when is_integer(AssocType), (is_binary(RefString) orelse is_list(RefString)) ->
    case get_left_associations(AssocType, RefString) of
        {ok, Assocs} -> {ok, length(Assocs)};
        Error -> Error
    end;
count_left_associations(_AssocType, _RefString) ->
    {error, invalid_association}.

-spec lock_unit(unit_ref_value(), lock_type(), ref_string()) -> ok | already_locked | {error, ipto_reason()}.
lock_unit(UnitRef, LockType, Purpose) ->
    Locks0 = ipto_db_utils:get_meta_map(?LOCK_KEY),
    Ref = ipto_db_utils:normalize_ref(UnitRef),
    case maps:is_key(Ref, Locks0) of
        true -> already_locked;
        false ->
            Lock = #{locktype => LockType, purpose => Purpose, locked_at => erlang:system_time(second)},
            ipto_cache:put(?LOCK_KEY, Locks0#{Ref => Lock}),
            ok
    end.

-spec unlock_unit(unit_ref_value()) -> ok | {error, ipto_reason()}.
unlock_unit(UnitRef) ->
    Locks0 = ipto_db_utils:get_meta_map(?LOCK_KEY),
    Ref = ipto_db_utils:normalize_ref(UnitRef),
    ipto_cache:put(?LOCK_KEY, maps:remove(Ref, Locks0)),
    ok.

-spec is_unit_locked(unit_ref_value()) -> boolean().
is_unit_locked(UnitRef) ->
    Locks0 = ipto_db_utils:get_meta_map(?LOCK_KEY),
    Ref = ipto_db_utils:normalize_ref(UnitRef),
    maps:is_key(Ref, Locks0).

-spec set_status(unit_ref_value(), unit_status()) -> ok | {error, ipto_reason()}.
set_status(UnitRef, Status) ->
    case ipto_db_utils:normalize_ref(UnitRef) of
        {TenantId, UnitId} ->
            case get_latest_version(TenantId, UnitId) of
                {ok, Unit} ->
                    Version = maps:get(unitver, Unit, 1),
                    Key = {unit, TenantId, UnitId, Version},
                    UpdatedUnit = Unit#{status => Status},
                    ipto_cache:put(Key, UpdatedUnit),
                    ok;
                not_found ->
                    {error, not_found}
            end;
        _ ->
            {error, invalid_unit_ref}
    end.

-spec create_attribute(attribute_alias(), attribute_name(), attribute_qualname(), attribute_type(), boolean()) ->
    ipto_result(attribute_info()).
create_attribute(Alias, Name, QualName, Type, IsArray)
  when is_binary(Name), is_binary(QualName), is_integer(Type), is_boolean(IsArray) ->
    AttrByName0 = ipto_db_utils:get_meta_map(?ATTR_BY_NAME_KEY),
    case maps:get(Name, AttrByName0, undefined) of
        undefined ->
            NextId = ipto_db_utils:next_attr_id(),
            Info = #{
                id => NextId,
                alias => Alias,
                name => Name,
                qualname => QualName,
                type => Type,
                forced_scalar => not IsArray
            },
            AttrById0 = ipto_db_utils:get_meta_map(?ATTR_BY_ID_KEY),
            ipto_cache:put(?ATTR_BY_NAME_KEY, AttrByName0#{Name => Info}),
            ipto_cache:put(?ATTR_BY_ID_KEY, AttrById0#{NextId => Info}),
            {ok, Info};
        Existing ->
            {ok, Existing}
    end;
create_attribute(_Alias, _Name, _QualName, _Type, _IsArray) ->
    {error, invalid_attribute_definition}.

-spec get_attribute_info(name_or_id()) -> {ok, attribute_info()} | not_found | {error, ipto_reason()}.
get_attribute_info(NameOrId) when is_binary(NameOrId) ->
    AttrByName = ipto_db_utils:get_meta_map(?ATTR_BY_NAME_KEY),
    case maps:get(NameOrId, AttrByName, undefined) of
        undefined -> not_found;
        Info -> {ok, Info}
    end;
get_attribute_info(NameOrId) when is_integer(NameOrId), NameOrId > 0 ->
    AttrById = ipto_db_utils:get_meta_map(?ATTR_BY_ID_KEY),
    case maps:get(NameOrId, AttrById, undefined) of
        undefined -> not_found;
        Info -> {ok, Info}
    end;
get_attribute_info(_NameOrId) ->
    {error, invalid_attribute_id_or_name}.

-spec get_tenant_info(name_or_id()) -> {ok, tenant_info()} | not_found | {error, ipto_reason()}.
get_tenant_info(NameOrId) when is_integer(NameOrId), NameOrId > 0 ->
    {ok, #{id => NameOrId, name => iolist_to_binary(io_lib:format("tenant-~p", [NameOrId]))}};
get_tenant_info(NameOrId) when is_binary(NameOrId) ->
    {ok, #{id => 1, name => NameOrId}};
get_tenant_info(_NameOrId) ->
    {error, invalid_tenant_id_or_name}.

-spec upsert_record_template(integer(), binary(), [{integer(), binary()}]) -> ok | {error, ipto_reason()}.
upsert_record_template(_RecordId, _RecordName, _Fields) ->
    {error, unsupported_operation}.

-spec upsert_unit_template(binary(), [{integer(), binary()}]) -> ok | {error, ipto_reason()}.
upsert_unit_template(_TemplateName, _Fields) ->
    {error, unsupported_operation}.

%% Search

-spec search_units(term(), search_order(), search_paging()) -> ipto_result(search_result()).
search_units(Expression, Order, PagingOrLimit) when is_tuple(Expression) ->
    Units0 = memory_latest_units(),
    Filtered = [Unit || Unit <- Units0, memory_match_ast(Unit, Expression)],
    Sorted = memory_sort_units(Filtered, Order),
    Paged = memory_apply_paging(Sorted, PagingOrLimit),
    {ok, #{results => Paged, total => length(Filtered)}};
search_units(Expression0, Order, PagingOrLimit) ->
    case ipto_db_utils:normalize_search_expression(Expression0) of
        {ok, Expression} ->
            Units0 = memory_latest_units(),
            Filtered = [Unit || Unit <- Units0, memory_match_unit(Unit, Expression)],
            Sorted = memory_sort_units(Filtered, Order),
            Paged = memory_apply_paging(Sorted, PagingOrLimit),
            {ok, #{results => Paged, total => length(Filtered)}};
        Error ->
            Error
    end.

-spec memory_match_ast(unit_map(), ipto_search_ast:search_expr()) -> boolean().
memory_match_ast(Unit, {'$and', Left, Right}) ->
    memory_match_ast(Unit, Left) andalso memory_match_ast(Unit, Right);
memory_match_ast(Unit, {'$or', Left, Right}) ->
    memory_match_ast(Unit, Left) orelse memory_match_ast(Unit, Right);
memory_match_ast(Unit, {'$not', Inner}) ->
    not memory_match_ast(Unit, Inner);
memory_match_ast(Unit, {'$between', Item1, Item2}) ->
    memory_match_item(Unit, Item1) andalso memory_match_item(Unit, Item2);
memory_match_ast(Unit, {leaf, Item}) ->
    memory_match_item(Unit, Item).

-spec memory_match_item(unit_map(), ipto_search_ast:search_item()) -> boolean().
memory_match_item(Unit, {unit, Col, Op, Val}) ->
    Field = case Col of
        unitname -> name;
        name -> name;
        _ -> Col
    end,
    case Op of
        eq -> compare_unit_field(Unit, Field, Val, eq);
        ne -> compare_unit_field(Unit, Field, Val, ne);
        gt -> compare_unit_field(Unit, Field, Val, gt);
        gte -> compare_unit_field(Unit, Field, Val, gte);
        lt -> compare_unit_field(Unit, Field, Val, lt);
        lte -> compare_unit_field(Unit, Field, Val, lte);
        like -> compare_unit_like(Unit, Field, Val)
    end;
memory_match_item(Unit, {attr, Name, _AttrId, _Type, Op, Value}) ->
    Attributes = maps:get(attributes, Unit, []),
    case find_attr_by_name(Attributes, Name) of
        {ok, AttrVal} ->
            case Op of
                eq -> compare_values(AttrVal, Value);
                ne -> not compare_values(AttrVal, Value);
                gt -> numeric_compare(AttrVal, Value, gt);
                gte -> numeric_compare(AttrVal, Value, gte);
                lt -> numeric_compare(AttrVal, Value, lt);
                lte -> numeric_compare(AttrVal, Value, lte);
                like -> like_compare(AttrVal, Value)
            end;
        false ->
            false
    end;
memory_match_item(_Unit, {rel, _Direction, _RelType, _Ref}) ->
    true;
memory_match_item(_Unit, {assoc, _Direction, _AssocType, _RefString}) ->
    true;
memory_match_item(_Unit, _) ->
    true.

-spec compare_unit_field(unit_map(), atom(), term(), ipto_search_ast:operator()) -> boolean().
compare_unit_field(Unit, Field, Val, eq) ->
    get_unit_field(Unit, Field) =:= Val;
compare_unit_field(Unit, Field, Val, ne) ->
    get_unit_field(Unit, Field) =/= Val;
compare_unit_field(Unit, Field, Val, gt) ->
    get_unit_field(Unit, Field) > Val;
compare_unit_field(Unit, Field, Val, gte) ->
    get_unit_field(Unit, Field) >= Val;
compare_unit_field(Unit, Field, Val, lt) ->
    get_unit_field(Unit, Field) < Val;
compare_unit_field(Unit, Field, Val, lte) ->
    get_unit_field(Unit, Field) =< Val.

-spec get_unit_field(unit_map(), atom()) -> term().
get_unit_field(Unit, name) -> maps:get(unitname, Unit, undefined);
get_unit_field(Unit, unitname) -> maps:get(unitname, Unit, undefined);
get_unit_field(Unit, corrid) -> maps:get(corrid, Unit, undefined);
get_unit_field(Unit, Field) -> maps:get(Field, Unit, undefined).

-spec compare_unit_like(unit_map(), atom(), binary()) -> boolean().
compare_unit_like(Unit, Field, Pattern) ->
    UnitVal = get_unit_field(Unit, Field),
    ipto_db_utils:like_match(UnitVal, Pattern).

-spec find_attr_by_name([map()], binary()) -> {ok, term()} | false.
find_attr_by_name([], _Name) -> false;
find_attr_by_name([Attr | Rest], Name) ->
    case maps:get(name, Attr, undefined) of
        Name -> {ok, maps:get(value, Attr, undefined)};
        _ -> find_attr_by_name(Rest, Name)
    end.

-spec compare_values(term(), term()) -> boolean().
compare_values(A, B) when is_binary(A), is_binary(B) ->
    ipto_db_utils:normalize_string(A) =:= ipto_db_utils:normalize_string(B);
compare_values(A, B) ->
    A =:= B.

-spec numeric_compare(term(), term(), gt | gte | lt | lte) -> boolean().
numeric_compare(A, B, Op) ->
    case {ipto_db_utils:to_int_maybe(A), ipto_db_utils:to_int_maybe(B)} of
        {{ok, IA}, {ok, IB}} ->
            ipto_db_utils:compare_int(IA, IB, Op);
        _ ->
            false
    end.

-spec like_compare(term(), term()) -> boolean().
like_compare(A, B) when is_binary(A); is_binary(B) ->
    ipto_db_utils:like_match(A, B);
like_compare(_, _) ->
    false.

-spec rel_matches_unit(relation()) -> boolean().
rel_matches_unit(_) -> true.

-spec memory_ast_to_map(ipto_search_ast:search_expr()) -> map().
memory_ast_to_map({'$and', Left, Right}) ->
    LM = memory_ast_to_map(Left),
    RM = memory_ast_to_map(Right),
    case {is_plain_ast_map(LM), is_plain_ast_map(RM)} of
        {true, true} -> maps:merge(LM, RM);
        _ -> #{'$and' => [LM, RM]}
    end;
memory_ast_to_map({'$or', Left, Right}) ->
    #{'$or' => [memory_ast_to_map(Left), memory_ast_to_map(Right)]};
memory_ast_to_map({'$not', Inner}) ->
    #{'$not' => memory_ast_to_map(Inner)};
memory_ast_to_map({'$between', Item1, Item2}) ->
    #{'$and' => [memory_item_to_map(Item1), memory_item_to_map(Item2)]};
memory_ast_to_map({leaf, Item}) ->
    memory_item_to_map(Item).

-spec memory_item_to_map(ipto_search_ast:search_item()) -> map().
memory_item_to_map({unit, name, eq, Val}) -> #{name => Val};
memory_item_to_map({unit, name, ne, Val}) -> #{name_ne => Val};
memory_item_to_map({unit, name, like, Val}) -> #{name_ilike => Val};
memory_item_to_map({unit, unitname, eq, Val}) -> #{name => Val};
memory_item_to_map({unit, unitname, ne, Val}) -> #{name_ne => Val};
memory_item_to_map({unit, unitname, like, Val}) -> #{name_ilike => Val};
memory_item_to_map({unit, corrid, eq, Val}) -> #{corrid => Val};
memory_item_to_map({unit, corrid, ne, Val}) -> #{corrid_ne => Val};
memory_item_to_map({unit, corrid, like, Val}) -> #{corrid_ilike => Val};
memory_item_to_map({unit, Col, eq, Val}) -> #{Col => Val};
memory_item_to_map({unit, Col, ne, Val}) -> #{list_to_atom(atom_to_list(Col) ++ "_ne") => Val};
memory_item_to_map({unit, Col, gt, Val}) -> #{list_to_atom(atom_to_list(Col) ++ "_gt") => Val};
memory_item_to_map({unit, Col, gte, Val}) ->
    Key = case Col of created -> created_after; modified -> modified_after; _ -> list_to_atom(atom_to_list(Col) ++ "_gte") end,
    #{Key => Val};
memory_item_to_map({unit, Col, lt, Val}) ->
    Key = case Col of created -> created_before; modified -> modified_before; _ -> list_to_atom(atom_to_list(Col) ++ "_lt") end,
    #{Key => Val};
memory_item_to_map({unit, Col, lte, Val}) -> #{list_to_atom(atom_to_list(Col) ++ "_lte") => Val};
memory_item_to_map({attr, Name, _AttrId, _Type, _Op, Val}) -> #{attr_value => #{name => Name, value => Val}};
memory_item_to_map(_) -> #{}.

-spec is_plain_ast_map(map()) -> boolean().
is_plain_ast_map(M) when map_size(M) =:= 0 -> true;
is_plain_ast_map(M) ->
    not maps:is_key('$and', M) andalso
    not maps:is_key('$or', M) andalso
    not maps:is_key('$not', M).

-spec memory_latest_units() -> [unit_map()].
memory_latest_units() ->
    Cache = ipto_cache:all(),
    maps:fold(
      fun(Key, Value, Acc) ->
          case Key of
              {latest, TenantId, UnitId} when is_integer(Value) ->
                  UnitKey = {unit, TenantId, UnitId, Value},
                  case maps:get(UnitKey, Cache, undefined) of
                      Unit when is_map(Unit) -> [Unit | Acc];
                      _ -> Acc
                  end;
              _ ->
                  Acc
          end
      end,
      [],
      Cache).

-spec memory_match_unit(unit_map(), map()) -> boolean().
memory_match_unit(_Unit, Expr) when map_size(Expr) =:= 0 ->
    true;
memory_match_unit(Unit, Expr) ->
    maps:fold(
      fun(Key, Value, Acc) ->
          Acc andalso memory_match_expr_key(Unit, Key, Value)
      end,
      true,
      Expr).

-spec memory_match_expr_key(unit_map(), atom(), term()) -> boolean().
memory_match_expr_key(Unit, '$and', Exprs) when is_list(Exprs) ->
    lists:all(fun(E) -> memory_match_unit(Unit, E) end, Exprs);
memory_match_expr_key(Unit, '$or', Exprs) when is_list(Exprs) ->
    lists:any(fun(E) -> memory_match_unit(Unit, E) end, Exprs);
memory_match_expr_key(Unit, '$not', Expr) when is_map(Expr) ->
    not memory_match_unit(Unit, Expr);
memory_match_expr_key(Unit, Key, Value) ->
    memory_match_field(Unit, Key, Value).

-spec memory_match_field(unit_map(), atom(), term()) -> boolean().
memory_match_field(Unit, tenantid, Value) when is_integer(Value) ->
    maps:get(tenantid, Unit, undefined) =:= Value;
memory_match_field(Unit, tenantid_ne, Value) when is_integer(Value) ->
    maps:get(tenantid, Unit, undefined) =/= Value;
memory_match_field(Unit, tenantid_gt, Value) when is_integer(Value) ->
    maps:get(tenantid, Unit, undefined) > Value;
memory_match_field(Unit, tenantid_gte, Value) when is_integer(Value) ->
    maps:get(tenantid, Unit, undefined) >= Value;
memory_match_field(Unit, tenantid_lt, Value) when is_integer(Value) ->
    maps:get(tenantid, Unit, undefined) < Value;
memory_match_field(Unit, tenantid_lte, Value) when is_integer(Value) ->
    maps:get(tenantid, Unit, undefined) =< Value;
memory_match_field(Unit, unitid, Value) when is_integer(Value) ->
    maps:get(unitid, Unit, undefined) =:= Value;
memory_match_field(Unit, unitid_ne, Value) when is_integer(Value) ->
    maps:get(unitid, Unit, undefined) =/= Value;
memory_match_field(Unit, unitid_gt, Value) when is_integer(Value) ->
    maps:get(unitid, Unit, undefined) > Value;
memory_match_field(Unit, unitid_gte, Value) when is_integer(Value) ->
    maps:get(unitid, Unit, undefined) >= Value;
memory_match_field(Unit, unitid_lt, Value) when is_integer(Value) ->
    maps:get(unitid, Unit, undefined) < Value;
memory_match_field(Unit, unitid_lte, Value) when is_integer(Value) ->
    maps:get(unitid, Unit, undefined) =< Value;
memory_match_field(Unit, status, Value) when is_integer(Value) ->
    maps:get(status, Unit, undefined) =:= Value;
memory_match_field(Unit, status_ne, Value) when is_integer(Value) ->
    maps:get(status, Unit, undefined) =/= Value;
memory_match_field(Unit, status_gt, Value) when is_integer(Value) ->
    maps:get(status, Unit, undefined) > Value;
memory_match_field(Unit, status_gte, Value) when is_integer(Value) ->
    maps:get(status, Unit, undefined) >= Value;
memory_match_field(Unit, status_lt, Value) when is_integer(Value) ->
    maps:get(status, Unit, undefined) < Value;
memory_match_field(Unit, status_lte, Value) when is_integer(Value) ->
    maps:get(status, Unit, undefined) =< Value;
memory_match_field(Unit, name, Value) ->
    ipto_db_utils:normalize_string(maps:get(unitname, Unit, <<>>)) =:= ipto_db_utils:normalize_string(Value);
memory_match_field(Unit, name_ne, Value) ->
    ipto_db_utils:normalize_string(maps:get(unitname, Unit, <<>>)) =/= ipto_db_utils:normalize_string(Value);
memory_match_field(Unit, name_ilike, Pattern) ->
    ipto_db_utils:like_match(maps:get(unitname, Unit, <<>>), Pattern);
memory_match_field(Unit, corrid, Value) ->
    ipto_db_utils:normalize_string(maps:get(corrid, Unit, <<>>)) =:= ipto_db_utils:normalize_string(Value);
memory_match_field(Unit, corrid_ne, Value) ->
    ipto_db_utils:normalize_string(maps:get(corrid, Unit, <<>>)) =/= ipto_db_utils:normalize_string(Value);
memory_match_field(Unit, corrid_ilike, Pattern) ->
    ipto_db_utils:like_match(maps:get(corrid, Unit, <<>>), Pattern);
memory_match_field(Unit, created, Value) ->
    ipto_db_utils:compare_created(maps:get(created, Unit, 0), Value, eq);
memory_match_field(Unit, created_ne, Value) ->
    ipto_db_utils:compare_created(maps:get(created, Unit, 0), Value, ne);
memory_match_field(Unit, created_gt, Value) ->
    ipto_db_utils:compare_created(maps:get(created, Unit, 0), Value, gt);
memory_match_field(Unit, created_after, Value) ->
    ipto_db_utils:compare_created(maps:get(created, Unit, 0), Value, ge);
memory_match_field(Unit, created_gte, Value) ->
    ipto_db_utils:compare_created(maps:get(created, Unit, 0), Value, ge);
memory_match_field(Unit, created_lt, Value) ->
    ipto_db_utils:compare_created(maps:get(created, Unit, 0), Value, lt);
memory_match_field(Unit, created_before, Value) ->
    ipto_db_utils:compare_created(maps:get(created, Unit, 0), Value, lt);
memory_match_field(Unit, created_lte, Value) ->
    ipto_db_utils:compare_created(maps:get(created, Unit, 0), Value, le);
memory_match_field(Unit, modified, Value) ->
    ipto_db_utils:compare_created(maps:get(modified, Unit, 0), Value, eq);
memory_match_field(Unit, modified_ne, Value) ->
    ipto_db_utils:compare_created(maps:get(modified, Unit, 0), Value, ne);
memory_match_field(Unit, modified_gt, Value) ->
    ipto_db_utils:compare_created(maps:get(modified, Unit, 0), Value, gt);
memory_match_field(Unit, modified_after, Value) ->
    ipto_db_utils:compare_created(maps:get(modified, Unit, 0), Value, ge);
memory_match_field(Unit, modified_gte, Value) ->
    ipto_db_utils:compare_created(maps:get(modified, Unit, 0), Value, ge);
memory_match_field(Unit, modified_lt, Value) ->
    ipto_db_utils:compare_created(maps:get(modified, Unit, 0), Value, lt);
memory_match_field(Unit, modified_before, Value) ->
    ipto_db_utils:compare_created(maps:get(modified, Unit, 0), Value, lt);
memory_match_field(Unit, modified_lte, Value) ->
    ipto_db_utils:compare_created(maps:get(modified, Unit, 0), Value, le);
memory_match_field(_Unit, _Key, _Value) ->
    true.

-spec memory_sort_units([unit_map()], search_order()) -> [unit_map()].
memory_sort_units(Units, Order) ->
    {Field, Dir} = ipto_db_utils:normalize_order(Order),
    lists:sort(fun(A, B) -> memory_unit_before(A, B, Field, Dir) end, Units).

-spec memory_unit_before(unit_map(), unit_map(), atom(), asc | desc) -> boolean().
memory_unit_before(A, B, Field, Dir) ->
    KA = maps:get(Field, A, undefined),
    KB = maps:get(Field, B, undefined),
    case KA =:= KB of
        true ->
            tie_break_before(A, B);
        false ->
            case Dir of
                asc -> KA < KB;
                desc -> KA > KB
            end
    end.

-spec tie_break_before(unit_map(), unit_map()) -> boolean().
tie_break_before(A, B) ->
    AUnitId = maps:get(unitid, A, 0),
    BUnitId = maps:get(unitid, B, 0),
    case AUnitId =:= BUnitId of
        true ->
            maps:get(unitver, A, 0) < maps:get(unitver, B, 0);
        false ->
            AUnitId < BUnitId
    end.

-spec memory_apply_paging([unit_map()], search_paging() | map() | integer()) -> [unit_map()].
memory_apply_paging(Units, #{limit := Limit, offset := Offset})
  when is_integer(Limit), Limit > 0, is_integer(Offset), Offset >= 0 ->
    ipto_db_utils:take_n(ipto_db_utils:drop_n(Units, Offset), Limit);
memory_apply_paging(Units, #{limit := Limit}) when is_integer(Limit), Limit > 0 ->
    ipto_db_utils:take_n(Units, Limit);
memory_apply_paging(Units, Limit) when is_integer(Limit), Limit > 0 ->
    ipto_db_utils:take_n(Units, Limit);
memory_apply_paging(Units, _) ->
    Units.

%% Internal helpers

-spec get_latest_version(tenantid(), unitid()) -> unit_lookup_result().
get_latest_version(TenantId, UnitId) ->
    LatestKey = {latest, TenantId, UnitId},
    case ipto_cache:get(LatestKey) of
        undefined ->
            not_found;
        Version ->
            Key = {unit, TenantId, UnitId, Version},
            case ipto_cache:get(Key) of
                undefined -> not_found;
                Unit -> {ok, Unit}
            end
    end.

-spec relation_from_key(term()) -> relation() | #{}.
relation_from_key({{TenantId, UnitId}, RelType, {RelTenantId, RelUnitId}}) ->
    #{
        tenantid => TenantId,
        unitid => UnitId,
        reltype => RelType,
        reltenantid => RelTenantId,
        relunitid => RelUnitId
    };
relation_from_key(_) ->
    #{}.

-spec association_from_key(term()) -> association() | #{}.
association_from_key({{TenantId, UnitId}, AssocType, AssocString}) ->
    #{
        tenantid => TenantId,
        unitid => UnitId,
        assoctype => AssocType,
        assocstring => ipto_db_utils:normalize_string(AssocString)
    };
association_from_key(_) ->
    #{}.
