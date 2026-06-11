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
-module(ipto_search_ast).

-include("ipto.hrl").

-export([
    and_expr/2, or_expr/2, not_expr/1, leaf/1, between_expr/2,
    is_and/1, is_or/1, is_not/1, is_leaf/1, is_between/1,
    unit_item/3, attr_item/5, rel_item/3, assoc_item/3,
    is_unit_item/1, is_attr_item/1, is_rel_item/1, is_assoc_item/1,
    item_attribute_name/1, item_attrid/1, item_type/1,
    item_operator/1, item_value/1, item_unit_column/1,
    item_rel_direction/1, item_rel_type/1, item_rel_ref/1,
    item_assoc_direction/1, item_assoc_type/1, item_assoc_string/1,
    fold/3, collect_leaves/1, collect_unit_leaves/1, collect_attr_leaves/1
]).

-type operator() :: eq | ne | gt | gte | lt | lte | like.
-type direction() :: left | right.

-type search_item() :: {unit, atom(), operator(), term()}
                     | {attr, binary(), pos_integer() | undefined, attribute_type(), operator(), term()}
                     | {rel, direction(), pos_integer(), unit_ref_tuple()}
                     | {assoc, direction(), pos_integer(), binary()}.

-type search_expr() :: {'$and', search_expr(), search_expr()}
                     | {'$or', search_expr(), search_expr()}
                     | {'$not', search_expr()}
                     | {'$between', search_item(), search_item()}
                     | {leaf, search_item()}.

-export_type([search_expr/0, search_item/0, operator/0, direction/0]).

-spec and_expr(search_expr(), search_expr()) -> search_expr().
and_expr(Left, Right) -> {'$and', Left, Right}.

-spec or_expr(search_expr(), search_expr()) -> search_expr().
or_expr(Left, Right) -> {'$or', Left, Right}.

-spec not_expr(search_expr()) -> search_expr().
not_expr(Inner) -> {'$not', Inner}.

-spec leaf(search_item()) -> search_expr().
leaf(Item) -> {leaf, Item}.

-spec between_expr(search_item(), search_item()) -> search_expr().
between_expr(Lower, Upper) -> {'$between', Lower, Upper}.

-spec is_and(search_expr()) -> boolean().
is_and({'$and', _, _}) -> true;
is_and(_) -> false.

-spec is_or(search_expr()) -> boolean().
is_or({'$or', _, _}) -> true;
is_or(_) -> false.

-spec is_not(search_expr()) -> boolean().
is_not({'$not', _}) -> true;
is_not(_) -> false.

-spec is_leaf(search_expr()) -> boolean().
is_leaf({leaf, _}) -> true;
is_leaf(_) -> false.

-spec is_between(search_expr()) -> boolean().
is_between({'$between', _, _}) -> true;
is_between(_) -> false.

-spec unit_item(atom(), operator(), term()) -> search_item().
unit_item(Column, Op, Value) -> {unit, Column, Op, Value}.

-spec attr_item(binary(), pos_integer() | undefined, attribute_type(), operator(), term()) -> search_item().
attr_item(Name, AttrId, Type, Op, Value) -> {attr, Name, AttrId, Type, Op, Value}.

-spec rel_item(direction(), pos_integer(), unit_ref_tuple()) -> search_item().
rel_item(Direction, RelType, Ref) -> {rel, Direction, RelType, Ref}.

-spec assoc_item(direction(), pos_integer(), binary()) -> search_item().
assoc_item(Direction, AssocType, RefString) -> {assoc, Direction, AssocType, RefString}.

-spec is_unit_item(search_item()) -> boolean().
is_unit_item({unit, _, _, _}) -> true;
is_unit_item(_) -> false.

-spec is_attr_item(search_item()) -> boolean().
is_attr_item({attr, _, _, _, _, _}) -> true;
is_attr_item(_) -> false.

-spec is_rel_item(search_item()) -> boolean().
is_rel_item({rel, _, _, _}) -> true;
is_rel_item(_) -> false.

-spec is_assoc_item(search_item()) -> boolean().
is_assoc_item({assoc, _, _, _}) -> true;
is_assoc_item(_) -> false.

-spec item_attribute_name(search_item()) -> binary() | undefined.
item_attribute_name({attr, Name, _, _, _, _}) -> Name;
item_attribute_name(_) -> undefined.

-spec item_attrid(search_item()) -> pos_integer() | undefined.
item_attrid({attr, _, AttrId, _, _, _}) -> AttrId;
item_attrid(_) -> undefined.

-spec item_type(search_item()) -> attribute_type() | undefined.
item_type({attr, _, _, Type, _, _}) -> Type;
item_type(_) -> undefined.

-spec item_operator(search_item()) -> operator().
item_operator({unit, _, Op, _}) -> Op;
item_operator({attr, _, _, _, Op, _}) -> Op;
item_operator(_) -> eq.

-spec item_value(search_item()) -> term().
item_value({unit, _, _, V}) -> V;
item_value({attr, _, _, _, _, V}) -> V;
item_value(_) -> undefined.

-spec item_unit_column(search_item()) -> atom() | undefined.
item_unit_column({unit, Col, _, _}) -> Col;
item_unit_column(_) -> undefined.

-spec item_rel_direction(search_item()) -> direction() | undefined.
item_rel_direction({rel, Dir, _, _}) -> Dir;
item_rel_direction(_) -> undefined.

-spec item_rel_type(search_item()) -> pos_integer() | undefined.
item_rel_type({rel, _, RT, _}) -> RT;
item_rel_type(_) -> undefined.

-spec item_rel_ref(search_item()) -> unit_ref_tuple() | undefined.
item_rel_ref({rel, _, _, Ref}) -> Ref;
item_rel_ref(_) -> undefined.

-spec item_assoc_direction(search_item()) -> direction() | undefined.
item_assoc_direction({assoc, Dir, _, _}) -> Dir;
item_assoc_direction(_) -> undefined.

-spec item_assoc_type(search_item()) -> pos_integer() | undefined.
item_assoc_type({assoc, _, AT, _}) -> AT;
item_assoc_type(_) -> undefined.

-spec item_assoc_string(search_item()) -> binary() | undefined.
item_assoc_string({assoc, _, _, S}) -> S;
item_assoc_string(_) -> undefined.

-spec fold(fun((search_item(), Acc) -> Acc), Acc, search_expr()) -> Acc.
fold(Fun, Acc, {'$and', Left, Right}) ->
    fold(Fun, fold(Fun, Acc, Left), Right);
fold(Fun, Acc, {'$or', Left, Right}) ->
    fold(Fun, fold(Fun, Acc, Left), Right);
fold(Fun, Acc, {'$not', Inner}) ->
    fold(Fun, Acc, Inner);
fold(Fun, Acc, {'$between', Lower, Upper}) ->
    Fun(Upper, Fun(Lower, Acc));
fold(Fun, Acc, {leaf, Item}) ->
    Fun(Item, Acc).

-spec collect_leaves(search_expr()) -> [search_item()].
collect_leaves(Expr) ->
    fold(fun(I, Acc) -> [I | Acc] end, [], Expr).

-spec collect_unit_leaves(search_expr()) -> [search_item()].
collect_unit_leaves(Expr) ->
    [I || I <- collect_leaves(Expr), is_unit_item(I)].

-spec collect_attr_leaves(search_expr()) -> [search_item()].
collect_attr_leaves(Expr) ->
    [I || I <- collect_leaves(Expr), not is_unit_item(I)].
