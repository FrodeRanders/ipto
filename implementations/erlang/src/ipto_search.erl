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
-module(ipto_search).

-include("ipto.hrl").

-export([search/3]).

search(Expression, Order, PagingOrLimit) when is_tuple(Expression) ->
    ipto_db:search_units(Expression, Order, PagingOrLimit);
search(Expression, Order, PagingOrLimit) when is_binary(Expression); is_list(Expression) ->
    case ipto_search_parser:parse_ast(Expression) of
        {ok, Expr} -> ipto_db:search_units(Expr, Order, PagingOrLimit);
        Error -> Error
    end;
search(Expression, Order, PagingOrLimit) when is_map(Expression) ->
    Expr = ipto_search_parser:map_to_ast(Expression),
    ipto_db:search_units(Expr, Order, PagingOrLimit);
search(Expression, Order, PagingOrLimit) ->
    ipto_db:search_units(Expression, Order, PagingOrLimit).
