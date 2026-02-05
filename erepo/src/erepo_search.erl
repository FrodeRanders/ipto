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
-module(erepo_search).

-include("erepo.hrl").

-export([search/3]).

%% --------------------------------------------------------------------
%% search/3
%%
%% Accepts a map expression or a textual query parsed into expression form.
%% --------------------------------------------------------------------
-spec search(search_expression() | map(), search_order(), search_paging()) -> erepo_result(search_result()).
search(Expression, Order, PagingOrLimit) ->
    case Expression of
        Query when is_binary(Query); is_list(Query) ->
            case erepo_search_parser:parse(Query) of
                {ok, Parsed} -> erepo_db:search_units(Parsed, Order, PagingOrLimit);
                Error -> Error
            end;
        _ ->
            erepo_db:search_units(Expression, Order, PagingOrLimit)
    end.
