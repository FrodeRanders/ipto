-module(erepo_search).

-export([search/3]).

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
