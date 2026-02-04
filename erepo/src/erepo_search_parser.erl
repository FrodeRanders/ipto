-module(erepo_search_parser).

-export([parse/1]).

parse(Query) when is_binary(Query); is_list(Query) ->
    Clauses = split_clauses(Query),
    parse_clauses(Clauses, #{});
parse(_Query) ->
    {error, invalid_query}.

split_clauses(Query) when is_binary(Query) ->
    split_clauses(unicode:characters_to_list(Query));
split_clauses(Query) when is_list(Query) ->
    [C || C <- re:split(Query, "\\s+[Aa][Nn][Dd]\\s+", [{return, list}]), C =/= []].

parse_clauses([], Expr) ->
    {ok, Expr};
parse_clauses([Clause | Rest], Expr) ->
    case parse_clause(Clause) of
        {ok, {Key, Value}} ->
            parse_clauses(Rest, Expr#{Key => Value});
        Error ->
            Error
    end.

parse_clause(Clause) ->
    case re:run(Clause, "^\\s*([A-Za-z_][A-Za-z0-9_]*)\\s*(=|~|>=|<)\\s*(.+?)\\s*$", [{capture, all_but_first, list}]) of
        {match, [Field0, Op, Value0]} ->
            Field = string:lowercase(Field0),
            Value = normalize_value(Value0),
            to_filter(Field, Op, Value);
        nomatch ->
            {error, {invalid_clause, Clause}}
    end.

to_filter("tenantid", "=", Value) ->
    int_filter(tenantid, Value);
to_filter("unitid", "=", Value) ->
    int_filter(unitid, Value);
to_filter("status", "=", Value) ->
    int_filter(status, Value);
to_filter("name", "=", Value) ->
    {ok, {name, to_binary(Value)}};
to_filter("name", "~", Value) ->
    {ok, {name_ilike, to_binary(Value)}};
to_filter("created", ">=", Value) ->
    {ok, {created_after, to_binary(Value)}};
to_filter("created", "<", Value) ->
    {ok, {created_before, to_binary(Value)}};
to_filter("created_after", "=", Value) ->
    {ok, {created_after, to_binary(Value)}};
to_filter("created_before", "=", Value) ->
    {ok, {created_before, to_binary(Value)}};
to_filter(Field, _Op, _Value) ->
    {error, {unsupported_field, Field}}.

int_filter(Key, Value) ->
    case string:to_integer(Value) of
        {I, []} -> {ok, {Key, I}};
        _ -> {error, {invalid_integer, {Key, Value}}}
    end.

normalize_value(Value0) ->
    Value1 = string:trim(Value0),
    case Value1 of
        [] ->
            [];
        _ ->
            case {lists:nth(1, Value1), lists:nth(length(Value1), Value1)} of
                {$", $"} when length(Value1) >= 2 ->
                    lists:sublist(Value1, 2, length(Value1) - 2);
                {$', $'} when length(Value1) >= 2 ->
                    lists:sublist(Value1, 2, length(Value1) - 2);
                _ ->
                    Value1
            end
    end.

to_binary(Value) when is_binary(Value) ->
    Value;
to_binary(Value) when is_list(Value) ->
    unicode:characters_to_binary(Value).
