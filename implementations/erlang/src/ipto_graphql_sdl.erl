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
-module(ipto_graphql_sdl).

-export([inspect/1, parse/1]).

-type validation_error() :: #{
    code := atom(),
    message := binary(),
    context := map()
}.

-spec inspect(binary() | string()) -> map().
inspect(Sdl0) ->
    Sdl = to_binary(Sdl0),
    AttributesInfo = parse_attribute_registry(Sdl),
    TypesInfo = parse_record_template_types(Sdl),
    Errors = validate(AttributesInfo, TypesInfo),
    build_catalog(AttributesInfo, TypesInfo, Errors).

-spec parse(binary() | string()) -> {ok, map()} | {error, {invalid_sdl, [validation_error()], map()}}.
parse(Sdl0) ->
    Catalog = inspect(Sdl0),
    Errors = maps:get(errors, Catalog, []),
    case Errors of
        [] ->
            {ok, Catalog};
        _ ->
            {error, {invalid_sdl, Errors, Catalog}}
    end.

-spec parse_attribute_registry(binary()) -> map().
parse_attribute_registry(Sdl) ->
    Pattern = "enum\\s+([A-Za-z_][A-Za-z0-9_]*)\\s+@attributeRegistry\\s*\\{([\\s\\S]*?)\\}",
    case match_first(Sdl, Pattern) of
        nomatch ->
            #{
                found => false,
                enum_name => undefined,
                attributes => [],
                attribute_defs => []
            };
        {ok, [EnumName, Body]} ->
            AttrDefs = parse_attribute_definitions(Body),
            AttrNames = [maps:get(symbol, Def) || Def <- AttrDefs],
            #{
                found => true,
                enum_name => EnumName,
                attributes => AttrNames,
                attribute_defs => AttrDefs
            }
    end.

-spec parse_attribute_definitions(binary()) -> [map()].
parse_attribute_definitions(Body) ->
    Pattern = "(?:^|\\n)\\s*([A-Za-z_][A-Za-z0-9_]*)\\s+@attribute\\s*\\(([^\\)]*)\\)",
    [parse_attribute_definition(Name, Args) || [Name, Args] <- match_all(Body, Pattern)].

-spec parse_attribute_definition(binary(), binary()) -> map().
parse_attribute_definition(Symbol, Args) ->
    Datatype = parse_arg_token(Args, "datatype"),
    Name = parse_arg_string(Args, "name"),
    Uri = parse_arg_string(Args, "uri"),
    Description = parse_arg_string(Args, "description"),
    IsArray = parse_arg_boolean(Args, "array", true),
    #{
        symbol => Symbol,
        datatype => Datatype,
        is_array => IsArray,
        name => Name,
        qualname => Name,
        uri => Uri,
        description => Description
    }.

-spec parse_record_template_types(binary()) -> [map()].
parse_record_template_types(Sdl) ->
    Pattern = "type\\s+([A-Za-z_][A-Za-z0-9_]*)\\s+([^\\{]*?)\\{([\\s\\S]*?)\\}",
    lists:foldl(
        fun([TypeName, Header, Body], Acc) ->
            case parse_record_template_type(TypeName, Header, Body) of
                undefined -> Acc;
                TypeInfo -> [TypeInfo | Acc]
            end
        end,
        [],
        match_all(Sdl, Pattern)
    ).

-spec parse_record_template_type(binary(), binary(), binary()) -> map() | undefined.
parse_record_template_type(TypeName, Header, Body) ->
    RecordAttr = parse_record_directive_attribute(Header),
    TemplateName = parse_template_directive_name(Header),
    case {RecordAttr, TemplateName} of
        {undefined, undefined} ->
            undefined;
        _ ->
            #{
                type_name => TypeName,
                record_attribute => RecordAttr,
                template_name => TemplateName,
                uses => parse_use_references(Body)
            }
    end.

-spec parse_record_directive_attribute(binary()) -> binary() | undefined.
parse_record_directive_attribute(Header) ->
    Pattern = "@record\\s*\\(\\s*attribute\\s*:\\s*([A-Za-z_][A-Za-z0-9_]*)\\s*\\)",
    case match_first(Header, Pattern) of
        {ok, [Attr]} -> Attr;
        nomatch -> undefined
    end.

-spec parse_template_directive_name(binary()) -> binary() | undefined.
parse_template_directive_name(Header) ->
    PatternWithName = "@template\\s*\\(\\s*name\\s*:\\s*\"([^\"]+)\"\\s*\\)",
    case match_first(Header, PatternWithName) of
        {ok, [Name]} ->
            Name;
        nomatch ->
            PatternWithoutName = "@template\\b",
            case match_first(Header, PatternWithoutName) of
                {ok, _} -> <<>>;
                nomatch -> undefined
            end
    end.

-spec parse_use_references(binary()) -> [map()].
parse_use_references(Body) ->
    Pattern = "(?:^|\\n)\\s*([A-Za-z_][A-Za-z0-9_]*)\\s*:[^\\n]*?@use\\s*\\(\\s*attribute\\s*:\\s*([A-Za-z_][A-Za-z0-9_]*)\\s*\\)",
    [
        #{
            field_name => FieldName,
            attribute => AttrName
        }
        || [FieldName, AttrName] <- match_all(Body, Pattern)
    ].

-spec validate(map(), [map()]) -> [validation_error()].
validate(AttributesInfo, TypesInfo) ->
    BaseErrors = case maps:get(found, AttributesInfo, false) of
        true -> [];
        false -> [error_missing_attribute_registry()]
    end,
    Attrs = maps:get(attributes, AttributesInfo, []),
    AttrSet = maps:from_keys(Attrs, true),
    DuplicateErrors = duplicate_attribute_errors(Attrs),
    lists:reverse(
        lists:foldl(
            fun(TypeInfo, Acc0) ->
                Acc1 = maybe_add_unresolved_record_attribute_error(TypeInfo, AttrSet, Acc0),
                maybe_add_unresolved_use_errors(TypeInfo, AttrSet, Acc1)
            end,
            DuplicateErrors ++ BaseErrors,
            TypesInfo
        )
    ).

-spec duplicate_attribute_errors([binary()]) -> [validation_error()].
duplicate_attribute_errors(Attrs) ->
    {_Seen, Dups} = lists:foldl(
        fun(Attr, {Seen0, Dups0}) ->
            case maps:is_key(Attr, Seen0) of
                true -> {Seen0, [Attr | Dups0]};
                false -> {Seen0#{Attr => true}, Dups0}
            end
        end,
        {#{}, []},
        Attrs
    ),
    lists:map(
        fun(Attr) ->
            #{
                code => duplicate_attribute,
                message => <<"Duplicate attribute definition in @attributeRegistry.">>,
                context => #{attribute => Attr}
            }
        end,
        lists:usort(Dups)
    ).

-spec maybe_add_unresolved_record_attribute_error(map(), map(), [validation_error()]) -> [validation_error()].
maybe_add_unresolved_record_attribute_error(TypeInfo, AttrSet, Errors) ->
    case maps:get(record_attribute, TypeInfo, undefined) of
        undefined ->
            Errors;
        Attr ->
            case maps:is_key(Attr, AttrSet) of
                true ->
                    Errors;
                false ->
                    [
                        #{
                            code => unresolved_record_attribute,
                            message => <<"@record(attribute: ...) references unknown attribute.">>,
                            context => #{
                                type => maps:get(type_name, TypeInfo),
                                attribute => Attr
                            }
                        }
                        | Errors
                    ]
            end
    end.

-spec maybe_add_unresolved_use_errors(map(), map(), [validation_error()]) -> [validation_error()].
maybe_add_unresolved_use_errors(TypeInfo, AttrSet, Errors) ->
    lists:foldl(
        fun(UseInfo, Acc) ->
            Attr = maps:get(attribute, UseInfo),
            case maps:is_key(Attr, AttrSet) of
                true ->
                    Acc;
                false ->
                    [
                        #{
                            code => unresolved_use_attribute,
                            message => <<"@use(attribute: ...) references unknown attribute.">>,
                            context => #{
                                type => maps:get(type_name, TypeInfo),
                                field => maps:get(field_name, UseInfo),
                                attribute => Attr
                            }
                        }
                        | Acc
                    ]
            end
        end,
        Errors,
        maps:get(uses, TypeInfo, [])
    ).

-spec error_missing_attribute_registry() -> validation_error().
error_missing_attribute_registry() ->
    #{
        code => missing_attribute_registry,
        message => <<"No enum annotated with @attributeRegistry was found.">>,
        context => #{}
    }.

-spec build_catalog(map(), [map()], [validation_error()]) -> map().
build_catalog(AttributesInfo, TypesInfo, Errors) ->
    Templates = [
        #{
            type_name => maps:get(type_name, T),
            name => maps:get(template_name, T),
            uses => maps:get(uses, T, [])
        }
        || T <- TypesInfo, maps:get(template_name, T, undefined) =/= undefined
    ],
    #{
        attribute_registry => #{
            found => maps:get(found, AttributesInfo, false),
            enum_name => maps:get(enum_name, AttributesInfo, undefined),
            attributes => maps:get(attributes, AttributesInfo, []),
            attribute_defs => maps:get(attribute_defs, AttributesInfo, [])
        },
        records => [
            #{
                type_name => maps:get(type_name, T),
                attribute => maps:get(record_attribute, T),
                uses => maps:get(uses, T, [])
            }
            || T <- TypesInfo, maps:get(record_attribute, T, undefined) =/= undefined
        ],
        templates => Templates,
        counts => #{
            attributes => length(maps:get(attributes, AttributesInfo, [])),
            records => length([T || T <- TypesInfo, maps:get(record_attribute, T, undefined) =/= undefined]),
            templates => length(Templates)
        },
        errors => Errors
    }.

-spec parse_arg_string(binary(), string()) -> binary() | undefined.
parse_arg_string(Args, Key) ->
    Pattern = Key ++ "\\s*:\\s*\"([^\"]+)\"",
    case match_first(Args, Pattern) of
        {ok, [Value]} -> Value;
        nomatch -> undefined
    end.

-spec parse_arg_token(binary(), string()) -> binary() | undefined.
parse_arg_token(Args, Key) ->
    Pattern = Key ++ "\\s*:\\s*([A-Za-z_][A-Za-z0-9_]*)",
    case match_first(Args, Pattern) of
        {ok, [Value]} -> Value;
        nomatch -> undefined
    end.

-spec parse_arg_boolean(binary(), string(), boolean()) -> boolean().
parse_arg_boolean(Args, Key, Default) ->
    Pattern = Key ++ "\\s*:\\s*(true|false)",
    case match_first(Args, Pattern) of
        {ok, [<<"true">>]} -> true;
        {ok, [<<"false">>]} -> false;
        nomatch -> Default
    end.

-spec match_first(binary(), string()) -> {ok, [binary()]} | nomatch.
match_first(Text, Pattern) ->
    case re:run(Text, Pattern, [{capture, all_but_first, binary}, unicode]) of
        {match, Captures} -> {ok, Captures};
        nomatch -> nomatch
    end.

-spec match_all(binary(), string()) -> [[binary()]].
match_all(Text, Pattern) ->
    case re:run(Text, Pattern, [global, {capture, all_but_first, binary}, unicode]) of
        {match, Captures} -> Captures;
        nomatch -> []
    end.

-spec to_binary(term()) -> binary().
to_binary(Value) when is_binary(Value) -> Value;
to_binary(Value) when is_list(Value) -> unicode:characters_to_binary(Value);
to_binary(Value) -> unicode:characters_to_binary(io_lib:format("~p", [Value])).
