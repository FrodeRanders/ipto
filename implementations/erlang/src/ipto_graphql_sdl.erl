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
    case parse_sdl_ast(Sdl) of
        {ok, Defs} ->
            AttributesInfo = extract_attribute_registry(Defs),
            TypesInfo = extract_record_template_types(Defs),
            Errors = validate(AttributesInfo, TypesInfo),
            build_catalog(AttributesInfo, TypesInfo, Errors);
        {error, Reason} ->
            Error = #{
                code => parse_error,
                message => iolist_to_binary(io_lib:format("SDL parse failed: ~p", [Reason])),
                context => #{}
            },
            build_catalog(
                #{found => false, enum_name => undefined, attributes => [], attribute_defs => []},
                [],
                [Error]
            )
    end.

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

%% SDL AST parsing via graphql-erlang

-spec parse_sdl_ast(binary()) -> {ok, list()} | {error, term()}.
parse_sdl_ast(Sdl) ->
    case code:ensure_loaded(graphql) of
        {module, graphql} ->
            case graphql:parse(Sdl) of
                {ok, Document} ->
                    {ok, element(2, Document)};
                {error, Reason} ->
                    {error, Reason}
            end;
        _ ->
            {error, graphql_not_loaded}
    end.

%% Attribute registry extraction

-spec extract_attribute_registry([term()]) -> map().
extract_attribute_registry(Defs) ->
    case find_attribute_registry_enum(Defs) of
        {ok, Enum} ->
            AttrDefs = extract_attribute_values(element(5, Enum)),
            AttrNames = [maps:get(symbol, Def) || Def <- AttrDefs],
            EnumId = element(2, Enum),
            EnumName = case EnumId of
                {name, _, N} -> N;
                N when is_binary(N) -> N;
                _ -> <<>>
            end,
            #{
                found => true,
                enum_name => EnumName,
                attributes => AttrNames,
                attribute_defs => AttrDefs
            };
        undefined ->
            #{found => false, enum_name => undefined, attributes => [], attribute_defs => []}
    end.

-spec find_attribute_registry_enum([term()]) -> {ok, term()} | undefined.
find_attribute_registry_enum([]) ->
    undefined;
find_attribute_registry_enum([Def | Rest]) ->
    case is_record_def(Def, p_enum) of
        true ->
            Dirs = element(4, Def),
            case has_directive_named(Dirs, <<"attributeRegistry">>) of
                true -> {ok, Def};
                false -> find_attribute_registry_enum(Rest)
            end;
        false ->
            find_attribute_registry_enum(Rest)
    end.

-spec extract_attribute_values([term()]) -> [map()].
extract_attribute_values(Variants) ->
    [attribute_value_def(V) || V <- Variants, has_directive_named(element(4, V), <<"attribute">>)].

-spec attribute_value_def(term()) -> map().
attribute_value_def(EnumValue) ->
    Symbol = element(2, EnumValue),
    Dirs = element(4, EnumValue),
    AttrDir = find_directive_named(Dirs, <<"attribute">>),
    Args = case AttrDir of
        {ok, D} -> element(3, D);
        undefined -> #{}
    end,
    Datatype = parse_value_token(get_arg(Args, <<"datatype">>)),
    Name = parse_value_string(get_arg(Args, <<"name">>)),
    Uri = parse_value_string(get_arg(Args, <<"uri">>)),
    Description = parse_value_string(get_arg(Args, <<"description">>)),
    IsArray = parse_value_boolean(get_arg(Args, <<"array">>), true),
    #{
        symbol => Symbol,
        datatype => Datatype,
        is_array => IsArray,
        name => Name,
        qualname => Name,
        uri => Uri,
        description => Description
    }.

%% Object type extraction (records and templates)

-spec extract_record_template_types([term()]) -> [map()].
extract_record_template_types(Defs) ->
    lists:foldl(
        fun(Def, Acc) ->
            case is_record_def(Def, p_object) of
                true ->
                    case extract_object_type_info(Def) of
                        undefined -> Acc;
                        TypeInfo -> [TypeInfo | Acc]
                    end;
                false ->
                    Acc
            end
        end,
        [],
        Defs
    ).

-spec extract_object_type_info(term()) -> map() | undefined.
extract_object_type_info(Object) ->
    Id = element(2, Object),
    TypeName = case Id of
        {name, _, N} -> N;
        N when is_binary(N) -> N
    end,
    Dirs = element(5, Object),
    RecordAttr = case find_directive_named(Dirs, <<"record">>) of
        {ok, RecDir} ->
            parse_value_token(get_arg(element(3, RecDir), <<"attribute">>));
        undefined ->
            undefined
    end,
    TemplateName = case find_directive_named(Dirs, <<"template">>) of
        {ok, TplDir} ->
            case get_arg(element(3, TplDir), <<"name">>) of
                undefined -> <<>>;
                V -> parse_value_string(V)
            end;
        undefined ->
            undefined
    end,
    case {RecordAttr, TemplateName} of
        {undefined, undefined} ->
            undefined;
        _ ->
            Fields = element(4, Object),
            Uses = extract_use_references(Fields),
            #{
                type_name => TypeName,
                record_attribute => RecordAttr,
                template_name => TemplateName,
                uses => Uses
            }
    end.

-spec extract_use_references([term()]) -> [map()].
extract_use_references(Fields) ->
    [
        use_field_def(F)
        || F <- Fields,
           is_record_def(F, p_field_def),
           has_directive_named(element(6, F), <<"use">>)
    ].

-spec use_field_def(term()) -> map().
use_field_def(Field) ->
    Id = element(2, Field),
    FieldName = case Id of
        {name, _, N} -> N;
        N when is_binary(N) -> N
    end,
    Dirs = element(6, Field),
    Attr = case find_directive_named(Dirs, <<"use">>) of
        {ok, UseDir} ->
            parse_value_token(get_arg(element(3, UseDir), <<"attribute">>));
        undefined ->
            undefined
    end,
    #{field_name => FieldName, attribute => Attr}.

%% Directive helpers

-spec is_record_def(term(), atom()) -> boolean().
is_record_def(Term, RecName) ->
    is_tuple(Term) andalso tuple_size(Term) > 0 andalso element(1, Term) =:= RecName.

-spec has_directive_named([term()], binary()) -> boolean().
has_directive_named(Dirs, Name) ->
    find_directive_named(Dirs, Name) =/= undefined.

-spec find_directive_named([term()], binary()) -> {ok, term()} | undefined.
find_directive_named(Dirs, Name) ->
    case [D || D <- Dirs, is_directive_named(D, Name)] of
        [Found | _] -> {ok, Found};
        [] -> undefined
    end.

-spec is_directive_named(term(), binary()) -> boolean().
is_directive_named(Dir, Name) when is_tuple(Dir), element(1, Dir) =:= directive ->
    DirId = element(2, Dir),
    case DirId of
        {name, _, N} -> N =:= Name;
        N when is_binary(N) -> N =:= Name;
        N when is_atom(N) -> atom_to_binary(N, utf8) =:= Name
    end;
is_directive_named(_, _) ->
    false.

%% Directive arg access

-spec get_arg(term(), binary()) -> term().
get_arg(Args, Key) when is_list(Args) ->
    case lists:keyfind(Key, 1, [{Kname, V} || {{name, _, Kname}, V} <- Args]) of
        false -> undefined;
        {_, V} -> V
    end;
get_arg(Args, Key) when is_map(Args) ->
    maps:get(Key, Args, undefined);
get_arg(_, _) ->
    undefined.

%% Value extraction from graphql-erlang's value() type

-spec parse_value_string(term()) -> binary() | undefined.
parse_value_string({string, Bin, _}) -> Bin;
parse_value_string(Bin) when is_binary(Bin) -> Bin;
parse_value_string(_) -> undefined.

-spec parse_value_token(term()) -> binary() | undefined.
parse_value_token({enum, Bin}) -> Bin;
parse_value_token({string, Bin, _}) -> Bin;
parse_value_token({name, _, Bin}) -> Bin;
parse_value_token(Bin) when is_binary(Bin) -> Bin;
parse_value_token(undefined) -> undefined;
parse_value_token(Atom) when is_atom(Atom) -> atom_to_binary(Atom, utf8);
parse_value_token(_) -> undefined.

-spec parse_value_boolean(term(), boolean()) -> boolean().
parse_value_boolean({bool, Val, _}, _Default) -> Val;
parse_value_boolean(true, _Default) -> true;
parse_value_boolean(false, _Default) -> false;
parse_value_boolean(_, Default) -> Default.

%% Validation

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

-spec to_binary(term()) -> binary().
to_binary(Value) when is_binary(Value) -> Value;
to_binary(Value) when is_list(Value) -> unicode:characters_to_binary(Value);
to_binary(Value) -> unicode:characters_to_binary(io_lib:format("~p", [Value])).
