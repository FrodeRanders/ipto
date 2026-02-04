-module(erepo_assoc).

-export([add/3, remove/3]).

add(UnitRef, AssocType, RefString) ->
    erepo_db:add_association(UnitRef, AssocType, RefString).

remove(UnitRef, AssocType, RefString) ->
    erepo_db:remove_association(UnitRef, AssocType, RefString).
