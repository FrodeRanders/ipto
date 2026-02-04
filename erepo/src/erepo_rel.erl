-module(erepo_rel).

-export([add/3, remove/3]).

add(UnitRef, RelType, OtherUnitRef) ->
    erepo_db:add_relation(UnitRef, RelType, OtherUnitRef).

remove(UnitRef, RelType, OtherUnitRef) ->
    erepo_db:remove_relation(UnitRef, RelType, OtherUnitRef).
