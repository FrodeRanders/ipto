-module(erepo_lock).

-export([lock/3, unlock/1]).

lock(UnitRef, LockType, Purpose) ->
    erepo_db:lock_unit(UnitRef, LockType, Purpose).

unlock(UnitRef) ->
    erepo_db:unlock_unit(UnitRef).
