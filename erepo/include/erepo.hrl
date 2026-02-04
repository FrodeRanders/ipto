-ifndef(EREPO_HRL).
-define(EREPO_HRL, true).

-define(STATUS_PENDING_DISPOSITION, 1).
-define(STATUS_PENDING_DELETION, 10).
-define(STATUS_OBLITERATED, 20).
-define(STATUS_EFFECTIVE, 30).
-define(STATUS_ARCHIVED, 40).

-record(unit_ref, {
    tenantid :: integer(),
    unitid :: integer(),
    unitver :: integer() | undefined
}).

-record(unit, {
    tenantid :: integer(),
    unitid :: integer() | undefined,
    unitver = 1 :: integer(),
    corrid :: binary(),
    status = ?STATUS_EFFECTIVE :: integer(),
    unitname = undefined :: binary() | undefined,
    attributes = [] :: list(),
    created = undefined :: undefined | binary(),
    modified = undefined :: undefined | binary(),
    is_new = true :: boolean(),
    is_read_only = false :: boolean()
}).

-endif.
