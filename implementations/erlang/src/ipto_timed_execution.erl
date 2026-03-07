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
-module(ipto_timed_execution).

-export([run/2]).

-spec run(atom() | binary() | string(), fun(() -> T)) -> T.
run(Name, Fun) when is_function(Fun, 0) ->
    T0 = erlang:monotonic_time(nanosecond),
    try
        Fun()
    after
        ElapsedNanos = erlang:monotonic_time(nanosecond) - T0,
        ElapsedMillis = ElapsedNanos / 1000000.0,
        ok = ipto_timing_data:add_sample(Name, ElapsedMillis)
    end.
