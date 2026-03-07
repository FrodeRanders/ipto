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
-module(ipto_running_statistics).

-export([new/0, add_sample/2, to_map/1]).

-type running_stats() :: #{
    count := non_neg_integer(),
    min := number() | undefined,
    max := number() | undefined,
    mean := float(),
    m2 := float(),
    total := integer()
}.

-spec new() -> running_stats().
new() ->
    #{count => 0, min => undefined, max => undefined, mean => 0.0, m2 => 0.0, total => 0}.

-spec add_sample(running_stats(), number()) -> running_stats().
add_sample(Stats0, Sample0) ->
    Sample = to_float(Sample0),
    Count0 = maps:get(count, Stats0),
    Mean0 = maps:get(mean, Stats0),
    M20 = maps:get(m2, Stats0),
    Min0 = maps:get(min, Stats0),
    Max0 = maps:get(max, Stats0),
    Total0 = maps:get(total, Stats0),

    Min = case Min0 of undefined -> Sample; _ -> erlang:min(Min0, Sample) end,
    Max = case Max0 of undefined -> Sample; _ -> erlang:max(Max0, Sample) end,

    Count = Count0 + 1,
    Delta = Sample - Mean0,
    Mean = Mean0 + (Delta / Count),
    Delta2 = Sample - Mean,
    M2 = M20 + (Delta * Delta2),
    Total = Total0 + erlang:round(Sample),

    Stats0#{count => Count, min => Min, max => Max, mean => Mean, m2 => M2, total => Total}.

-spec to_map(running_stats()) -> map().
to_map(Stats) ->
    Count = maps:get(count, Stats),
    Mean = maps:get(mean, Stats),
    M2 = maps:get(m2, Stats),
    Variance = case Count > 1 of true -> M2 / (Count - 1); false -> undefined end,
    StdDev = case Variance of undefined -> undefined; _ -> math:sqrt(Variance) end,
    CV = case {Count > 1, StdDev} of
        {true, S} when is_float(S), erlang:abs(Mean) > 0.0 -> 100.0 * S / Mean;
        _ -> undefined
    end,
    #{
        count => Count,
        min => maps:get(min, Stats),
        max => maps:get(max, Stats),
        mean => Mean,
        total => maps:get(total, Stats),
        variance => Variance,
        stddev => StdDev,
        cv => CV
    }.

-spec to_float(number()) -> float().
to_float(Value) when is_integer(Value) ->
    float(Value);
to_float(Value) when is_float(Value) ->
    Value.
