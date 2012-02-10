-module(bench_stats).
-export([start/0, write_report/1, stop/0, report/1]).
-export([init/0, loop/1]).

start() -> proc_lib:start_link(?MODULE, init, []).

report(Rows) ->
    ets:insert(bench_stats, Rows).
    %?MODULE ! {report, os:timestamp(), Who, What}.

stop() ->
    R = erlang:monitor(process, whereis(?MODULE)),
    ?MODULE ! stop,
    receive
        {'DOWN', R, process, _, _} -> ok
    end.

write_report(Where) ->
    ?MODULE ! {write, Where}.

init() ->
    register(?MODULE,self()),
    ets:new(bench_stats, [duplicate_bag, named_table, public, {write_concurrency, true}]),
    proc_lib:init_ack(ok),
    loop([]).

loop(State) ->
    receive
        stop -> ok;
        {write, Where} ->
            dump_stats(Where),
            loop(State)
    end.

dump_stats(Where) ->
    %% the table is not ordered. Make a new one and go from there.
    CTid = ets:new(compiling_table, [named_table,ordered_set,private]),
    DTid = ets:new(delay_table, [named_table,ordered_set,private]),
    ets:foldl(
        fun({TS={_,_,Micro},{delay,D}}, _) -> % Delays
            {_,{H,Mi,S}} = calendar:now_to_datetime(TS),
            Key = {H,Mi,S,Micro div 100000},
            case ets:lookup(DTid, Key) of
                [] ->
                    ets:insert(DTid, {Key,{0,0}});
                [{Key,{N,Sum}}] ->
                    ets:insert(DTid, {Key,{N+1,Sum+D}})
            end;
           ({TS={_,_,Micro},What}, _) -> % Others
            {_,{H,Mi,S}} = calendar:now_to_datetime(TS),
            Key = {H,Mi,S,Micro div 100000},
            case ets:lookup(CTid, Key) of
                [] ->
                    ets:insert(CTid, {Key,incr(What,{0,0,0})});
                [{Key,Cols}] ->
                    ets:insert(CTid, {Key,incr(What,Cols)})
            end
        end,
        [],
        bench_stats),
    %% Insert the delays back in the compiling_table after getting the avg
    ets:foldl(fun({Key,{N,Sum}}, _) ->
            case ets:lookup(CTid, Key) of
                [] -> ets:insert(CTid, {0,0,0,avg(Sum,N)});
                [{Key,{A,B,C}}] -> ets:insert(CTid, {Key,{A,B,C,avg(Sum,N)}})
            end
        end,
        [],
        DTid),
    %% write the file for times
    {ok, IoDevice} = file:open(Where++"report.csv", [write]),
    file:write(IoDevice, io_lib:format("Time,Ready,Done,Busy,AvgDelayinMs~n", [])),
    ets:foldl(fun({{H,Mi,S,Ms},{Ready,Done,Busy,Delay}},_) ->
                file:write(IoDevice, io_lib:format("~2.10.0B:~2.10.0B:~2.10.0B.~b,~b,~b,~b,~b~n",[H,Mi,S,Ms,Ready,Done,Busy,Delay]));
                 ({{H,Mi,S,Ms},{Ready,Done,Busy}},_) ->
                file:write(IoDevice, io_lib:format("~2.10.0B:~2.10.0B:~2.10.0B.~b,~b,~b,~b,~b~n",[H,Mi,S,Ms,Ready,Done,Busy,0]))
              end,
              [],
              compiling_table),
    file:close(IoDevice).


incr(What, {R,Do,B}) ->
    case What of
        ready -> {R+1, Do, B};
        done  -> {R, Do+1, B};
        busy  -> {R, Do, B+1}
    end;
incr(What, {R,De,Do,B}) ->
    case What of
        ready -> {R+1, De, Do, B};
        dead  -> {R, De+1, Do, B};
        done  -> {R, De, Do+1, B};
        busy  -> {R, De, Do, B+1}
    end.

avg(_Sum,0) -> 0;  % oooh this might look bad!
avg(Sum,N) -> round((Sum/N)/1000). %µs to ms
