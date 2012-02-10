-module(bench).
-export([start/1]).
%%%%%%%%%

start([Mod, NumA, DurA, MsgProcsA]) ->
    Num = list_to_integer(atom_to_list(NumA)),
    Dur = list_to_integer(atom_to_list(DurA)),
    MsgProcs = list_to_integer(atom_to_list(MsgProcsA)),
    io:format("TEST:~n=====~n Mod: ~p~n Units available: ~p~n "
              "Task Duration: ~p~n Processes involved: ~p~n "
              "Duration: 50s~n----------------~n",
              [Mod,Num,Dur,MsgProcs]),
    spawn(fun() -> run(Mod,Num,Dur,MsgProcs) end).

run(Mod,Num,Duration,ProcNum) ->
    bench_stats:start(),
    %% Routers:
    io:format("starting messages~n"),
    Token = Mod:start(Num),
    Workers = spawn_workers(Mod, Duration, {ProcNum,3}, Token),
    io:format("messagers spawned~n"),
    timer:sleep(50000),
    io:format("pausing messagers~n"),
    S = self(),
    [Pid ! {S,pause} || Pid <- Workers],
    [receive _ -> ok end || _ <- Workers],
    io:format("messagers reporting in~n"),
    [Pid ! {S,final_report} || Pid <- Workers],
    [receive _ -> ok end || _ <- Workers],
    io:format("stopping workers~n"),
    Mod:stop(),
    io:format("done, writing reports~n"),
    bench_stats:write_report("./"),
    bench_stats:stop(),
    io:format("messages written~n"),
    init:stop().

worker(Mod, Duration, Interval, Token, 1000, Acc) ->
    bench_stats:report(Acc),
    worker(Mod,Duration,Interval, Token, 0, []);
worker(Mod, Duration, Interval, Token, Ct, Acc) ->
    Ready = {T1=os:timestamp(),ready},
    Work = {T2,_} = case (catch Mod:get_resource(Token)) of
        {ok, Res} ->
            timer:sleep(Duration),
            Mod:put_resource(Token,Res),
            {os:timestamp(),done};
        busy ->
            {os:timestamp(),busy}
    end,
    Delay = {T1,{delay, timer:now_diff(T2,T1)}},
    receive
        {Pid,pause} ->
            Pid ! ok,
            receive
                {Pid,final_report} ->
                    bench_stats:report([Ready,Work,Delay | Acc]),
                    Pid ! ok
            end
    after Interval ->
        worker(Mod,Duration,Interval, Token, Ct+1, [Ready,Work,Delay | Acc])
    end.

spawn_workers(Mod, Duration, {Num,Interval}, Token) ->
    [proc_lib:spawn(fun() ->
        <<A:32, B:32, C:32>> = crypto:rand_bytes(12),
        random:seed(A,B,C),
        worker(Mod, Duration, Interval, Token, 0, [])
     end) || _ <- lists:seq(1,Num)].

