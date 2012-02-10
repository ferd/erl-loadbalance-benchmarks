%% Prototype of a pool model inspired by hash rings.
%% The gist of it is that each worker is a tiny pool
%% where resources are asked based on a hash of things like
%% the Pid and/or a timestamp to get an even distribution
%% between all processes answering messages.
%% Then resource acquisition is attempted by looking up a worker
%% by a hash shared by all workers.
%% This should greatly reduce the contention on each resource.
-module(hrworkers).
-export([start/1, get_resource/1, put_resource/2, update/1, stop/0]).

start(Num) ->
    Heir = spawn(fun() -> timer:sleep(infinity) end),
    ?MODULE = ets:new(?MODULE, [named_table, set, public, {heir,Heir,handover},{read_concurrency,true}]),
    true = ets:insert(?MODULE, {size,Num}),
    [start_watcher(N) || N <- lists:seq(1,Num)],
    [{size,Num} | ets:tab2list(?MODULE)].

get_resource(Table) ->
    %[{_,Pid}] = ets:lookup(?MODULE, find_id()),
    {_,Pid} = lists:keyfind(find_id(Table),1,Table),
    sync(Pid,{get, self()}).

put_resource(Table,Resource) ->
    %[{_,Pid}] = ets:lookup(?MODULE, find_id()),
    {_,Pid} = lists:keyfind(find_id(Table),1,Table),
    async(Pid, {put, Resource}).

stop() ->
    ets:foldl(fun({size,_},_) ->
                    ok;
                 ({_,Pid},_) ->
                    sync(Pid,stop)
    end, [], ?MODULE),
    ets:delete(?MODULE).

%%% PRIV

start_watcher(Id) ->
    S = self(),
    R = make_ref(),
    Pid = spawn(fun() -> init_watcher(Id,S,R) end),
    MonRef = erlang:monitor(process,Pid),
    receive
        R -> erlang:demonitor(MonRef,[flush]);
        {'DOWN', MonRef, process, _, Reason} -> erlang:error({badstart,Reason})
    end.

init_watcher(Id,Parent,Ref) ->
    ets:insert(?MODULE, {Id, self()}),
    Parent ! Ref,
    Res = make_ref(),
    loop({Id,Res,undefined,undefined}).

%% res available
loop(S = {Id,Res,undefined,undefined}) ->
    receive
        update -> ?MODULE:update(S);
        {From, stop} -> terminate(From, S);
        {From, {get, To}} ->
            Ref = erlang:monitor(process,To),
            reply(From, {ok, Res}),
            loop({Id,Res,From,Ref});
        {put, _Resource} -> % unexpected
            loop(S);
        {'DOWN', _, process, _Pid, _Reason} -> % unexpected
            loop(S)
    end;
%% res unavailable
loop(S = {Id,Res, OwnerPid, Ref}) ->
    receive
        update -> ?MODULE:update(S);
        {From, stop} -> terminate(From, S);
        {From, {get, _To}} ->
            reply(From, busy),
            loop(S);
        {put, Res} ->
            erlang:demonitor(Ref, [flush]),
            loop({Id,Res,undefined,undefined});
        {put, _WeirdRes} -> % undexpected
            loop(S);
        {'DOWN', Ref, process, OwnerPid, _Reason} ->
            NewRes = make_ref(),
            loop({Id, NewRes, undefined, undefined});
        {'DOWN', _, process, _Pid, _Reason} -> % unexpected
            loop(S)
    end.

%%% UTILS
async(Pid,Msg) ->
    Pid ! Msg.

sync(Pid,Msg) ->
    Ref = erlang:monitor(process, Pid),
    Pid ! {{self(), Ref}, Msg},
    receive
        {Ref, Reply} ->
            erlang:demonitor(Ref, [flush]),
            Reply;
        {'DOWN', Ref, process, _, Reason} ->
            error(Reason)
    end.

reply(_From = {Pid, Ref}, Reply) ->
    Pid ! {Ref, Reply}.

update(State) -> loop(State).

terminate(From, _State) ->
    reply(From, ok).

find_id(Table) ->
    {size,Size} = lists:keyfind(size,1,Table),
    erlang:phash2(self(),Size)+1.
