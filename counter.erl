%%% Use a write-only counter to take possession of an item.
%%% If the counter returns 1, you get to own the resource. Otherwise,
%%% you don't. Once you're done, you have to set the counter back to
%%% 0 or let an associated resource manager do it.
%%% To know who to contact, use another round-robin counter that
%%% gradually iterates over all available workers as time goes,
%%% assuring an even distribution of requests.
-module(counter).
-export([start/1, get_resource/1, put_resource/2, update/1, stop/0]).

start(Num) ->
    Heir = spawn(fun() -> timer:sleep(infinity) end),
    dispatch_table = ets:new(dispatch_table, [named_table, set, public, {heir,Heir,handover}, {write_concurrency,true}]),
    worker_table = ets:new(worker_table, [named_table, set, public, {heir,Heir,handover}, {read_concurrency,true}]),
    true = ets:insert(dispatch_table, {ct,0}),
    [start_watcher(N) || N <- lists:seq(1,Num)],
    Num.

get_resource(Num) ->
    case is_free(Id = dispatch_id(Num)) of
        true ->
            [{_,Pid}] = ets:lookup(worker_table, Id),
            sync(Pid, {get,self()});
        false ->
            busy
    end.

put_resource(_Num, {Pid,Resource}) ->
    async(Pid, {put, Resource}).

stop() ->
    ets:foldl(fun({size,_},_) ->
                    ok;
                 ({_,Pid},_) ->
                    sync(Pid,stop)
    end, [], worker_table),
    ets:delete(worker_table),
    ets:delete(dispatch_table).

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
    ets:insert(worker_table, {Id, self()}),
    ets:insert(dispatch_table, {Id, 0}),
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
            reply(From, {ok, {self(),Res}}),
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
            set_free(Id),
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

%%% ID handling
dispatch_id(Num) ->
    %ets:update_counter(dispatch_table, ct, {2,1,Num,1}).
    erlang:phash2(self(),Num)+1.

is_free(Id) ->
    case ets:update_counter(dispatch_table, Id, {2,1}) of
        1 -> true;
        _ -> false
    end.

set_free(Id) ->
    ets:insert(dispatch_table, {Id,0}).
