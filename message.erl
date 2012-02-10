%% Basic pool implementation -- one process dispatching.
-module(message).
-export([start/1, get_resource/1, put_resource/2, update/1, stop/0]).

-record(state, {num_resources,
                max_resources,
                owners,
                resources=[]}).

start(Num) ->
    proc_lib:spawn_link(fun() -> init(Num) end),
    ok.

get_resource(_) ->
    sync({get, self()}).

put_resource(_,Resource) ->
    async({put, Resource}).

stop() ->
    sync(stop).

%%% Private

%%% UTILS
async(Msg) ->
    ?MODULE ! Msg.

sync(Msg) ->
    Ref = erlang:monitor(process, ?MODULE),
    ?MODULE ! {{self(), Ref}, Msg},
    receive
        {Ref, Reply} ->
            erlang:demonitor(Ref, [flush]),
            Reply;
        {'DOWN', Ref, process, _, Reason} ->
            error(Reason)
    end.

reply(_From = {Pid, Ref}, Reply) ->
    Pid ! {Ref, Reply}.

init(Num) ->
    register(?MODULE, self()),
    loop(#state{num_resources=Num,
                max_resources=Num,
                owners=ets:new(owners, [set,private]),
                resources=[make_ref() || _ <- lists:seq(1,Num)]}).

loop(S = #state{num_resources=Max, max_resources=Max, owners=O, resources=R}) ->
    receive
        update -> ?MODULE:update(S);
        {From, stop} -> terminate(From, S);
        {From, {get, To}} ->
            case R of
                [H|T] ->
                    Ref = erlang:monitor(process, To),
                    ets:insert(O, {H, Ref}),
                    reply(From, {ok, H}),
                    loop(S#state{resources=T});
                [] ->
                    reply(From, busy),
                    loop(S)
            end;
        {put, Resource} ->
            case ets:lookup(O, Resource) of
                [{_, Ref}] ->
                    ets:delete(O, Resource),
                    erlang:demonitor(Ref, [flush]),
                    loop(S#state{resources=[Resource|R]});
                [] ->
                    loop(S)
            end;
        {'DOWN', Ref, process, _Pid, _Reason} ->
            ets:match_delete(O, {'$1',Ref}),
            loop(S#state{resources=[make_ref() | R]})
    end.

update(State) -> loop(State).

terminate(From, _State) ->
    reply(From, ok).
