-module(hackney_connector).

-export([start_link/2,
         init/2]).

-include("hackney.hrl").
-include("hackney_socket.hrl").

-record(state, {pool,
                fallback_time}).

-define(DEFAULT_LOOKUP_ORDER, [inet6, inet]).

start_link(Pool, FallbackTime) ->
    Pid = spawn_link(?MODULE, init, [Pool, FallbackTime]),
    {ok, Pid}.


init(Pool, FallbackTime) ->
    hackney_pool:registor_acceptor(Pool, self()),
    loop(#state{pool=Pool, fallback_time=FallbackTime}).



loop(State) ->
    receive
        {connect, Group, {Host, Port, Options, Timeout}} ->
            Ref = make_ref(),
            Req = {Group, Host, Port, Options, Timeout},
            LookupOrder = lru:get(?LOOKUP_CACHE, Host, ?DEFAULT_LOOKUP_ORDER),
            handle_connect(Ref, Req, LookupOrder, State),
            loop(State);
        stop ->
            exit(normal)
    end.


handle_connect(Ref, Req, LookupOrder, State) ->
   [F1, F2] = LookupOrder,
    Pid = spawn_connection(Ref, Req, F1, State#state.pool),
    TRef = erlang:send_after(State#state.fallback_time, self(), {Ref, fallback, F2}),
    connect_loop(Ref, TRef, Req, Pid, nil, LookupOrder, State).

connect_loop(Ref, TRef, {Group, H, _, _, _} = Req, P1, P2, LookupOrder, State) ->
    #state{pool=Pool} = State,
    receive
        {Ref, connected, P1} ->
            maybe_kill_job(Ref, TRef, P2),
            ok;
        {Ref, connected, P2} ->
            catch exit(P1, normal),
            flush(Ref, P2),
            %% lookup order is reversed, store it.
            lru:add(?LOOKUP_CACHE, H, lists:reverse(LookupOrder)),
            ok;
        {Ref, fallback, F} ->
            Pid = spawn_connection(Ref, Req, F, State#state.pool),
            connect_loop(Ref, TRef, Req, P1, Pid, LookupOrder, State);
        {Ref, 'DOWN', P1, _Error} ->
            connect_loop(Ref, TRef, Req, P1, P2, LookupOrder, State);
        {Ref, 'DOWN', P2, Error} ->
            Pool ! {connect_error, Group, Error},
            ok
    end.

maybe_kill_job(Ref, TRef, Pid) ->
    case is_pid(Pid) of
        true -> catch exit(Pid, normal);
        false -> erlang:cancel_timer(TRef)
    end,
    flush(Ref, Pid).


flush(Ref, Pid) ->
    receive
        {Ref, connected, Pid} -> ok;
        {Ref, fallback, _Familly} -> ok
    after 0 -> ok
    end.

spawn_connection(Ref, {Group, Host, Port, Opts0, Timeout}, Familly, Pool) ->
    Opts = [Familly | Opts0],
    Connector = self(),
    spawn_link(fun() ->
                       case gen_tcp:connect(Host, Port, Opts, Timeout)of
                           {ok, Sock} ->
                               HS = #hackney_socket{transport=hackney_tcp,
                                                    sock=Sock,
                                                    group=Group,
                                                    pool=Pool},

                               Connector ! {Ref, connected, self()},
                               hackney_pool:release(Pool, HS);
                           Error ->
                               Connector ! {Ref, 'DOWN', self(), Error}
                       end
               end).
