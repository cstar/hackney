-module(hackney_pool).

%% public API
-export([open/5, open/6, open/7,
         close/1,
         release/1]).

%% hackney internal api
-export([register_connector/2]).
-export([start_link/2]).

%% pool internals
-export([init/1]).
-export([system_continue/3]).
-export([system_terminate/4]).
-export([system_code_change/4]).

-include("hackney_socket.hrl").

-record(state, {
        name :: atom(),
        parent :: pid(),
        idle_timeout :: non_neg_integer(),
        group_limit = 8,
        proxy_limit = 20,
        max_conns = 200,
        refs,
        sockets,
        connectors=[],
        next=0}).


-record(group, {name,
                conns = [],
                pending = queue:new(),
                num_connect_jobs = 0,
                backup = false}).


-define(DEFAULT_IDLE_TIMEOUT, 150000). %% default time until a connectino is forced to closed
-define(DEFAULT_GROUP_LIMIT, 6). %% max number of connections kept for a group
-define(DEFAULT_PROXY_LIMIT, 20). %% max number of connections cached / proxy
-define(DEFAULT_MAX_CONNS, 200). %% maximum number of connections kept

request(Pool, Group, Host, Port, Options, Timeout) ->
    Expires = case Timeout of
                  infinity -> infinity;
                  Timeout -> now_in_ms() + Timeout
              end,
    Tag = erlang:monitor(process, Pool),
    From = {Pool, Tag},
    Req = {Host, Port, Options, Timeout},
    catch Pool ! {request, From, Group, Req, Expires},
    receive
        {Tag, {ok, HS}} ->
            {ok, HS};
        {'DOWN', Tag, _, _, Reason} ->
            {error, Reason};
        {Tag, Error} -> 
            Error
    after Timeout ->
        {error, timeout}
    end.


close(HS) ->
    hackney_socket:setopts(HS, [{active, false}]),
    case sync_socket(HS) of
        true -> release(HS);
        _Error ->
            catch hackney_socket:close(HS),
            {error, sync_error}
    end.


release(#hackney_socket{pool=Pool}=HS) ->
    hackney_socket:controlling_process(HS, Pool),
    Tag = erlang:monitor(process, Pool),
    Pool ! {release, {self(), Tag}, HS},
    %% we directly pass the socket control to a pending connection if any. if
    %% the pool can't accept the socket we kill it.
    receive
        {Tag, ok} ->
            ok;
        {'DOWN', Tag, _, _, Reason} ->
            hackney_socket:close(HS),
            {error, Reason};
        {Tag, Error} ->
            hackney_socket:close(HS),
            Error
    end.

register_connector(Pool, Pid) ->
    Pool ! {register_connector, Pid},
    receive Pool -> ok end.



start_link(Ref, Opts) ->
    proc_lib:start_link(?MODULE, init, [self(), Ref, Opts]).


init([Parent, Ref, Opts]) ->
    ok = hackney_server:set_pool(Ref, self()),

    IdleTimeout = hackney_util:get_opt(idle_timeout, Opts, ?DEFAULT_IDLE_TIMEOUT),
    GroupLimit = hackney_util:get_opt(group_limit, Opts, ?DEFAULT_GROUP_LIMIT),
    ProxyLimit =  hackney_util:get_opt(group_limit, Opts, ?DEFAULT_PROXY_LIMIT),
    MaxConns =  hackney_util:get_opt(max_connections, Opts, ?DEFAULT_MAX_CONNS),
    %% init tables
    Refs = ets:new(hackney_pool_refs, [bag]),
    Sockets = ets:new(hackney_pool_sockets, [set]),

    ok = proc_lib:init_ack(Parent, {ok, self()}),
    loop(#state{name = Ref,
                parent = Parent,
                idle_timeout = IdleTimeout,
                group_limit = GroupLimit,
                proxy_limit = ProxyLimit,
                max_conns = MaxConns,
                refs = Refs,
                sockets = Sockets}).


loop(State=#state{parent=Parent}) ->
    receive
        {request, {Pid, Tag} = From, Group, CReq, Expires} ->
            case reuse_connection(Group, State) of
                {ok, HS} ->
                    hackney_socket:controlling_process(HS, Pid),
                    Pid ! {Tag, {ok, HS}},
                    loop(State);
                no_socket ->
                    %% insert pending request
                    ets:insert(State#state.refs, {{pending, Group}, {From, Expires}),
                    State2 = request_socket(Group, CReq, State),
                    loop(State2)
            end;
        {preconnect, {Pid, Tag}, Group, CReq} ->
            State2 = request_socket(Group, CReq, State),
            Pid ! {Tag, ok},
            loop(State2);
        {release, {Pid, Tag}, HS} ->
            Reply = release_socket(HS),
            Pid ! {Ref, Reply},
            loop(State);
        {register_connector, Pid} ->
            _ = erlang:monitor(process, Pid),
            loop(State#state{connectors=[Pid | State#state.connectors]});
        {'DOWN', _, process, Pid} ->
            Connectors2 = State#state.connectors -- [Pid],
            loop(State#state{connectors=Connectors2});
        {'EXIT', Parent, Reason} ->
            exit(Reason);
        {system, From, Request} ->
            system:handle_system_msg(Request, From, Parent, ?MODULE, [],
                                     State);
        Msg ->
            error_logger:error_msg(
              "Hackney pool ~p received an unexped message ~p",
              [State#state.name, Msg])
    end.

system_continue(_, _, State) ->
    loop(State).

system_terminate(Reason, _, _, _State) ->
    exit(Reason).

system_code_change(Misc, _, _, _) ->
    {ok, Misc}.


reuse_connection(Group, State) ->
    Refs = ets:lookup(State#state.refs, {conns, Group}),
    reuse_connection1(Refs, State).


reuse_connection1([], _State) ->
    no_socket;
reuse_connection1([{Group, S} | Rest], State) ->
    [{_, _, HS, T}] = ets:lookup(State#state.sockets, S),
    ets:delete_object(State#state.refs, {Group, S}),
    ets:delete(State#state.sockets, S),
    cancel_timer(T, S),
    hackney_socket:setopts(HS, [{active, false}]),
    case sync_socket(HS) of
        true ->
            {ok, HS};
        false ->
            reuse_connection1(Rest, State)
    end.

request_socket(Group, Req, State) ->
    %% we simply balance connections tasks between connectors using an RR algorithm
    {Connector, State2} = pick_connector(State),
    catch Connector ! {connect, Group, Req},
    State2.


pick_connector(State=#state{connectors=Connectors}) ->
    [Connector | Rest] = Connectors,
    {Connector, State#state{connectors = Rest ++ [Connector]}}.


release_connection(#hackney_socket{group=Group}=HS, State) ->
    Pending = ets:lookup(State#state.refs, {pending, Group}),
    case Pending of
        [] -> cache_connection(HS, State);
        _ -> dispatch_connection(Pending, HS, State)
    end.

cache_connection(HS=#hackney_socket{sock=Sock, group=Group}, State) ->
    Conns = ets:lookup(State#state.refs, {conns, Group}),
    TotalGroup = length(Conns),
    TotalConns = ets:info(State#state.sockets, size),

    Limit = case Group of
            <<"proxy/">> -> State#state.proxy_limit;
            _ -> State#state.group_limit
    end,

    case {TotalConns < State#state.max_conns, TotalGroup < Limit} of
        {true, true} ->
            Timer = erlang:send_after(State#state.idle_timeout, self(), {timeout, Sock}),
            ets:insert(State#state.sockets, {Sock, Group, HS, T}),
            ets:insert(State#state.refs, {{conns, Group}, S}),
            ok;
        {_, _} ->
            {error, max_conns}
    end.

dispatch_connection([{{Pid, Tag}, Expires} | Rest], HS, State) ->
    Now = now_in_ms(),
    if
        Expires >= Now ->
            case catch hackney_socket:controlling_process(HS, Pid) of
                ok ->
                    catch Pid ! {Tag, {ok, JS}},
                    ets:delete_object()



dispatch_connection(HS, Pending, Group, State) ->
    case queue:out(Pending) of
        {{value, {{_Tag, Pid} = Req, Expires}}, Pending2} ->
            Now = now_in_ms(),
            if
                Expires >= Now ->
                    case catch hackney_socket:controlling_process(HS, Pid) of
                        ok ->
                            #hackney_socket{count=C}=HS,
                            gen_server:reply(Req, HS#hackney_socket{count=C + 1}),
                            Group2 = Group#group{pending=Pending2},
                            Groups = dict:store(Group#group.name, Group2, State#state.groups),
                            State#state{groups=Groups};
                        _Error ->
                            State
                    end;
                true ->
                    %% connection expired we try a new one
                    dispatch_connection(HS, Pending2, Group, State)
            end;
        {empty, _} ->
            if
                Pending =/= Group#group.pending ->
                    %% no more pending connections, all expired, we update the
                    %% group and the state.
                    Group2 = Group#group{pending=Pending},
                    Groups = dict:store(Group#group.name, Group2, State#state.groups),
                    store_connection(HS, Group, State#state{groups=Groups}, false);
                true ->
                    store_connection(HS, Group, State, false)
            end
    end.

store_connection(HS, Group, State, override_limit) ->
    store_connection1(HS, Group, State);
store_connection(HS, Group, State, _) ->
    MaxIdle = case Group#group.name of
                 <<"proxy/">> -> State#state.proxy_limit;
                 _ -> State#state.group_limit
              end,
    NbConns = length(Group#group.conns),
    PoolSize = dict:size(State#state.sockets),
    if
        NbConns >= MaxIdle;  NbConn, PoolSize >= State#state.max_connections -> State;
        true -> store_connection1(HS, Group, State)
    end.


store_connection1(HS, Group, State) ->
    #hackney_socket{sock=Sock} = HS,
    Group2 = Group#group{conns = [ HS | Group#group.conns ]},
    Groups = dict:store(Group#group.name, Group2, State#state.groups),
    %% setup timeout timer
    Timer = erlang:send_after(State#state.idle_timeout, self(), {timeout, Sock}),
    Sockets = dict:store(Sock, {HS, Timer}, State#state.sockets),
    State#state{sockets=Sockets, groups=Groups}.

remove_socket(#hackney_socket{sock=Sock}, State) ->
    remove_socket(Sock, State);
remove_socket(Sock, #state{groups=Groups, sockets=Sockets}=State) ->
    case dict:find(Sock, Sockets) of
        {ok, {#hackney_socket{sock=Sock, group=Group}=HS, Timer}} ->
            _ = cancel_timer(Sock, Timer),
            catch hackney_socket:close(HS),
            _ = sync_socket(HS),
            NewConns = update(Group, lists:delete(HS, dict:fetch(Group, Conns)), Conns),
            NewSocks = dict:erase(Sock, Socks),
            State#state{conns=NewConns, socks=NewSocks};
        error ->
            State
    end.


cancel_timer(Timer, S) ->
    case erlang:cancel_timer(Timer) of
        false ->
            receive
                {timeout, Socket} -> ok
            after
                0 -> ok
            end;
        _ -> ok
    end.

%% check that no events from the sockets is received after setting it to
%% passive.
sync_socket(#hackney_socket{transport=Transport, sock=Sock} ) ->
    {Msg, MsgClosed, MsgError} = Transport:messages(),
    receive
        {Msg, Sock, _} -> false;
        {MsgClosed, Sock} -> false;
        {MsgError, Sock, _} -> false
    after 0 ->
              true
    end.

now_in_ms() -> timer:now_diff(os:timestamp(), {0, 0, 0}).

send(Pid, Msg) ->
    catch Pid ! Msg.
