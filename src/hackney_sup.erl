%%% -*- erlang -*-
%%%
%%% This file is part of hackney released under the Apache 2 license.
%%% See the NOTICE for more information.
%%%
%%% Copyright (c) 2012-2014 Benoît Chesneau <benoitc@e-engura.org>
%%%

-module(hackney_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).


-include("hackney.hrl").

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    {ok, Pid} = supervisor:start_link({local, ?MODULE}, ?MODULE, []),
    %% start the pool handler
    PoolHandler = hackney_app:get_app_env(pool_handler, hackney_pool),
    ok = PoolHandler:start(),

    %% finish to start the application
    {ok, Pid}.

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
    Manager = ?CHILD(hackney_manager, worker),

    %% server maintaing some meta data related to connections
    Server = {hackney_server,
              {hackney_server, start_link, []},
              {permanent, 5000, worker, [hackney_server]}},

    %% cache used to store host lookup prefereneces
    CacheSize = application:get_env(hackney, lookup_cache_size, ?DEFAULT_CACHE_SIZE),
    Cache = {?LOOKUP_LCACHE,
             {lru, start_link, [CacheSize]},
             {permananent, 5000, worker, [lru]}},


    {ok, { {one_for_one, 10, 1}, [Manager, Server, Cache]}}.
