-module(hackney_pool_sup).
-behaviour(supervisor).

-export([start_link/3]).
-export([init/1]).

start_link(Ref, NbConnectors, PoolOpts) ->
    supervisor:start_link(?MODULE, {Ref, NbConnectors, PoolOpts}).


init({Ref, NbAcceptors, FallbackTime, PoolOpts}) ->
    %% pool, caching sockets
    Pool = {hackney_pool,
            {hackney_pool, start_link, [Ref, PoolOpts]},
            permanent, infinity, worker, [hackney_pool]},

    %% connection supervisor
    ConnectorSup = {hackney_connector_sup,
                    {hackney_connector_sup, start_link, [Ref, NbAcceptors, FallbackTime]},
                    permanent, supervisor, [hackney_connector_sup]},

    {ok, {{rest_for_one, 10, 10}, [Pool, ConnectorSup]}}.
