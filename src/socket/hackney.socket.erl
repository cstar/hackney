-module(hackney_socket).

-include("hackney_socket.hrl").

%% public api
-export([connect/4, connect/5]).

-export([exit_if_closed/1]).

connect(Transport, Host, Port, Options) ->
    connect(Transport, Host, Port, Options, infinity).

connect(Transport, Host, Port, Options, Timeout) ->
    Pool = proplists:get_value(pool, Options, default),
    Prefix = proplists:get_value(prefix, Options, ""),


    ok.





groupname(Parts) ->
    string:join(Parts, "/").

netloc(Host, Port) when is_list(Host), is_integer(Port) ->
    Host ++ ":" ++ integer_to_list(Port).


exit_if_closed({error, closed}) ->
    exit({error, closed});
exit_if_closed(Res) ->
    Res.

