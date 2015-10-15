-module(hackney_socks_connection).
-export([connect/5, connect/6,
         do_handshake/4]).


-define(TIMEOUT, 5000).

connect(Host, Port, TargetHost, TargetPort, Options) ->
    connect(Host, Port, TargetHost, TargetPort, Options, infinity).

connect(Host, Port, TargetHost, TargetPort, Options, Timeout) ->
    case gen_tcp:connect(Host, Port, Options, Timeout) of
        {ok, Socket} ->
            do_handshake(Socket, TargetHost, TargetPort, Options);
        Error ->
            Error
    end.

do_handshake(Socket, Host, Port, Options) ->
    ProxyUser = proplists:get_value(user, Options),
    ProxyPass = proplists:get_value(password, Options, <<>>),
    Result = case ProxyUser of
        undefined ->
            %% no auth
            ok = gen_tcp:send(Socket, << 5, 1, 0 >>),
            case gen_tcp:recv(Socket, 2, ?TIMEOUT) of
                {ok, << 5, 0 >>} ->
                    do_connection(Socket, Host, Port);
                {ok, _Reply} ->
                    {error, unknown_reply};
                Error ->
                    Error
            end;
        _ ->
            case do_authentication(Socket, ProxyUser, ProxyPass) of
                ok ->
                    do_connection(Socket, Host, Port);
                Error ->
                    Error
            end
    end,
    case Result of
        ok ->
            {ok, Socket};
        _ ->
            %% error, close the underlying socket and return
            catch gen_tcp:close(Socket),
            Result
    end.

do_authentication(Socket, User, Pass) ->
    ok = gen_tcp:send(Socket, << 5, 1, 2 >>),
    case gen_tcp:recv(Socket, 2, ?TIMEOUT) of
        {ok, <<5, 0>>} ->
        	ok;
        {ok, <<5, 2>>} ->
            UserLength = byte_size(User),
            PassLength = byte_size(Pass),
            Msg = iolist_to_binary([<< 1, UserLength >>,
                                    User, << PassLength >>,
                                    Pass]),
            ok = gen_tcp:send(Socket, Msg),
            case gen_tcp:recv(Socket, 2, ?TIMEOUT) of
                {ok, <<1, 0>>} ->
                    ok;
                _ ->
                    {error, {proxy_error, not_authenticated}}
            end;
        _ ->
            {error, {proxy_error, not_authenticated}}
    end.


do_connection(Socket, Host, Port) ->
    Addr = case inet_parse:address(Host) of
        {ok, {IP1, IP2, IP3, IP4}} ->
            << 1, IP1, IP2, IP3, IP4, Port:16 >>;
        {ok, {IP1, IP2, IP3, IP4, IP5, IP6, IP7, IP8}} ->
            << 4, IP1, IP2, IP3, IP4, IP5, IP6, IP7, IP8, Port:16 >>;
        _ ->
            %% domain name
            case inet:getaddr(Host, inet) of
                {ok, {IP1, IP2, IP3, IP4}} ->
                    << 1, IP1, IP2, IP3, IP4, Port:16 >>;
                _Else ->
                    case inet:getaddr(Host, inet6) of
                         {ok, {IP1, IP2, IP3, IP4, IP5, IP6, IP7, IP8}} ->
                            << 4, IP1, IP2, IP3, IP4, IP5, IP6, IP7,
                              IP8, Port:16 >>;
                        _ ->
                            Host1 = list_to_binary(Host),
                            HostLength = byte_size(Host1),
                            << 3, HostLength, Host1/binary, Port:16 >>
                    end
            end
    end,
    ok = gen_tcp:send(Socket, << 5, 1, 0, Addr/binary >>),
    case gen_tcp:recv(Socket, 10, ?TIMEOUT) of
        {ok, << 5, 0, 0, BoundAddr/binary >>} ->
            check_connection(BoundAddr);
        {ok, _} ->
            {error, {proxy_error, badarg}};
        Error ->
            Error
    end.


check_connection(<< 3, _DomainLen:8, _Domain/binary >>) ->
    ok;
check_connection(<< 1, _Addr:32, _Port:16 >>) ->
    ok;
check_connection(<< 4, _Addr:128, _Port:16 >>) ->
   ok;
check_connection(_) ->
    {error, {proxy_error, no_connection}}.
