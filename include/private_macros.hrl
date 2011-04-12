-define(PV(X,P), proplists:get_value(X,P,false)).
-ifdef(debug).
  -define(info(M,P),
     error_logger:info_msg("[~p:~p] "++M++"~n", [?MODULE,?LINE]++P)).
  -define(info(M),
     error_logger:info_msg("[~p:~p] "++M++"~n", [?MODULE,?LINE])).
-else.
  -define(info(M,P), true).
  -define(info(M), true).
-endif.
-ifdef(verbose).
  -define(verbose(M,P),
     error_logger:info_msg("[~p:~p] "++M++"~n", [?MODULE,?LINE]++P)).
  -define(verbose(M),
     error_logger:info_msg("[~p:~p] "++M++"~n", [?MODULE,?LINE])).
-else.
  -define(verbose(M,P), true).
  -define(verbose(M), true).
-endif.
