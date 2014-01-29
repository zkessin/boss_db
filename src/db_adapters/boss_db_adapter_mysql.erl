-module(boss_db_adapter_mysql).
-behaviour(boss_db_adapter).
-export([init/1, terminate/1, start/1, stop/0, find/2, find/7]).
-export([count/3, counter/2, incr/3, delete/2, save_record/2]).
-export([push/2, pop/2, dump/1, execute/2, transaction/2]).
-export([get_migrations_table/1, migration_done/3]).
-compile(export_all).
-include_lib("emysql/include/emysql.hrl").
-type nothing()  ::null|undefined.
-type date()     ::calendar:date().
-type datetime() ::calendar:datetime().

-spec start(_) -> 'ok'.
-spec stop() -> 'ok'.
-spec init([any()]) -> {'error',_} | {'ok',pid()}.
-spec terminate(pid() | port()) -> 'true'.
-spec find(atom() | pid() | port() | {atom(),atom()},maybe_improper_list(binary() | maybe_improper_list(any(),binary() | []) | char(),binary() | [])) -> any().
-spec find(_,atom(),maybe_improper_list(),'all' | integer(),integer(),atom(),atom()) -> [any()] | {'error',_}.
-spec count(atom() | pid() | port() | {atom(),atom()},atom() | tuple(),[any()]) -> any().
-spec counter(atom() | pid() | port() | {atom(),atom()},[any()]) -> any().
-spec incr(atom() | pid() | port() | {atom(),atom()},'false' | 'null' | 'true' | 'undefined' | binary() | [any()] | number() | {{_,_,_},{_,_,_}} | {_,_,_},'false' | 'null' | 'true' | 'undefined' | binary() | [any()] | number() | {{_,_,_},{_,_,_}} | {_,_,_}) -> any().
-spec delete(atom() | pid() | port() | {atom(),atom()},maybe_improper_list(binary() | maybe_improper_list(any(),binary() | []) | char(),binary() | [])) -> 'ok' | {'error',_}.
-spec save_record(atom() | pid() | port() | {atom(),atom()},tuple()) -> {'error',_} | {'ok',_}.
-spec push(atom() | pid() | port() | {atom(),atom()},integer()) -> any().
-spec pop(atom() | pid() | port() | {atom(),atom()},integer()) -> any().
-spec dump(_) -> [].
-spec execute(atom() | pid() | port() | {atom(),atom()},binary() | maybe_improper_list(binary() | maybe_improper_list(any(),binary() | []) | byte(),binary() | [])) -> any().
-spec transaction(atom() | pid() | port() | {atom(),atom()},fun()) -> {'aborted','error' | {'EXIT',_} | {'error',_}} | {'atomic',_}.
-spec do_transaction(atom() | pid() | port() | {atom(),atom()},fun()) -> {'aborted','error' | {'EXIT',_} | {'error',_}} | {'atomic',_}.
-spec do_begin(atom() | pid() | port() | {atom(),atom()},_) -> any().
-spec do_commit(atom() | pid() | port() | {atom(),atom()},_) -> any().
-spec do_rollback(atom() | pid() | port() | {atom(),atom()},_) -> any().
-spec get_migrations_table(atom() | pid() | port() | {atom(),atom()}) -> any().
-spec migration_done(atom() | pid() | port() | {atom(),atom()},atom(),'down' | 'up') -> any().
-spec activate_record(_,_,atom()) -> any().
-spec keyindex(_,_,maybe_improper_list()) -> any().
-spec keyindex(_,_,maybe_improper_list(),_) -> any().
-spec sort_order_sql('ascending' | 'descending') -> [65 | 67 | 68 | 69 | 83,...].
-spec build_insert_query(tuple()) -> [any(),...].
-spec escape_attr([any()]) -> [[any(),...]].
-spec build_update_query(atom() | tuple()) -> [any(),...].
-spec build_select_query(atom() | tuple(),[any()],'all' | integer(),_,atom(),'ascending' | 'descending') -> [any(),...].
-spec build_conditions(atom() | tuple(),[any()]) -> any().
-spec build_conditions1([{atom() | [any()] | number(),atom(),_}],_) -> any().
-spec add_cond(_,atom() | string() | number(),atom() | string() | number(),atom() | string() | number()) -> nonempty_maybe_improper_list().
-spec pack_match(atom() | string() | number()) -> string().
-spec pack_match_not(atom() | string() | number()) -> string().
-spec pack_boolean_query([any()],_) -> nonempty_string().
-spec pack_set([any()]) -> nonempty_string().
-spec pack_range('false' | 'null' | 'true' | 'undefined' | binary() | maybe_improper_list() | number() | {{_,_,_},{_,_,_}} | {_,_,_},'false' | 'null' | 'true' | 'undefined' | binary() | maybe_improper_list() | number() | {{_,_,_},{_,_,_}} | {_,_,_}) -> nonempty_maybe_improper_list().
-spec escape_sql([any()]) -> [any()].
-spec escape_sql1([any()],[any()]) -> [any()].
-spec pack_datetime(datetime()) -> any().
-spec pack_date(date()) -> any().

-spec pack_value(boolean()| nothing() | binary() | string() | number() | datetime() | date()) -> string()|binary().

-spec fetch(atom() | pid() | port() | {atom(),atom()},binary() | maybe_improper_list(binary() | maybe_improper_list(any(),binary() | []) | byte(),binary() | [])) -> 
                   #ok_packet{}|#result_packet{}|#error_packet{}.
                  
                                                                                                                                                                          

start(_) ->
    ok.

stop() ->
    ok.

init(Options) ->
    PoolSize     = proplists:get_value(db_poolsize, Options, 5),
    DBHost       = proplists:get_value(db_host,     Options, "localhost"),
    DBPort       = proplists:get_value(db_port,     Options, 3306),
    DBUsername   = proplists:get_value(db_username, Options, "guest"),
    DBPassword   = proplists:get_value(db_password, Options, ""),
    DBDatabase   = proplists:get_value(db_database, Options, "test"),
    DBIdentifier = proplists:get_value(db_shard_id, Options, boss_pool),
    Encoding     = utf8,
    case emysql:add_pool(DBIdentifier,
                    PoolSize,
                    DBUsername,
                    DBPassword,
                    DBHost,
                    DBPort,
                    DBDatabase,
                    Encoding) of
        {reply, ok, State} ->
            {ok,State};
        {reply, {error, E}}->
            lager:error("Unable to create Mysql Connection Pool due to ~p", [E]),
            {error, E}
    end.

terminate(Pid) -> 
    exit(Pid, normal).

find(DBPool, Id) when is_list(Id) ->
    {Type, TableName, IdColumn, TableId} = boss_sql_lib:infer_type_from_id(Id),
    Res = emysql:execute(DBPool, ["SELECT * FROM `", TableName, "` WHERE `", IdColumn, "` = ?"], [TableId]),
    case Res of
        #result_packet{ rows = []} ->
            {error, not_found};
        #result_packet{
           field_list = Columns,
           rows       = [Row]
          } ->
            IsLoaded = boss_record_lib:ensure_loaded(Type),
            activate_if_loaded(Type, Columns, Row, IsLoaded);
        #error_packet{}  ->
            {error, Res#error_packet.msg}
    end.

activate_if_loaded(Type, Columns, Row, true) ->
    activate_record(Row, Columns, Type);
activate_if_loaded(Type, _, _, false) ->
    {error, {module_not_loaded, Type}}.
    

%% REFAC
find(Pid, Type, Conditions, Max, Skip, Sort, SortOrder) when is_atom(Type), 
                                                             is_list(Conditions), 
                                                             is_integer(Max) orelse Max =:= all, 
                                                             is_integer(Skip), 
                                                             is_atom(Sort),
                                                             is_atom(SortOrder) ->
    io:format("Pid ~p", [Pid]),
    case boss_record_lib:ensure_loaded(Type) of
        true ->
            Query = build_select_query(Type, Conditions, Max, Skip, Sort, SortOrder),
            io:format("Query ~p~n", [Query]),
            Res = emysql:execute(Pid, Query),
                
            case Res of
                #error_packet{}  ->
                    {error, Res#error_packet.msg};
                #result_packet{
                   field_list = Columns,
                   rows       = ResultRows 
                   } ->
                    FilteredRows = case {Max, Skip} of
                                       {all, Skip} when Skip > 0 ->
                                           lists:nthtail(Skip, ResultRows);
                                       _ ->
                                           ResultRows
                                   end,
                    lists:map(fun(Row) ->
                                      activate_record(Row, Columns, Type)
                              end, FilteredRows)
            end;
        false -> {error, {module_not_loaded, Type}}
    end.


count(Pid, Type, Conditions) ->
    ConditionClause = build_conditions(Type, Conditions),
    TableName = boss_record_lib:database_table(Type),
    Res = emysql:execute(Pid, ["SELECT COUNT(*) AS count FROM ", TableName, " WHERE ", ConditionClause]),
    case Res of
        #error_packet{}  ->
            {error, Res#error_packet.msg};
        #result_packet{
           rows       = [[Count]]
          } ->
            Count
    end.
%%REFAC
counter(Pid, Id) when is_list(Id) ->
    Res = emysql:execute(Pid, ["SELECT value FROM counters WHERE name = ", pack_value(Id)]),
    case Res of
        #error_packet{}  ->
            {error, Res#error_packet.msg};
        #result_packet{
           rows       = [[Count]]
          } ->
            Count
    end. 

%%REFAC This one needs work
incr(Pid, Id, Count) ->
    Res = emysql:execute(Pid, ["UPDATE counters SET value = value + ", pack_value(Count), 
            " WHERE name = ", pack_value(Id)]),
      case Res of
        #error_packet{}  ->
            {error, Res#error_packet.msg};
        #result_packet{
           rows       = [[Count]]
          } ->
            Count
    end. 
%% case Res of
%%         {updated, _} ->
%%             counter(Pid, Id); % race condition
%%         {error, _Reason} -> 
%%             Res1 = fetch(Pid, ["INSERT INTO counters (name, value) VALUES (",
%%                     pack_value(Id), ", ", pack_value(Count), ")"]),
%%             case Res1 of
%%                 {updated, _} -> counter(Pid, Id); % race condition
%%                 {error, MysqlRes} -> {error, mysql:get_result_reason(MysqlRes)}
%%             end
%%     end.

%%REFAC
delete(Pid, Id) when is_list(Id) ->
    {_, TableName, IdColumn, TableId} = boss_sql_lib:infer_type_from_id(Id),
    Res = emysql:execute(Pid, ["DELETE FROM `", TableName, "` WHERE `", IdColumn, "` = ", 
                               pack_value(TableId)]),
    case Res of
        #result_packet{} ->
            emysql:execute(Pid, ["DELETE FROM counters WHERE name = ", 
                    pack_value(Id)]),
            ok;
        #error_packet{}  ->
            {error, Res#error_packet.msg}
    end.


%%REFAC
save_record(Pid, Record) when is_tuple(Record) ->
    case Record:id() of
        id ->
            Type  = element(1, Record),
            Query = build_insert_query(Record),
            Res   = fetch(Pid, Query),
            case Res of
                #ok_packet{} ->
                    Res1 = fetch(Pid, "SELECT last_insert_id()"),
                    case Res1 of
                        #result_packet{rows =[[Id]]} ->
                            {ok, Record:set(id, lists:concat([Type, "-", integer_to_list(Id)]))};

                        #error_packet{}  ->
                            {error, Res#error_packet.msg}

                    end;
                #error_packet{}  ->
                    {error, Res#error_packet.msg}
 
            end;
        Identifier when is_integer(Identifier) ->
            Type  = element(1, Record),
            Query = build_insert_query(Record),
            Res   = fetch(Pid, Query),
            case Res of
                #ok_packet{} ->
                    {ok, Record:set(id, lists:concat([Type, "-", integer_to_list(Identifier)]))};
                #error_packet{}  ->
                    {error, Res#error_packet.msg}
                
            end;			
        Defined when is_list(Defined) ->
            Query = build_update_query(Record),
            Res = fetch(Pid, Query),
            case Res of
                #ok_packet{} -> {ok, Record};
                #error_packet{}  ->
                            {error, Res#error_packet.msg}
            end
    end.

push(Pid, Depth) ->
    case Depth of 0 -> fetch(Pid, "BEGIN"); _ -> ok end,
    fetch(Pid, ["SAVEPOINT savepoint", integer_to_list(Depth)]).

pop(Pid, Depth) ->
    fetch(Pid, ["ROLLBACK TO SAVEPOINT savepoint", integer_to_list(Depth - 1)]),
    fetch(Pid, ["RELEASE SAVEPOINT savepoint", integer_to_list(Depth - 1)]).

dump(_Conn) -> "".

execute(Pid, Commands) ->
    fetch(Pid, Commands).

transaction(Pid, TransactionFun) when is_function(TransactionFun) ->
    do_transaction(Pid, TransactionFun).
    
%REFAC
do_transaction(Pid, TransactionFun) when is_function(TransactionFun) ->
    case do_begin(Pid, self()) of
        {error, _} = Err ->	
            {aborted, Err};
        {updated,{mysql_result,[],[],0,0,[]}} ->
            case catch TransactionFun() of
                error = Err ->  
                    do_rollback(Pid, self()),
                    {aborted, Err};
                {error, _} = Err -> 
                    do_rollback(Pid, self()),
                    {aborted, Err};
                {'EXIT', _} = Err -> 
                    do_rollback(Pid, self()),
                    {aborted, Err};
                Res ->
                    case do_commit(Pid, self()) of
                        #error_packet{} = Err ->
                            do_rollback(Pid, self()),
                            {aborted, Err};
                        _ ->
                            {atomic, Res}
                    end
            end
    end.

do_begin(Pid,_)->
    fetch(Pid, ["BEGIN"]).	

do_commit(Pid,_)->
    fetch(Pid, ["COMMIT"]).

do_rollback(Pid,_)->
    fetch(Pid, ["ROLLBACK"]).

get_migrations_table(Pid) ->
    fetch(Pid, "SELECT * FROM schema_migrations").

migration_done(Pid, Tag, up) ->
    fetch(Pid, ["INSERT INTO schema_migrations (version, migrated_at) values (",
                atom_to_list(Tag), ", NOW())"]);
migration_done(Pid, Tag, down) ->
    fetch(Pid, ["DELETE FROM schema_migrations WHERE version = ", atom_to_list(Tag)]).

% internal

%% integer_to_id(Val, KeyString) ->
%%     ModelName = string:substr(KeyString, 1, string:len(KeyString) - string:len("_id")),
%%     ModelName ++ "-" ++ integer_to_list(Val).
%REFAC
convert_cols(Metadata) ->
    [{Field,Index}|| #field{name=Field, 
                seq_num = Index}<- Metadata].

activate_record(Record, Columns, Type) ->
    Metadata           = convert_cols(Columns),
    AttributeTypes     = boss_record_lib:attribute_types(Type),
    AttributeColumns   = boss_record_lib:database_columns(Type),
    RetypedForeignKeys = boss_sql_lib:get_retyped_foreign_keys(Type),

    apply(Type, new, lists:map(fun
                (id) ->
                    DBColumn = proplists:get_value('id', AttributeColumns),
                    Index    = keyindex(list_to_binary(DBColumn), 1, Metadata),
                    atom_to_list(Type) ++ "-" ++ integer_to_list(lists:nth(Index , Record));
                (Key) ->
                    DBColumn = proplists:get_value(Key, AttributeColumns),
                    Index    = keyindex(list_to_binary(DBColumn), 1, Metadata),
                    AttrType = proplists:get_value(Key, AttributeTypes),
                    case lists:nth(Index, Record) of
                        undefined            -> 
                            undefined;
                        {datetime, DateTime} -> 
                            boss_record_lib:convert_value_to_type(DateTime, AttrType);
                        Val                  -> 
                            boss_sql_lib:convert_possible_foreign_key(RetypedForeignKeys, Type, Key, Val, AttrType)
                    end
            end, boss_record_lib:attribute_names(Type))).

keyindex(Key, N, TupleList) ->
    keyindex(Key, N, TupleList, 1).

keyindex(_Key, _N, [], _Index) ->
    undefined;
keyindex(Key, N, [Tuple|Rest], Index) ->
    case element(N, Tuple) of
        Key -> Index;
        _ -> keyindex(Key, N, Rest, Index + 1)
    end.

sort_order_sql(descending) ->
    "DESC";
sort_order_sql(ascending) ->
    "ASC".
%REFAC
build_insert_query(Record) ->
    Type = element(1, Record),
    TableName = boss_record_lib:database_table(Type),
    AttributeColumns = Record:database_columns(),
    {Attributes, Values} = lists:foldl(fun
            ({_, undefined}, Acc) -> Acc;
            ({'id', 'id'}, Acc) -> Acc;
            ({'id', V}, {Attrs, Vals}) when is_integer(V) -> 
                 {[atom_to_list(id)|Attrs], [pack_value(V)|Vals]};
            ({'id', V}, {Attrs, Vals}) -> 
                DBColumn = proplists:get_value('id', AttributeColumns),
                {_, _, _, TableId} = boss_sql_lib:infer_type_from_id(V),
                {[DBColumn|Attrs], [pack_value(TableId)|Vals]};
            ({A, V}, {Attrs, Vals}) ->
                DBColumn = proplists:get_value(A, AttributeColumns),
                Value    = case boss_sql_lib:is_foreign_key(Type, A) of
                    true ->
                        {_, _, _, ForeignId} = boss_sql_lib:infer_type_from_id(V),
                        ForeignId;
                    false ->
                        V
                end,
                {[DBColumn|Attrs], [pack_value(Value)|Vals]}
        end, {[], []}, Record:attributes()),
    ["INSERT INTO ", TableName, " (", 
        string:join(escape_attr(Attributes), ", "),
        ") values (",
        string:join(Values, ", "),
        ")"
    ].
escape_attr(Attrs) ->
    [["`", Attr, "`"] || Attr <- Attrs].

build_update_query(Record) ->
    {Type, TableName, IdColumn, TableId} = boss_sql_lib:infer_type_from_id(Record:id()),
    AttributeColumns = Record:database_columns(),
    Updates = lists:foldl(fun
            ({id, _}, Acc) -> Acc;
            ({A, V}, Acc) -> 
                DBColumn = proplists:get_value(A, AttributeColumns),
                Value = case {boss_sql_lib:is_foreign_key(Type, A), V =/= undefined} of
                    {true, true} ->
                        {_, _, _, ForeignId} = boss_sql_lib:infer_type_from_id(V),
                        ForeignId;
                    {_, false} ->
                        null;
                    _ ->
                        V
                end,
                ["`"++DBColumn ++ "` = " ++ pack_value(Value)|Acc]
        end, [], Record:attributes()),
    ["UPDATE ", TableName, " SET ", string:join(Updates, ", "),
        " WHERE ", IdColumn, " = ", pack_value(TableId)].

build_select_query(Type, Conditions, Max, Skip, Sort, SortOrder) ->	
    TableName = boss_record_lib:database_table(Type),
    ["SELECT * FROM ", TableName, 
        " WHERE ", build_conditions(Type, Conditions),
        " ORDER BY ", atom_to_list(Sort), " ", sort_order_sql(SortOrder),
        case Max of all -> ""; _ -> [" LIMIT ", integer_to_list(Max),
                    " OFFSET ", integer_to_list(Skip)] end
    ].

build_conditions(Type, Conditions) ->
    AttributeColumns = boss_record_lib:database_columns(Type),
    Conditions2 = lists:map(fun
            ({'id' = Key, Op, Value}) ->
                Key2 = proplists:get_value(Key, AttributeColumns, Key),
                boss_sql_lib:convert_id_condition_to_use_table_ids({Key2, Op, Value});
            ({Key, Op, Value}) ->
                Key2 = proplists:get_value(Key, AttributeColumns, Key),
                case boss_sql_lib:is_foreign_key(Type, Key) of
                    true -> boss_sql_lib:convert_id_condition_to_use_table_ids({Key2, Op, Value});
                    false -> {Key2, Op, Value}
                end
        end, Conditions),
    build_conditions1(Conditions2, [" TRUE"]).

build_conditions1([], Acc) ->
    Acc;
build_conditions1([{Key, 'equals', Value}|Rest], Acc) when Value == undefined ; Value == null->
    build_conditions1(Rest, add_cond(Acc, Key, "is", pack_value(Value)));
build_conditions1([{Key, 'equals', Value}|Rest], Acc) ->
    build_conditions1(Rest, add_cond(Acc, Key, "=", pack_value(Value)));
build_conditions1([{Key, 'not_equals', Value}|Rest], Acc) when Value == undefined ; Value == null ->
    build_conditions1(Rest, add_cond(Acc, Key, "is not", pack_value(Value)));
build_conditions1([{Key, 'not_equals', Value}|Rest], Acc) ->
    build_conditions1(Rest, add_cond(Acc, Key, "!=", pack_value(Value)));
build_conditions1([{Key, 'in', Value}|Rest], Acc) when is_list(Value) ->
    PackedValues = pack_set(Value),
    build_conditions1(Rest, add_cond(Acc, Key, "IN", PackedValues));
build_conditions1([{Key, 'not_in', Value}|Rest], Acc) when is_list(Value) ->
    PackedValues = pack_set(Value),
    build_conditions1(Rest, add_cond(Acc, Key, "NOT IN", PackedValues));
build_conditions1([{Key, 'in', {Min, Max}}|Rest], Acc) when Max >= Min ->
    PackedValues = pack_range(Min, Max),
    build_conditions1(Rest, add_cond(Acc, Key, "BETWEEN", PackedValues));
build_conditions1([{Key, 'not_in', {Min, Max}}|Rest], Acc) when Max >= Min ->
    PackedValues = pack_range(Min, Max),
    build_conditions1(Rest, add_cond(Acc, Key, "NOT BETWEEN", PackedValues));
build_conditions1([{Key, 'gt', Value}|Rest], Acc) ->
    build_conditions1(Rest, add_cond(Acc, Key, ">", pack_value(Value)));
build_conditions1([{Key, 'lt', Value}|Rest], Acc) ->
    build_conditions1(Rest, add_cond(Acc, Key, "<", pack_value(Value)));
build_conditions1([{Key, 'ge', Value}|Rest], Acc) ->
    build_conditions1(Rest, add_cond(Acc, Key, ">=", pack_value(Value)));
build_conditions1([{Key, 'le', Value}|Rest], Acc) ->
    build_conditions1(Rest, add_cond(Acc, Key, "<=", pack_value(Value)));
build_conditions1([{Key, 'matches', "*"++Value}|Rest], Acc) ->
    build_conditions1(Rest, add_cond(Acc, Key, "REGEXP", pack_value(Value)));
build_conditions1([{Key, 'not_matches', "*"++Value}|Rest], Acc) ->
    build_conditions1(Rest, add_cond(Acc, Key, "NOT REGEXP", pack_value(Value)));
build_conditions1([{Key, 'matches', Value}|Rest], Acc) ->
    build_conditions1(Rest, add_cond(Acc, Key, "REGEXP BINARY", pack_value(Value)));
build_conditions1([{Key, 'not_matches', Value}|Rest], Acc) ->
    build_conditions1(Rest, add_cond(Acc, Key, "NOT REGEXP BINARY", pack_value(Value)));
build_conditions1([{Key, 'contains', Value}|Rest], Acc) ->
    build_conditions1(Rest, add_cond(Acc, pack_match(Key), "AGAINST", pack_boolean_query([Value], "")));
build_conditions1([{Key, 'not_contains', Value}|Rest], Acc) ->
    build_conditions1(Rest, add_cond(Acc, pack_match_not(Key), "AGAINST", pack_boolean_query([Value], "")));
build_conditions1([{Key, 'contains_all', Values}|Rest], Acc) when is_list(Values) ->
    build_conditions1(Rest, add_cond(Acc, pack_match(Key), "AGAINST", pack_boolean_query(Values, "+")));
build_conditions1([{Key, 'not_contains_all', Values}|Rest], Acc) when is_list(Values) ->
    build_conditions1(Rest, add_cond(Acc, pack_match_not(Key), "AGAINST", pack_boolean_query(Values, "+")));
build_conditions1([{Key, 'contains_any', Values}|Rest], Acc) when is_list(Values) ->
    build_conditions1(Rest, add_cond(Acc, pack_match(Key), "AGAINST", pack_boolean_query(Values, "")));
build_conditions1([{Key, 'contains_none', Values}|Rest], Acc) when is_list(Values) ->
    build_conditions1(Rest, add_cond(Acc, pack_match_not(Key), "AGAINST", pack_boolean_query(Values, ""))).

add_cond(Acc, Key, Op, PackedVal) ->
    [[Key, " ", Op, " ", PackedVal, " AND "]|Acc].

pack_match(Key) ->
    ["MATCH(", Key, ")"].

pack_match_not(Key) ->
    ["NOT MATCH(", Key, ")"].

pack_boolean_query(Values, Op) ->
    ["('" ,lists:map(fun(Val) -> [Op, escape_sql(Val)] end, Values), " ", "' IN BOOLEAN MODE)"].
pack_set(Values) ->
    ["(" , lists:map(fun pack_value/1, Values), ")"].

pack_range(Min, Max) ->
    pack_value(Min) ++ " AND " ++ pack_value(Max).

escape_sql(Value) ->
    escape_sql1(Value, []).

escape_sql1([], Acc) ->
    lists:reverse(Acc);
escape_sql1([$'|Rest], Acc) ->
    escape_sql1(Rest, [$', $'|Acc]);
escape_sql1([C|Rest], Acc) ->
    escape_sql1(Rest, [C|Acc]).

pack_datetime(DateTime) ->
    dh_date:format("'Y-m-d H:i:s'",DateTime ).

pack_date(Date) ->
    dh_date:format("'Y-m-d\TH:i:s'",{Date, {0,0,0}}).


%pack_now(Now) -> pack_datetime(calendar:now_to_datetime(Now)).

pack_value(null) ->
    "null";
pack_value(undefined) ->
    "null";
pack_value(V) when is_binary(V) ->
    pack_value(binary_to_list(V));
pack_value(V) when is_list(V) ->
    emysql_util:encode(V);
pack_value({_, _, _} = Val) ->
    pack_date(Val);    
pack_value({{_, _, _}, {_, _, _}} = Val) ->
    pack_datetime(Val);
pack_value(Val) when is_integer(Val) ->
    integer_to_list(Val);
pack_value(Val) when is_float(Val) ->
    float_to_list(Val);
pack_value(true) ->
    "TRUE";
pack_value(false) ->
    "FALSE".

fetch(Pool, Query) ->
    lager:notice("Query ~s", [iolist_to_binary(Query)]),
    emysql:execute(Pool, iolist_to_binary(Query)).


