%% -------------------------------------------------------------------
%%
%% Copyright (c) 2015 Carlos Gonzalez Florido.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

%% @private Backend for ets and leveldb
-module(nkbase_backend).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').

-export([get/2, put/4, del/2, reindex/4]).
-export([fold_domains/1, fold_classes/1, fold_objs/1, search/1]).
-export([leveldb_open/1, leveldb_destroy/2, leveldb_status/1]).
-export([extract_indices/1, do_fold/7, has_idx/4]).
-export([get_meta/3, put_meta/3, del_meta/2]).
-export_type([db_ref/0]).

-type db_ref() :: term().
-type fold_acc() :: term().
-type fold_fun() ::  
	fun((ExtKey::nkbase:ext_key(), [Data::nkbase:ext_obj()], Acc::fold_acc()) ->
		{iter, fold_acc()} | {done, fold_acc()} | {jump, binary(), fold_acc()}).


-define(DVSN, d0).
-define(IVSN, i0).
-define(MVSN, m0).

-include("nkbase.hrl").


%% Erlang Sort Order:
%% number < atom < reference < fun < port < pid < tuple < list < bit string


%% @doc Gets an object from the backend
-spec get({nkbase:backend(), db_ref()}, nkbase:ext_key()) ->
	{ok, dvvset:clock()} | {error, term()}.

get({ets, Ref}, ExtKey) ->
	case ets:lookup(Ref, encode_key(ets, ExtKey)) of
		[] -> {error, not_found};
		[{_, Data}] -> decode_data(ets, Data)
	end;

get({leveldb, Ref}, ExtKey) ->
	case eleveldb:get(Ref, encode_key(leveldb, ExtKey), []) of
		{ok, Data} -> decode_data(leveldb, Data);
		not_found -> {error, not_found};
		{error, Error} -> {error, {leveldb_error, Error}}
	end.


%% @doc Gets the metadata of an object
-spec has_idx({nkbase:backend(), db_ref()}, nkbase:ext_key(), 
		      nkbase:index_name(), term()) ->
	boolean().

has_idx({ets, Ref}, ExtKey, Index, Value) ->
	case ets:lookup(Ref, encode_index(ets, ExtKey, Index, Value)) of
		[] -> false;
		[{_, <<>>}] -> true
	end;

has_idx({leveldb, Ref}, ExtKey, Index, Value) ->
	case eleveldb:get(Ref, encode_index(leveldb, ExtKey, Index, Value), []) of
		{ok, <<>>} -> true;
		_ -> false
	end.


%% @doc Saves an object to the backend
-spec put({nkbase:backend(), db_ref()}, nkbase:ext_key(), 
		  dvvset:clock(), nkbase:put_meta()) ->
	{ok, list(), dvvset:clock()} | {error, term()}.

put({Backend, Ref}, ExtKey, DVV, Meta) ->
	try
		case Meta of
			#{old_dvv:=none} -> 
				OldDVV = {},
				OldIndices = [];
			#{old_dvv:=OldDVV} ->
				OldIndices = extract_indices(OldDVV);
			_ ->
				case get({Backend, Ref}, ExtKey) of
					{ok, OldDVV} -> 
						OldIndices = extract_indices(OldDVV);
					{error, not_found} ->
						OldDVV = {},
						OldIndices = [];
					{error, ReadError} ->
						OldDVV = OldIndices = throw(ReadError)
				end
		end,
		Actor = maps:get(actor, Meta, false),
		DVV1 = case Actor of
			false ->
				dvvset:sync([DVV, OldDVV]);
			ignore ->
				DVV;
			Actor ->
				dvvset:update(DVV, OldDVV, Actor)
		end,
		NewDVV = case dvvset:size(DVV1) of
			1 -> 
				DVV1;
			_ when Actor==ignore ->
				DVV1;
			_ -> 
				Reconcile = maps:get(reconcile, Meta, undefined),
				case nkbase_util:reconcile(Reconcile, ExtKey, DVV1) of
					{ok, DVV2} -> DVV2;
					{error, ReconcileError} -> throw(ReconcileError)
				end
		end,
		NewIndices = extract_indices(NewDVV),
		% io:format("INDICES: ~p, ~p\n", [ExtKey, NewIndices]),
		EncKey = encode_key(Backend, ExtKey),
		EncData = encode_data(Backend, NewDVV),
		UpdatedIndices = indices(Backend, ExtKey, OldIndices, NewIndices), 
		Updates = [{put, EncKey, EncData}|UpdatedIndices],
		% io:format("Updates: ~p\n", [Updates]),
		case do_update(Backend, Ref, Updates) of
			ok -> {ok, NewIndices, NewDVV};
			{error, UpdateError} -> {error, UpdateError}
		end
	catch
		throw:Throw -> {error, Throw}
	end.


%% @private
%% If any object's exp is undefned, no expiration
%% If all object has numeric expirations, pick last
-spec extract_indices(dvvset:clock()) ->
	[{nkbase:index_name(), term()}].

extract_indices(DVV) ->
	Metas = [Meta || {Meta, _Obj} <- dvvset:values(DVV)],
	Indices = extract_indices(Metas, []),
	case lists:max([maps:get(exp, Meta, undefined) || Meta <- Metas]) of
		undefined ->
			Indices;
		Exp ->
			[{?EXP_IDX, Exp}|Indices]
	end.


%% @private
%% If any object's ttl is undefned, no ttl
%% If all object has numeric ttls, pick last
-spec extract_indices([map()], [{nkbase:index_name(), term()}]) ->
	[{nkbase:index_name(), term()}].

extract_indices([Meta|Rest], Acc) ->
	Acc1 = case maps:get(idx, Meta, []) of
		[] -> Acc;
		Indices when Acc==[] -> Indices;
		Indices -> do_extract_indices(Indices, Acc)
	end,
	extract_indices(Rest, Acc1);

extract_indices([], Acc) ->
	Acc.


%% @private
-spec do_extract_indices([{nkbase:index_name(), term()}], 
						 [{nkbase:index_name(), term()}]) ->
	[{nkbase:index_name(), term()}].

do_extract_indices([Idx|Rest], Acc) ->
	case lists:member(Idx, Acc) of
		false -> do_extract_indices(Rest, [Idx|Acc]);
		true -> do_extract_indices(Rest, Acc)
	end;

do_extract_indices([], Acc) ->
	Acc.


%% @private Atomically updates elements on backend
-spec do_update(nkbase:backend(), db_ref(), [Op]) ->
	ok | {error, term()}
	when Op :: {put, binary(), binary()} | {delete, binary()}.

do_update(ets, Ref, Updates) ->
	case [{Key, Val} || {put, Key, Val} <- Updates] of
		[] -> ok;
		Put -> true = ets:insert(Ref, Put)
	end,
	[true=ets:delete(Ref, Key) || {delete, Key} <- Updates],
	ok;
do_update(leveldb, Ref, Updates) ->
	case eleveldb:write(Ref, Updates, []) of
		ok -> ok;
		{error, Error} -> {error, {leveldb_error, Error}}
	end.


%% @private Generates a list of updates related to indices
-spec indices(nkbase:backend(), nkbase:ext_key(), 
			  [{nkbase:index_name(), term()}], [{nkbase:index_name(), term()}]) ->
	[Op]
	when Op :: {put, binary(), binary()} | {delete, binary()} .

indices(Backend, ExtKey, OldIndices, NewIndices) ->
	[
		{delete, encode_index(Backend, ExtKey, Index, Value)}
		|| {Index, Value} <- OldIndices -- NewIndices
	]
	++
	[
		{put, encode_index(Backend, ExtKey, Index, Value), <<>>}
		|| {Index, Value} <- NewIndices -- OldIndices
	].


%% @doc Removes and object from the backend
-spec del({nkbase:backend(), db_ref()}, nkbase:ext_key()) ->
	ok | {error, term()}.

del({Backend, Ref}, ExtKey) ->
	case get({Backend, Ref}, ExtKey) of
		{ok, DVV} -> 
			Indices = extract_indices(DVV),
			Updates = [
				{delete, encode_key(Backend, ExtKey)} |
				indices(Backend, ExtKey, Indices, [])
			],
			do_update(Backend, Ref, Updates);
	 	{error, Error} ->
	 		{error, Error}
	end.
	

%% @doc Reindexes a DVV
-spec reindex({nkbase:backend(), db_ref()}, nkbase:ext_key(), 
			  dvvset:clock(), [nkbase:index_spec()]) ->
	ok | {error, term()}.

reindex(BackSpec, ExtKey, DVV, IndexSpec) ->
	DVV1 = dvvset:map(
		fun({Meta, Obj}) ->
			Indices = nkbase_util:expand_indices(IndexSpec, ExtKey, Obj),
			{Meta#{idx=>Indices}, Obj}
		end,
		DVV),
	case put(BackSpec, ExtKey, DVV1, #{old_dvv=>DVV, actor=>ignore}) of
		{ok, _, _} -> ok;
		{error, Error} -> {error, Error}
	end.


%% @doc Scans all domains in database
-spec fold_domains(#fold_spec{}) ->
	{done, term()} | {error, term()}.

fold_domains(Spec) ->
	#fold_spec{
		backend = Backend, 
		db_ref = Ref, 
		fold_fun = FoldFun, 
		fold_acc = FoldAcc
	} = Spec,
	IterFun = fun(BinKey, _, Acc) ->
		case decode_key(Backend, BinKey) of
			{data, {Dom, _, _}} ->
				Acc1 = FoldFun({Dom, none, none}, [], Acc),
				{jump, encode_key(Backend, {Dom, ?ERL_HIGH, ?ERL_HIGH}), Acc1};
			{index, _} ->
				{done, Acc};
			{meta, _} ->
				{done, Acc};
			{error, Error} ->
				{error, Error}
		end
	end,
	First = encode_key(Backend, {?ERL_LOW, ?ERL_LOW, ?ERL_LOW}),
	do_fold(Backend, asc, Ref, First, keys, IterFun, FoldAcc).


%% @doc Scans all classes in database for a domain
-spec fold_classes(#fold_spec{}) ->
	{done, term()} | {error, term()}.

fold_classes(Spec) ->
	#fold_spec{
		backend = Backend, 
		db_ref = Ref, 
		domain = Dom,
		fold_fun = FoldFun, 
		fold_acc = FoldAcc
	} = Spec,
	IterFun = fun(BinKey, _, Acc) ->
		case decode_key(Backend, BinKey) of
			{data, {FDom, _, _}} when FDom/=Dom ->
				{done, Acc};
			{data, {Dom, Class, _}} ->
				Acc1 = FoldFun({Dom, Class, none}, [], Acc),
				{jump, encode_key(Backend, {Dom, Class, ?ERL_HIGH}), Acc1};
			{index, _} ->
				{done, Acc};
			{meta, _} ->
				{done, Acc};
			{error, Error} ->
				{error, Error}
		end
	end,
	First = encode_key(Backend, {Dom, ?ERL_LOW, ?ERL_LOW}),
	do_fold(Backend, asc, Ref, First, keys, IterFun, FoldAcc).



%% @doc Scans over all keys or values in the backend belonging to a Domain and Class
-spec fold_objs(#fold_spec{}) ->
	{done, term()} | {error, term()}.

fold_objs(Spec) ->
	#fold_spec{
		backend = Backend,
		db_ref = Ref,
		domain = Dom,
		class = Class,
		start = Start,
		stop = Stop,
		fold_type = FoldType,
		fold_fun = FoldFun,
		fold_acc = FoldAcc,
		keyfilter = KeyFilter,
		max_items = MaxItems
	} = Spec,
	IterFun = fun(BinKey, BinVal, {Iters, Items, Acc}=FAcc) ->
		% lager:warning("FOLD: ~p", [BinKey]),
		fold_check_abort(Iters, Items, MaxItems, FAcc),
		case decode_key(Backend, BinKey) of
			{data, {FDom, _, _}} when Dom/='$nk_all', FDom/=Dom -> 
				{done, FAcc};
			{data, {_, FClass, _}} when Class/='$nk_all', FClass/=Class -> 
				{done, FAcc};
			{data, {_, _, FKey}} when Start/='$nk_first', FKey<Start ->
				{done, FAcc};
			{data, {_, _, FKey}} when Stop/='$nk_last', FKey>Stop ->
				{done, FAcc};
			{data, ExtKey} ->
				case KeyFilter==undefined orelse KeyFilter(ExtKey) of
					true when FoldType==keys ->
						Acc1 = FoldFun(ExtKey, [], Acc),
						{iter, {Iters+1, Items+1, Acc1}};
					true when FoldType==values ->
						case decode_data(Backend, BinVal) of
							{ok, DVV} ->
								Values = dvvset:values(DVV),
								Acc1 = FoldFun(ExtKey, Values, Acc),
								{iter, {Iters+1, Items+1, Acc1}};
							{error, Error} ->
								throw({error, Error})
						end;
					false ->
						{iter, {Iters+1, Items, Acc}}
				end;
			{index, _} ->
				{done, FAcc};
			{meta, _} ->
				{done, FAcc};
			{error, Error} ->
				throw({error, Error})
		end
	end,
	StartDom = case Dom of '$nk_all' -> ?ERL_LOW; _ -> Dom end,
	StartClass = case Class of '$nk_all' -> ?ERL_LOW; _ -> Class end,
	StartKey = case Start of '$nk_first' -> ?ERL_LOW; _ -> Start end,
	First = encode_key(Backend, {StartDom, StartClass, StartKey}),
	try
		do_fold(Backend, asc, Ref, First, FoldType, IterFun, {0, 0, FoldAcc})
	of
		{done, {_Iters, _Total, Value}} -> {done, Value};
		{error, Error} -> {error, Error}
	catch
		throw:{done, {_Iters, _Total, Value}} -> {done, Value};
		throw:{error, Error} -> {error, Error}
	end.


%% @private Folds over all data in the backend
-spec search(#search_spec{}) ->
	{done, term()} | {error, term()}.

search(Spec) ->
	#search_spec{
		backend = Bk,
		db_ref = Ref,
		domain = Dom,
		class = Class,
		order = Order,
		indices = Indices,
		next = Next,
		fold_fun = FoldFun,
		fold_acc = FoldAcc,
		keyfilter = KeyFilter,
		max_items = MaxItems
	} = Spec,
	[{Index, Filters}|_] = Indices,
	{Start, Stop, FoldHoles0} = nkbase_search:get_ranges(Filters),
	FoldHoles = case Order of
		asc -> FoldHoles0;
		desc -> lists:reverse(FoldHoles0)
	end,
	% lager:warning("Start: ~p, Stop: ~p, Holes: ~p", [Start, Stop, FoldHoles]),
	{StartValue, StartKey} = case Next of
		undefined when Order==asc -> {Start, ?ERL_LOW};
		undefined when Order==desc -> {Stop, ?ERL_HIGH};
		{StartValue0, StartKey0} -> {StartValue0, StartKey0}
	end,
	% lager:warning("R: ~p-~p, N: ~p,~p", [Start, Stop, StartValue, StartKey]),
	IterFun = fun(BinKey, none, {Iters, Items, Acc, Holes}=FAcc) ->
		fold_check_abort(Iters, Items, MaxItems, FAcc),
		case Holes of
			[{HStart, HStop}|HRest] -> ok;
			[] -> HStart = HStop = HRest = []
		end,
		case decode_key(Bk, BinKey) of
			{index, {{FD, FC, _FK}, FI, FV}} when
					FD/=Dom; FC/=Class; FI/=Index; FV<Start; FV>Stop ->
				{done, FAcc};
			{index, {_ExtKey, _, FV}} when
					Holes/=[], FV>HStart, FV<HStop ->
					HNext = case Order of
						asc -> 
							% lager:warning("JUMP ~p FROM ~p to ~p", [Bk, FV, HStop]),
							encode_index(Bk, {Dom, Class, ?ERL_LOW}, Index, HStop);
						desc -> 
							% lager:warning("JUMP ~p FROM ~p to ~p", [Bk, FV, HStart]),
							encode_index(Bk, {Dom, Class, ?ERL_HIGH}, Index, HStart)
					end,
				{jump, HNext, {Iters, Items, Acc, HRest}};
			{index, {ExtKey, _, FV}} ->
				% lager:warning("HS: ~p, HT: ~p, FV: ~p", [HStart, HStop, FV]),
				Holes1 = find_next_hole(Order, FV, Holes),
				ExtKey1 = case {Dom, Class} of
					{'$nkbase', '$g'} -> element(3, ExtKey);
					_ -> ExtKey
				end,
				case KeyFilter==undefined orelse KeyFilter(ExtKey1) of
					true ->
						case do_search_find(Spec, ExtKey, FV, Indices) of
							{true, Values} ->
								case FoldFun(FV, ExtKey, Values, Acc) of
									{done, Acc1} -> 
										{done, {Iters, Items, Acc1, []}};
									{iter, Acc1} -> 
										{iter, {Iters+1, Items+1, Acc1, Holes1}}
								end;
							false ->
								{iter, {Iters+1, Items, Acc, Holes1}}
						end;
					false ->
						{iter, {Iters+1, Items, Acc, Holes1}}
				end;
			{data, _} ->
				{done, FAcc};
			{meta, _} ->
				{done, FAcc};
			{error, Error} ->
				throw({error, Error})
		end
	end,
	First = encode_index(Bk, {Dom, Class, StartKey}, Index, StartValue),
	try
		do_fold(Bk, Order, Ref, First, keys, IterFun, {0, 0, FoldAcc, FoldHoles})
	of
		{done, {_Iters, _Total, Value, _}} -> {done, Value};
		{error, Error} -> {error, Error}
	catch
		throw:{done, {_Iters, _Total, Value, _}} -> {done, Value};
		throw:{error, Error} -> {error, Error}
	end.


%% @private
-spec do_search_find(#search_spec{}, nkbase:ext_key(), term(), 
					 nkbase_search:ext_search_spec()) ->
	{true, [nkbase:ext_obj()]} | false.

do_search_find(Spec, ExtKey, FV, [{_Index, Filters}|Rest]=Indices) ->
	#search_spec{backend=Bk, db_ref=Ref, fold_type=FoldType} = Spec,
	case nkbase_search:any_filter(FV, Filters) of
		false ->
			false;
		true when FoldType==keys, Rest==[] ->
			{true, []};
		true ->
			case get({Bk, Ref}, ExtKey) of
				{ok, DVV} ->
					Values = dvvset:values(DVV),
					case do_search_filter(Values, Indices, []) of
						[] -> 
							false;
						Res when FoldType==values ->
							{true, Res};
						_ when FoldType==keys -> 
							{true, []}
					end;
				{error, Error} ->
					lager:warning("NkBASE error getting ~p: ~p", [ExtKey, Error]),
					throw({error, Error})
			end
	end.


%% @private
do_search_filter([], _Indices, Acc) ->
	Acc;

do_search_filter([{Meta, Obj}|Rest], Indices, Acc) ->
	ObjIndices = maps:get(idx, Meta, []),
	Acc1 = case nkbase_search:test_indices(Indices, ObjIndices) of
		true -> [{Meta, Obj}|Acc];
		false -> Acc
	end,
	do_search_filter(Rest, Indices, Acc1).


%% @private
find_next_hole(_Order, _FV, []) -> 
	[];

find_next_hole(asc, FV, [{_, Stop}|Rest]) when FV >= Stop -> 
	find_next_hole(asc, FV, Rest);

find_next_hole(desc, FV, [{Start, _}|Rest]) when FV =< Start -> 
	find_next_hole(desc, FV, Rest);

find_next_hole(_Order, _FV, Holes) -> 
	Holes.


%% @doc Gets external metadata from database
-spec get_meta({ets, db_ref()}, term(), term()) ->
	term().

get_meta({ets, Ref}, Key, Default) ->
	case ets:lookup(Ref, encode_meta_key(ets, Key)) of
		[{_, Value}] -> Value;
		[] -> Default
	end.


%% @doc Puts external metadata in database
-spec put_meta({ets, db_ref()}, term(), term()) ->
	ok.

put_meta({ets, Ref}, Key, Value) ->
	true = ets:insert(Ref, {encode_meta_key(ets, Key), Value}),
	ok.


%% @doc Deletes a external metadata from database
-spec del_meta({ets, db_ref()}, term()) ->
	ok.

del_meta({ets, Ref}, Key) ->
	true = ets:delete(Ref, encode_meta_key(ets, Key)),
	ok.


%% @private Opens the LevelDB file
-spec leveldb_open(string()) ->
	{ok, db_ref()} | {error, term()}.

leveldb_open(Path) ->
	leveldb_open(Path, true).


%% @private
leveldb_open(Path, Retry) ->
	Opts = nkbase_app:get_env(leveldb_opts),
	case catch 
		eleveldb:open(Path, [{create_if_missing, true}|Opts])
	of
		{ok, Ref} -> 
			{ok, Ref};
		{error, Error} when Retry ->
			case catch eleveldb:repair(Path, []) of
				ok ->
					lager:warning("LevelDB ~s repair OK", [Path]),
					leveldb_open(Path, false);
				O ->
					lager:warning("LevelDB ~s repair ERROR: ~p", [Path, O]),
					{error, {leveldb_error, Error}}
			end;
		{error, Error} -> 
			{error, {leveldb_error, Error}};
		Other -> 
			{error, {leveldb_error, Other}}
	end.


%% @private Destroys the LevelDB file
-spec leveldb_destroy(db_ref(), string()) ->
	ok | {error, term()}.

leveldb_destroy(DbRef, Path) ->
	case eleveldb:close(DbRef) of
		ok ->
			case catch eleveldb:destroy(Path, []) of
				ok -> ok;
				{error, Error} -> {error, {leveldb_error, Error}};
				Other -> {error, {leveldb_error, Other}}
			end;
		Other ->
			{error, Other}
	end.


-spec leveldb_status(db_ref()) ->
	{ok, binary()} | {error, term()}.

leveldb_status(Ref) ->
	case catch eleveldb:status(Ref, <<"leveldb.stats">>) of
		{ok, Stats} -> {ok, Stats};
		{'EXIT', Error} -> {error, Error};
		{error, Error} -> {error, Error}
	end.
	


%%%%%%%%%%%%%%%%%%% Internal %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @private Serializes and generates a backend key
-spec encode_key(nkbase:backend(), nkbase:ext_key()) ->
	tuple() | binary().

encode_key(ets, {Dom, Class, Key}) -> 
	{?DVSN, Dom, Class, Key};
encode_key(leveldb, ExtKey) -> 
	sext:encode(encode_key(ets, ExtKey)).


%% @private Serializes and generates a backend index key
-spec encode_index(nkbase:backend(), nkbase:ext_key(), nkbase:index_name(), term()) ->
	tuple() | binary().

encode_index(ets, {Dom, Class, Key}, {'$g', Index}, Value) -> 
	{?IVSN, '$nkbase', '$g', Index, Value, {Dom, Class, Key}};
encode_index(ets, {Dom, Class, Key}, Index, Value) -> 
	{?IVSN, Dom, Class, Index, Value, Key};
encode_index(leveldb, ExtKey, Index, Value) ->
	sext:encode(encode_index(ets, ExtKey, Index, Value)).
	

%% @private Serializes and generates a backend metadata
-spec encode_meta_key(ets, term()) ->
	tuple() | binary().

encode_meta_key(ets, Term) -> 
	{?MVSN, Term}.
% encode_meta_key(leveldb, Term) ->
% 	sext:encode(encode_meta_key(ets, Term)).



%% @private Deserializes and gets a key or index key from the backend
-spec decode_key(nkbase:backend(), tuple()|binary()) ->
	{data, nkbase:ext_key()} | {meta, nkbase:ext_key()} |
	{index, {nkbase:ext_key(), nkbase:index_name(), term()}} |
	{meta, term()} | {error, term()}.

decode_key(ets, {?DVSN, Dom, Class, Key}) ->
	{data, {Dom, Class, Key}};
decode_key(ets, {?IVSN, Dom, Class, Index, Value, Key}) -> 
	{index, {{Dom, Class, Key}, Index, Value}};
decode_key(ets, {?MVSN, Term}) -> 
	{meta, Term};
decode_key(ets, _) -> 
	{error, decode_error};
decode_key(leveldb, BinKey) ->
	case catch sext:decode(BinKey) of
		{'EXIT', _} -> {error, decode_error};
		Decode -> decode_key(ets, Decode)
	end.


%% @private Serializes and generates a backend object
-spec encode_data(nkbase:backend(), dvvset:clock()) ->
	tuple() | binary().

encode_data(ets, DVV) ->
	{?DVSN, DVV};
encode_data(leveldb, DVV) ->
	term_to_binary(encode_data(ets, DVV)).


%% @private Deserializes and gets an object from the backend
-spec decode_data(nkbase:backend(), tuple() | binary()) ->
	{ok, list(), dvvset:clock()} | {error, Error}
	when Error :: unknown_version | decode_error.

decode_data(ets, {?DVSN, DVV}) ->
	{ok, DVV};
decode_data(ets, _) ->
	{error, decode_error};
decode_data(leveldb, Bin) ->
	decode_data(ets, catch binary_to_term(Bin)).


%% @private Folds over the backend
%% Iterates over all objects in the backend, starting with Key, and folding with
%% FoldFun(Key, Data, Acc). Data will be '$none' for keys folding type.
%% FoldFun can request to jump to another key
-spec do_fold(nkbase:backend(), asc|desc, db_ref(), 
			 '$nk_first'|'$nk_last'|term(),
			  keys|values, fold_fun(), fold_acc()) ->
	{done, term()} | {error, term()}.


do_fold(ets, asc, Ref, '$nk_first', FoldType, FoldFun, Acc) -> 
	do_fold(ets, asc, Ref, ets:first(Ref), FoldType, FoldFun, Acc);

do_fold(ets, desc, Ref, '$nk_last', FoldType, FoldFun, Acc) -> 
	do_fold(ets, desc, Ref, ets:last(Ref), FoldType, FoldFun, Acc);

do_fold(ets, Order, Ref, Key, FoldType, FoldFun, Acc) -> 
	Next = case ets:member(Ref, Key) of
		true -> Key;
		false when Order==asc -> ets:next(Ref, Key);
		false when Order==desc -> ets:prev(Ref, Key)
	end,
	do_fold_ets(Order, Ref, Next, FoldType, FoldFun, Acc);
				
do_fold(leveldb, asc, Ref, '$nk_first', FoldType, FoldFun, Acc) -> 
	do_fold(leveldb, asc, Ref, first, FoldType, FoldFun, Acc);

do_fold(leveldb, desc, Ref, '$nk_last', FoldType, FoldFun, Acc) -> 
	do_fold(leveldb, desc, Ref, last, FoldType, FoldFun, Acc);

do_fold(leveldb, Order, Ref, Key, FoldType, FoldFun, Acc) -> 
	{ok, Itr} = case FoldType of
		keys ->	eleveldb:iterator(Ref, [], keys_only);
		values -> eleveldb:iterator(Ref, [])
	end,
	try
		case Order of
			asc ->
				case leveldb_move(Itr, Key) of
					{ok, NewKey, NewData} -> 
						do_fold_leveldb(Itr, next, NewKey, NewData, FoldFun, Acc);
					done ->	
						{done, Acc};
					{error, Error} -> 
						{error, Error}
				end;
			desc ->
				case leveldb_move(Itr, Key) of
					{ok, Key, Data} -> 
						do_fold_leveldb(Itr, prev, Key, Data, FoldFun, Acc);
					{ok, _, _} ->	
						case leveldb_move(Itr, prev) of
							{ok, NewKey, NewData} ->
								do_fold_leveldb(Itr, prev, NewKey, NewData, FoldFun, Acc);
							done ->
								{done, Acc};
							{error, Error} ->
								{error, Error}
						end;		
					done ->
						case leveldb_move(Itr, last) of
							{ok, NewKey, NewData} ->
								do_fold_leveldb(Itr, prev, NewKey, NewData, FoldFun, Acc);
							done ->
								{done, Acc};
							{error, Error} ->
								{error, Error}
						end;		
					{error, Error} ->
						{error, Error}
				end
		end
	after
		eleveldb:iterator_close(Itr)
	end.
			

%% @private
-spec do_fold_ets(asc|desc, db_ref(), binary(), keys|values, 
					 fold_fun(), fold_acc()) ->
	{done, term()} | {error, term()}.

do_fold_ets(_, _, '$end_of_table', _, _, Acc) -> 
	{done, Acc};

do_fold_ets(Order, Ref, Key, FoldType, Fun, Acc) -> 
	Op = case FoldType of
		keys -> 
			Fun(Key, none, Acc);
		values -> 
			case ets:lookup(Ref, Key) of
				[{_, Data}] -> Fun(Key, Data, Acc);
				[] -> skip							% The key has been removed
			end
	end,
	case Op of
		{iter, NewAcc} when Order==asc -> 
			do_fold_ets(asc, Ref, ets:next(Ref, Key), FoldType, Fun, NewAcc);
		{iter, NewAcc} when Order==desc -> 
			do_fold_ets(desc, Ref, ets:prev(Ref, Key), FoldType, Fun, NewAcc);
		{done, NewAcc} ->
			{done, NewAcc};
		{error, Error} ->
			{error, Error};
		{jump, Jump, NewAcc} -> 
			do_fold_ets(Order, Ref, ets:next(Ref, Jump), FoldType, Fun, NewAcc);
		skip when Order==asc ->
			do_fold_ets(asc, Ref, ets:next(Ref, Key), FoldType, Fun, Acc);
		skip when Order==desc ->
			do_fold_ets(desc, Ref, ets:prev(Ref, Key), FoldType, Fun, Acc)
	end.



%% @private
-spec do_fold_leveldb(term(), next|prev, binary(), binary(), fold_fun(), fold_acc()) ->
	{done, term()} | {error, term()}.

do_fold_leveldb(Itr, Act, Key, Data, Fun, Acc) -> 
	case Fun(Key, Data, Acc) of
		{iter, NewAcc} ->
			case leveldb_move(Itr, Act) of
				{ok, NewKey, NewData} -> 
					do_fold_leveldb(Itr, Act, NewKey, NewData, Fun, NewAcc);
				done -> 
					{done, NewAcc};
				{error, Error} -> 
					{error, Error}
			end;
		{done, NewAcc} ->
			{done, NewAcc};
		{error, Error} ->
			{error, Error};
		{jump, Jump, NewAcc} -> 
			case leveldb_move(Itr, Jump) of
				{ok, NewKey, NewData} -> 
					do_fold_leveldb(Itr, Act, NewKey, NewData, Fun, NewAcc);
				done -> 
					{done, NewAcc};
				{error, Error} -> 
					{error, Error}
			end
	end.


%% @private
-spec leveldb_move(term(), first|last|next|prev|binary()) ->
	{ok, NewKey::binary(), NewData::binary()|none} | done | {error, term()}.

leveldb_move(Itr, Key) ->
	case catch eleveldb:iterator_move(Itr, Key) of
		{ok, NewKey} -> {ok, NewKey, none};
		{ok, NewKey, NewData} -> {ok, NewKey, NewData};
		{error, invalid_iterator} -> done;
		{error, Error} -> {error, Error};
		{'EXIT', Error} -> {error, Error}
	end.


%% @private
-spec fold_check_abort(integer(), integer(), integer(), term()) ->
	ok | no_return().

fold_check_abort(Iters, Items, MaxItems, Acc) ->
	case is_integer(MaxItems) andalso Items >= MaxItems of 
		true -> throw({done, Acc});
		false -> ok
	end,
	case (Iters rem 1000) == 0 of
		true ->
			receive
				'$nkbase_abort' -> throw({done, Acc})
			after
				0 -> ok
			end;
		false ->
			ok
	end.


		