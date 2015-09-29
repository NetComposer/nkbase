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

%% @doc Comands Processing
-module(nkbase_cmds).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').

-export([get/2, put/3, update/4]).
-export([send_all/2, cmd_all/2, fold/8, search/4]).
-export([make_ext_obj/3, find_ttl/2]).
-export_type([update_fun/0]).

-include("nkbase.hrl").

%% Update fun
-type update_fun() ::
	fun((nkbase:ext_key(), [nkbase:ext_obj()], Arg::term(), Actor::term()) -> 
		{ok, nkbase:ext_obj()} | {error, term()}).		


%% ===================================================================
%% Public
%% ===================================================================


%% @doc Gets and object from the database
-spec get(nkbase:ext_key(), nkbase:class_meta()) ->
	{maps, {nkbase:ctx(), [nkbase:obj()]}} |
	{values, {nkbase:ctx(), [nkbase:obj()]}} |
	{error, term()}.

get(ExtKey, Meta) ->
	nkbase_vnode_get_fsm:get(ExtKey, Meta).


%% @doc Puts a new object in the database
-spec put(nkbase:ext_key(), nkbase:ext_obj(), nkbase:put_meta()) ->
	ok | {error, term()}.

put(ExtKey, {ObjMeta, Obj}, Meta) ->
	{Domain, Class, Key} = ExtKey,
	try
		if 
			Domain >= <<255>> -> throw(invalid_domain);
			Class >= <<255>> -> throw(invalid_class);
			Key >= <<255>> -> throw(invalid_key);
			true -> ok
		end,
		{ExtKey1, {ObjMeta1, Obj1}, Meta1} = case 
			maps:get(pre_write_hook, Meta, undefined) 
		of
			undefined -> {ExtKey, {ObjMeta, Obj}, Meta};
			PreFun when is_function(PreFun, 3) -> PreFun(ExtKey, {ObjMeta, Obj}, Meta)
		end,
		case nkbase_vnode_put_fsm:put(ExtKey1, {ObjMeta1, Obj1}, Meta1) of
			ok ->
				case maps:get(post_write_hook, Meta1, undefined) of
					undefined -> 
						ok;
					PostFun when is_function(PostFun, 3) -> 
						PostFun(ExtKey1, {ObjMeta1, Obj1}, Meta1)
				end,
				ok;
			{error, Error} ->
				{error, Error}
		end
	catch
		throw:Throw -> {error, Throw}
	end.


%% @private
-spec update(nkbase:ext_key(), update_fun(), term(), nkbase:put_meta()) ->
	ok | {error, term()}.

% The context will be read from the stored object
update(_, _, _, #{context:=_}) ->
	{error, context_not_allowed};

update(ExtKey, UpdateFun, Arg, Meta) ->
	Obj = {'$nkupdate', UpdateFun, Arg},
	{ExtObj, Meta1} = make_ext_obj(ExtKey, Obj, Meta),
	put(ExtKey, ExtObj, Meta1).



%% @private
-spec send_all(term(), nkbase:class_meta()) ->
	{ok, list()} | {error, term()}.

send_all(Cmd, Meta) ->
 	Timeout = maps:get(timeout, Meta, ?DEFAULT_TIMEOUT),
	Fun = fun(Term, Acc) -> [Term|Acc] end,
	case nkbase_coverage_fsm:launch(Cmd, 1, Timeout, Fun, []) of
		{ok, _T, List} -> {ok, List};
		{error, Error} -> {error, Error}
	end.


%% @private
-spec cmd_all(term(), nkbase:class_meta()) ->
	ok | {error, term()}.

cmd_all(Cmd, Meta) ->
 	case send_all(Cmd, Meta) of
		{ok, List} -> 
			case [Term || Term <- List, Term /= ok] of
				[] -> ok;
				[Error|_] -> {error, Error}
			end;
		{error, Error} ->
			{error, Error}
	end.


%% @private
-spec fold(nkbase:domain()|none, nkbase:class()|none, nkbase:class_meta(), 
		   domains|classes|keys|values, 
		   fun((nkbase:key(), [nkbase:ext_obj()], AccA::term()) -> AccA::term()), 
		   AccA0::term(),
		   fun((AccA::term(), AccB::term()) -> AccB::term()), AccB0::term()) ->
	{ok, term()} | {error, term()}.

fold(Domain, Class, Meta, Type, FunA, AccA0, FunB, AccB0) ->
	Meta1 = nkbase:get_class(Domain, Class, Meta),
	N = maps:get(n, Meta1, ?DEFAULT_N),
 	Timeout = maps:get(timeout, Meta1, ?DEFAULT_TIMEOUT),
 	{Cmd, FoldType} = case Type of
 		domains -> {fold_domains, keys};
 		classes -> {fold_classes, keys};
 		keys -> {fold_objs, keys};
 		values -> {fold_objs, values}
 	end,
	Spec = #fold_spec{
		backend = maps:get(backend, Meta1, ?DEFAULT_BACKEND),
		domain = Domain,
		class = Class,
		start = maps:get(start, Meta1, '$nk_first'),
		stop = maps:get(stop, Meta1, '$nk_last'),
		n = N,
		fold_type = FoldType,
		fold_fun = FunA,
		fold_acc = AccA0,
		timeout = Timeout,
		max_items = maps:get(page_size, Meta1, ?DEFAULT_PAGE_SIZE)
	},
	case nkbase_coverage_fsm:launch({Cmd, Spec}, N, Timeout, FunB, AccB0) of
		{ok, _T, Term} -> {ok, Term};
		{error, Error} -> {error, Error}
	end.



%% @private
-spec search(nkbase:domain(), nkbase:class(), nkbase:class_meta(), 
			 nkbase_search:ext_search_spec()) ->
	{ok, [{term(), nkbase:ext_key(), [nkbase:obj()]}]} | {error, term()}.

search(Domain, Class, Meta, Indices) ->
	Meta1 = nkbase:get_class(Domain, Class, Meta),
	N = maps:get(n, Meta1, ?DEFAULT_N),
 	Timeout = maps:get(timeout, Meta1, ?DEFAULT_TIMEOUT),
 	FunA = fun(Val, {_, _, Key}, Values, Acc) -> 
 		Data = nkbase_util:get_spec(Meta1, Values),
 		{iter, [{Val, Key, Data}|Acc]} 
 	end,
	Order = maps:get(order, Meta1, asc),
	PageSize = maps:get(page_size, Meta1, ?DEFAULT_PAGE_SIZE),
	FoldType = case 
		maps:is_key(get_values, Meta1) orelse
		maps:is_key(get_fields, Meta1) orelse
		maps:is_key(get_indices, Meta1)
	of
		true -> values;
		false -> keys
	end,
	Spec = #search_spec{
		backend = maps:get(backend, Meta1, ?DEFAULT_BACKEND),
		domain = Domain,
		class = Class,
		n = N,
		indices = Indices,
		order = Order,
		next = maps:get(next, Meta1, undefined),
		fold_type = FoldType,
		fold_fun = FunA,
		fold_acc = [],
		timeout = Timeout,
		max_items = PageSize
	},
	FunB = case Order of
		asc -> 
			fun(List, Acc) -> 
				lists:umerge(lists:reverse(List), Acc) end;
		desc ->
			fun(List, Acc) -> 
				lists:umerge(List, Acc) end
	end,
	case nkbase_coverage_fsm:launch({search, Spec}, N, Timeout, FunB, []) of
		{ok, T, List} -> 
			case T >= 200 of
				true -> lager:notice("Search operation time: ~p msecs", [T]);
				false -> lager:debug("Search operation time: ~p msecs", [T])
			end,
			List1 = case Order of 
				asc -> List;
				desc -> lists:reverse(List)
			end,
			case length(List1) > PageSize of
				true -> {ok, lists:sublist(List1, PageSize)};
				false -> {ok, List1}
			end;
		{error, Error} -> 
			{error, Error}	
	end.


%% @private 
-spec make_ext_obj(nkbase:ext_key(), nkbase:obj(), nkbase:put_meta()) ->
	{nkbase:ext_obj(), nkbase:put_meta()}.

make_ext_obj({Domain, Class, _Key}, '$nkdeleted', Meta) ->
	Meta1 = nkbase:get_class(Domain, Class, Meta),
	Now = nklib_util:l_timestamp(),
	ObjMeta1 = #{time=>Now},
	ObjMeta2 = case maps:get(ttl, Meta1, ?DEFAULT_DEL_TTL) of
		undefined -> ObjMeta1;
		TTL -> ObjMeta1#{time=>Now, exp=>round(Now+1000000*TTL)}
	end,
	{{ObjMeta2, '$nkdeleted'}, Meta1};

make_ext_obj({Domain, Class, _Key}=ExtKey, Obj, Meta) ->
	Meta1 = nkbase:get_class(Domain, Class, Meta),
	Now = nklib_util:l_timestamp(),
	ObjMeta1 = #{time=>Now},
	ObjMeta2 = case maps:get(indices, Meta1, []) of
		_ when element(1, Obj)=='$nkupdate' -> 
			ObjMeta1;
		[] -> 
			ObjMeta1;
		IndexSpec -> 
			Indices = nkbase_util:expand_indices(IndexSpec, ExtKey, Obj),
			ObjMeta1#{idx=>Indices}
	end,
	ObjMeta3 = case maps:get(ttl, Meta1, undefined) of
		undefined ->
			ObjMeta2;
		TTL when TTL > 0 ->
			ObjMeta2#{exp=>round(Now+1000000*TTL)}
	end,
	{{ObjMeta3, Obj}, Meta1}.



%% @private Finds the ttl for an object
%% It has to go over all records with ttl, do not use in production with many keys
%% with ttl 
-spec find_ttl(nkbase:ext_key(), nkbase:class_meta()) ->
	float() | none.

find_ttl(ExtKey, Meta) ->
	{ok, List} = nkbase_cmds:search('$nkbase', '$g', Meta, [{exp, [all]}]),
	case [V || {V, K, []} <- List, K == ExtKey] of
		[V] -> (V - nklib_util:l_timestamp()) / 1000000;
		[] -> none
	end.

