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

-module(backend_test).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').

-compile([export_all]).
-include_lib("eunit/include/eunit.hrl").
-include("nkbase.hrl").

-define(BULK_DOMAINS, 5).
-define(BULK_CLASSES, 10).
-define(BULK_KEYS, 1000).
-define(NUM_ITEMS, ?BULK_DOMAINS*?BULK_CLASSES*?BULK_KEYS).


backend_ets_test_() ->
  	{setup, spawn, 
    	fun() -> 
    		% We need to start riak to access to medatada
    		test_util:start(),
			?debugMsg("Starting backed test for ets"),
			ets:new(backend_test_m, [named_table, ordered_set, public]),
			ets:new(backend_test_s, [named_table, ordered_set, public]),
			{backend_test_m, backend_test_s}
		end,
		fun({MemM, MemS}) -> 
			[] = ets:tab2list(MemM),
			ets:delete(MemM),
			ets:delete(MemS),
			test_util:stop()
		end,
		fun({MemM, MemS}) ->
		    [
				fun() -> put_remove({ets, MemM}) end,
				fun() -> dist_store({ets, MemM}, {ets, MemS}) end,
				{timeout, 60, fun() -> bulk_insert({ets, MemM}) end},
				fun() -> scan_domains({ets, MemM}) end,
				fun() -> scan_classes({ets, MemM}) end,
				fun() -> scan_keys({ets, MemM}) end,
				fun() -> search1({ets, MemM}) end,
				fun() -> search2({ets, MemM}) end,
				{timeout, 60, fun() -> bulk_remove({ets, MemM}) end}
			]
		end
  	}.


backend_leveldb_test_() ->
  	{setup, spawn, 
    	fun() -> 
    		% We need to start riak to access to medatada
    		test_util:start(),
			?debugMsg("Starting backed test for leveldb"),
			Unique = riak_core_util:unique_id_62(),
			PathM = filename:join("/tmp", Unique ++ "_m"),
			PathS = filename:join("/tmp", Unique ++ "_s"),
			Opts = [{create_if_missing, true}, {max_open_files, 100}],
			{ok, LevelM} = eleveldb:open(PathM, Opts),
			{ok, LevelS} = eleveldb:open(PathS, Opts),
			{ok, PathM, LevelM, PathS, LevelS}
		end,
		fun({ok, PathM, LevelM, PathS, LevelS}) -> 
			true = eleveldb:is_empty(LevelM),
			ok = nkbase_backend:leveldb_destroy(LevelM, PathM),
			ok = nkbase_backend:leveldb_destroy(LevelS, PathS),
			test_util:stop()
		end,
		fun({ok, _PathM, LevelM, _File2, LevelS}) ->
		    [
				{timeout, 60, fun() -> put_remove({leveldb, LevelM}) end},
				{timeout, 60, fun() -> dist_store({leveldb, LevelM}, {leveldb, LevelS}) end},
				{timeout, 60, fun() -> bulk_insert({leveldb, LevelM}) end},
				{timeout, 60, fun() -> scan_domains({leveldb, LevelM}) end},
				{timeout, 60, fun() -> scan_classes({leveldb, LevelM}) end},
				{timeout, 60, fun() -> scan_keys({leveldb, LevelM}) end},
				{timeout, 60, fun() -> search1({leveldb, LevelM}) end},
				{timeout, 60, fun() -> search2({leveldb, LevelM}) end},
				{timeout, 60, fun() -> bulk_remove({leveldb, LevelM}) end}
			]
		end
  	}.



% all() ->
% 	M = backend_test_m,
% 	ets:new(M, [named_table, ordered_set, public]),
% 	bulk_insert({ets, M}),
% 	search1({ets, M}).


%% Put and Remove
put_remove(Bk) ->
	Key = {d, c, k2},
	IndexSpec1 = [
		{i1, {field, a}}, 
		{i2, {field, b}, [normalize]}, 
		{<<"i3">>, vi3},
		{i4, {field, c}}
	],
	Meta1 = #{update=>actor1, indices=>IndexSpec1},

	% We save first object, i1=a1, i2=<<"b1">>, i3=vi3
	Obj1 = #{vsn=>1, a=>a1, b=>b1},
	ExtObj1 = nkbase:make_ext_obj(Key, Obj1, Meta1),
	DVV1 = dvvset:new(ExtObj1),
	{ok, Indices1, DVV1a} = nkbase_backend:put(Bk, Key, DVV1, Meta1),
	{ok, DVV1a} = nkbase_backend:get(Bk, Key),
	Indices1 = nkbase_backend:extract_indices(DVV1a),
	[{i1, a1}, {i2, <<"b1">>}, {<<"i3">>, vi3}] = Indices1,
	[ExtObj1] = dvvset:values(DVV1a),
	test_indices(Indices1, [], Bk, Key),

	ok = nkbase_backend:del(Bk, Key),
	{error, not_found} = nkbase_backend:get(Bk, Key),
	test_indices([], Indices1, Bk, Key),
	{error, not_found} = nkbase_backend:del(Bk, Key),
	ok.


%% Coordinator vnode test
dist_store(MasterBk, SlaveBk) ->
	Key = {d, c, k1},
	IndexSpec1 = [
		{i1, {field, a}}, 
		{i2, {field, b}, [normalize]}, 
		{<<"i3">>, vi3},
		{i4, {field, c}}
	],
	M_Meta = #{actor=>actor1, indices=>IndexSpec1},
	S_Meta = #{indices=>IndexSpec1},
	%% In this test, 'Master' means we use actor, so that the context will
	%% be incremented by this actor
	%% 'Slave' means we are not using actor, so the context is not incremented 

	
	%% I: No previous object

	% We save first object, i1=a1, i2=<<"b1">>, i3=vi3
	Obj1 = #{vsn=>1, a=>a1, b=>b1},
	ExtObj1 = nkbase:make_ext_obj(Key, Obj1, M_Meta),
	M_DVV1 = dvvset:new(ExtObj1),
	{ok, Indices1, M_DVV1a} = nkbase_backend:put(MasterBk, Key, M_DVV1, M_Meta),
	{ok, M_DVV1a} = nkbase_backend:get(MasterBk, Key),
	Indices1 = nkbase_backend:extract_indices(M_DVV1a),
	[{i1, a1}, {i2, <<"b1">>}, {<<"i3">>, vi3}] = Indices1,
	[ExtObj1] = dvvset:values(M_DVV1a),
	test_indices(Indices1, [], MasterBk, Key),

	% We copy the object to the slave
	{ok, Indices1, S_DVV1a} = nkbase_backend:put(SlaveBk, Key, M_DVV1a, S_Meta),
	{ok, S_DVV1a} = nkbase_backend:get(SlaveBk, Key),
	Indices1 = nkbase_backend:extract_indices(S_DVV1a),
	[ExtObj1] = dvvset:values(S_DVV1a),
	test_indices(Indices1, [], SlaveBk, Key),


	%% II: No context, duplicated value

	% We save seconds object, i1=a2, i3=vi3
	Obj2 = #{vsn=>2, a=>a2},
	ExtObj2 = nkbase:make_ext_obj(Key, Obj2, M_Meta),
	M_DVV2 = dvvset:new(ExtObj2),
	{ok, Indices2, M_DVV2a} = nkbase_backend:put(MasterBk, Key, M_DVV2, M_Meta),
	[ExtObj2, ExtObj1] = dvvset:values(M_DVV2a),
	{ok, M_DVV2a} = nkbase_backend:get(MasterBk, Key),
	Indices2 = nkbase_backend:extract_indices(M_DVV2a),
	[{i2, <<"b1">>}, {i1, a1}, {i1, a2}, {<<"i3">>, vi3}] = Indices2,
	test_indices(Indices2, [], MasterBk, Key),

	% We copy the object to the slave
	{ok, Indices2, S_DVV2a} = nkbase_backend:put(SlaveBk, Key, M_DVV2a, S_Meta),
	{ok, S_DVV2a} = nkbase_backend:get(SlaveBk, Key),
	Indices2 = nkbase_backend:extract_indices(S_DVV2a),
	[ExtObj2, ExtObj1] = dvvset:values(S_DVV2a),
	test_indices(Indices2, [], SlaveBk, Key),


	%% III: Update with correct context

	% We save third object, i1=a3, i3=vi3, i4=c3
	Ctx3 = dvvset:join(M_DVV2a),
	Obj3 = #{vsn=>3, a=>a3, c=>c3},
	ExtObj3 = nkbase:make_ext_obj(Key, Obj3, M_Meta),
	M_DVV3 = dvvset:new(Ctx3, ExtObj3),
	{ok, Indices3, M_DVV3a} = nkbase_backend:put(MasterBk, Key, M_DVV3, M_Meta),
	[ExtObj3] = dvvset:values(M_DVV3a),
	{ok, M_DVV3a} = nkbase_backend:get(MasterBk, Key),
	Indices3 = nkbase_backend:extract_indices(M_DVV3a),
	[{i1,a3}, {i4,c3}, {<<"i3">>,vi3}] = Indices3,
	test_indices(Indices3, Indices2, MasterBk, Key),

	% We copy the object to the slave
	{ok, Indices3, S_DVV3a} = nkbase_backend:put(SlaveBk, Key, M_DVV3a, S_Meta),
	{ok, S_DVV3a} = nkbase_backend:get(SlaveBk, Key),
	Indices3 = nkbase_backend:extract_indices(S_DVV3a),
	[ExtObj3] = dvvset:values(S_DVV3a),
	test_indices(Indices3, Indices2, SlaveBk, Key),


	%% IV: Update with incompatible context

	% We generate a divergent context from DVV2a
	Ctx4 = dvvset:join(M_DVV2a),
	Obj4 = #{vsn=>4, a=>a4},
	ExtObj4 = nkbase:make_ext_obj(Key, Obj4, M_Meta),
	M_DVV4 = dvvset:new(Ctx4, ExtObj4),
	{ok, Indices4, M_DVV4a} = 
		nkbase_backend:put(MasterBk, Key, M_DVV4, M_Meta#{actor=>actor2}),
	[ExtObj3, ExtObj4] = dvvset:values(M_DVV4a),
	{ok, M_DVV4a} = nkbase_backend:get(MasterBk, Key),
	Indices4 = nkbase_backend:extract_indices(M_DVV4a),
	[{i1, a4}, {i1,a3}, {i4,c3}, {<<"i3">>,vi3}] = Indices4,
	test_indices(Indices4++Indices3, [], MasterBk, Key),
	[{actor1,3}, {actor2,1}] = Ctx4b = dvvset:join(M_DVV4a),
	
	% We copy the object to the slave, and the same happens
	{ok, Indices4, S_DVV4a} = nkbase_backend:put(SlaveBk, Key, M_DVV4a, S_Meta),
	{ok, S_DVV4a} = nkbase_backend:get(SlaveBk, Key),
	Indices4 = nkbase_backend:extract_indices(S_DVV4a),
	[ExtObj3, ExtObj4] = dvvset:values(S_DVV4a),
	test_indices(Indices4++Indices3, [], SlaveBk, Key),
	Ctx4b = dvvset:join(S_DVV4a),


	%% V: Reconcile

	% We could avoid this using reconciliation
	{ok, Indices4b, M_DVV4b} = nkbase_backend:put(MasterBk, Key, M_DVV4,
									   M_Meta#{actor=>actor2, reconcile=>lww}),
	[ExtObj4] = dvvset:values(M_DVV4b),
	{ok, M_DVV4b} = nkbase_backend:get(MasterBk, Key),
	Indices4b = nkbase_backend:extract_indices(M_DVV4b),
	[{i1, a4}, {<<"i3">>,vi3}] = Indices4b,
	test_indices(Indices4b, Indices3, MasterBk, Key),
	% However we keep the state
	[{actor1,3}, {actor2,2}] = Ctx4c = dvvset:join(M_DVV4b),

	% And the same on the slave
	% We don't need the reconcile option, because we are receiving a winning
	% context anycase
	{ok, Indices4b, S_DVV4b} = nkbase_backend:put(SlaveBk, Key, M_DVV4b, S_Meta),
	{ok, S_DVV4b} = nkbase_backend:get(SlaveBk, Key),
	Indices4b = nkbase_backend:extract_indices(S_DVV4b),
	[ExtObj4] = dvvset:values(S_DVV4b),
	test_indices(Indices4b, Indices3, SlaveBk, Key),
	Ctx4c = dvvset:join(S_DVV4b),


	%% VI: Old value
	
	% We save and old value again (to the slave, not using actor).
	% The stored value is not affected
	% If we do this to master (using actor), the context is incremented and it is
	% a different one, resulting in a conflict
	{ok, Indices4b, S_DVV4b} = nkbase_backend:put(SlaveBk, Key, M_DVV3a, S_Meta),


	%% VI: Deletion

	Obj5 = '$nkdeleted',
	ExtObj5 = nkbase:make_ext_obj(Key, Obj5, #{ttl=>undefined}),
	M_DVV5 = dvvset:new(Ctx4c, ExtObj5),
	{ok, [], M_DVV5a} = nkbase_backend:put(MasterBk, Key, M_DVV5, M_Meta),
	[ExtObj5] = dvvset:values(M_DVV5a),
	{ok, M_DVV5a} = nkbase_backend:get(MasterBk, Key),
	[] = nkbase_backend:extract_indices(M_DVV5a),
	test_indices([], Indices4b, MasterBk, Key),
	
	% We copy the object to the slave
	{ok, [], S_DVV5a} = nkbase_backend:put(SlaveBk, Key, M_DVV5a, S_Meta),
	{ok, S_DVV5a} = nkbase_backend:get(SlaveBk, Key),
	[] = nkbase_backend:extract_indices(S_DVV5a),
	[ExtObj5] = dvvset:values(S_DVV5a),


	%% VII: Remove
	ok = nkbase_backend:del(MasterBk, Key),
	ok = nkbase_backend:del(SlaveBk, Key),
	ok.


bulk_insert(Bk) ->
	?debugFmt("Inserting ~p records...", [?NUM_ITEMS]),
	Domains = lists:seq(1, ?BULK_DOMAINS),
	Classes = lists:seq(1, ?BULK_CLASSES),
	Keys = lists:seq(1, ?BULK_KEYS),
	lists:foreach(
		fun(Domain) ->
			lists:foreach(
				fun(Class) ->
					lists:foreach(
						fun(Key) ->
							F1 = crypto:rand_uniform(1,100),
							bulk_insert(Bk, {d, Domain}, {c, Class}, {k, Key}, F1)
						end,
						Keys)
				end,
				Classes)
		end,
		Domains),
	?debugMsg("...done").


bulk_insert(Bk, Domain, Class, Key, F1) ->
	ExtKey = {Domain, Class, Key},
	Obj = #{k => Key, f1 => F1},
	IndexSpec = [{ik, key}, {i1, {field, f1}}, {{'$g', i2}, {field, f1}}],
	Meta = #{indices=>IndexSpec},
	ExtObj = nkbase:make_ext_obj(ExtKey, Obj, Meta),
	DVV = dvvset:new(ExtObj),
	{ok, _, _} = nkbase_backend:put(Bk, ExtKey, DVV, Meta).




scan_domains({Bk, Ref}) ->
	Fun = fun({D, none, none}, [], Acc) -> [D|Acc] end,
	Spec = #fold_spec{
		backend = Bk,
		db_ref = Ref,
		fold_fun = Fun,
		fold_acc = []
	},
	{done, List} = nkbase_backend:fold_domains(Spec),
	List = lists:reverse([{d, D} ||  D <- lists:seq(1,?BULK_DOMAINS)]),
	ok.


scan_classes({Bk, Ref}) ->
	Fun = fun({D, C, none}, [], Acc) -> [{D, C}|Acc] end,
	Spec1 = #fold_spec{
		backend = Bk,
		db_ref = Ref,
		domain = {d, 1},
		fold_fun = Fun,
		fold_acc = []
	},
	{done, List1} = nkbase_backend:fold_classes(Spec1),
	List1 = lists:reverse([{{d, 1}, {c, C}} ||  C <- lists:seq(1,?BULK_CLASSES)]),

	Spec2 = Spec1#fold_spec{domain={d, ?BULK_DOMAINS}},
	{done, List2} = nkbase_backend:fold_classes(Spec2),
	List2 = lists:reverse(
				[{{d, ?BULK_DOMAINS}, {c, C}} ||  C <- lists:seq(1,?BULK_CLASSES)]),
	ok.


scan_keys({Bk, Ref}) ->
	Fun1 = fun(K, V, Acc) ->
		case V of
			[] -> [K|Acc];
			_ -> [{K, V}|Acc]
		end
	end,
	Spec1 = #fold_spec{
		backend = Bk,
		db_ref = Ref,
		fold_fun = Fun1,
		fold_acc = [],
		keyfilter = fun(_) -> true end,
		domain={d, 1}, 
		class={c, 1},
		max_items = ?NUM_ITEMS
	},
	{done, List1} = nkbase_backend:fold_objs(Spec1),
	?BULK_KEYS = length(List1),
	[{{d,1}, {c,1}, {k, ?BULK_KEYS}}|_] = List1,
	[{{d,1}, {c,1}, {k, 1}}|_] = lists:reverse(List1),

	{done, []} = nkbase_backend:fold_objs(Spec1#fold_spec{class={c,0}}),

	Spec2 = Spec1#fold_spec{start={k,5}},
	{done, List2} = nkbase_backend:fold_objs(Spec2),
	Length2 = ?BULK_KEYS-4,
	Length2 = length(List2),
	[{{d,1}, {c,1}, {k, ?BULK_KEYS}}|_] = List2,
	[{{d,1}, {c,1}, {k, 5}}|_] = lists:reverse(List2),

	Spec3 = Spec2#fold_spec{start={k,2}, stop={k,5}},
	{done, List3} = nkbase_backend:fold_objs(Spec3),
	[5, 4, 3, 2] = [K || {{d,1}, {c,1}, {k, K}} <-List3],

	{done, [_, _]} = nkbase_backend:fold_objs(Spec3#fold_spec{max_items=2}),

	{done, [_,_,_,_]=List4} = nkbase_backend:fold_objs(Spec3#fold_spec{fold_type=values}),
	% lager:warning("List4: ~p", [List4]),
	lists:foreach(
		fun({{{d,1}, {c,1}, {k, Key}}, Values4}) -> 
			[{_Meta, #{k:={k,Key}}}] = Values4,
			ok
		end,
		List4),

	% Reach the end of db
	Spec5 = Spec1#fold_spec{
		domain = {d, ?BULK_DOMAINS}, 
		class = {c, ?BULK_CLASSES},
		start = {k, ?BULK_KEYS-2}
	},
	{done, [_,_,_]=List5} = nkbase_backend:fold_objs(Spec5),
	[{{d, ?BULK_DOMAINS}, {c, ?BULK_CLASSES}, {k, ?BULK_KEYS}}|_] = List5,


	%% All objects
	Fun6 = fun(Key, [], Acc) ->
		case Key of
			{{d,1},{c,1},{k,1}} -> 1+Acc;
			{{d,?BULK_DOMAINS},{c,?BULK_CLASSES},{k,?BULK_KEYS}} -> 1+Acc;
			_ -> Acc
		end
	end,
	Spec6 = Spec1#fold_spec{
		fold_fun = Fun6,
		fold_acc = 0,
		domain = '$nk_all',
		class = '$nk_all'
	},
	{done, 2} = nkbase_backend:fold_objs(Spec6),

	
	%% All objects for a domain
	Fun7 = fun(Key, [], Acc) ->
		case Key of
			{{d,3},{c,1},{k,1}} -> 1+Acc;
			{{d,3},{c,?BULK_CLASSES},{k,?BULK_KEYS}} -> 1+Acc;
			{{d,3}, _, __} -> Acc
		end
	end,
	Spec7 = Spec6#fold_spec{
		fold_fun = Fun7,
		domain = {d,3}
	},
	{done, 2} = nkbase_backend:fold_objs(Spec7),


	%% All objects for a class
	Spec8 = Spec7#fold_spec{
		fold_fun = fun({{d,3},{c,3},{k,K}}, [], Acc) -> [K|Acc] end,
		fold_acc = [],
		class = {c,3}
	},
	{done, List8} = nkbase_backend:fold_objs(Spec8),
	List8 = lists:seq(?BULK_KEYS, 1, -1),
	ok.


	
search1({Bk, Ref}) ->
	Fun1 = fun(V, K, [], Acc) -> {iter, [{K, V}|Acc]} end,
	Spec1 = #search_spec{
		backend = Bk,
		db_ref = Ref,
		domain = {d, 1}, 
		class = {c, 1},
		indices = [{ik, [all]}],
		order = asc,
		next = undefined,
		fold_fun = Fun1,
		fold_acc = [],
		max_items = 1000000
	},
	{done, List1} = nkbase_backend:search(Spec1),
	?BULK_KEYS = length(List1),
	List1 = [{{{d,1},{c,1},{k,K}}, {k,K}} || K <- lists:seq(?BULK_KEYS, 1, -1)],
	{done, List1b} = nkbase_backend:search(Spec1#search_spec{order=desc}),
	?BULK_KEYS = length(List1b),
	List1b = [{{{d,1},{c,1},{k,K}}, {k,K}} || K <- lists:seq(1, ?BULK_KEYS)],


	Spec2 = Spec1#search_spec{indices=[{ik, [{range, {k,3}, {k,5}}]}]},
	{done, List2} = nkbase_backend:search(Spec2),
	% io:format("A: ~p\nB: ~p\n", [List2, [{{{d,1},{c,1},{k,K}}, {k,K}} || K <- lists:seq(5, 3, -1)]]),

	List2 = [{{{d,1},{c,1},{k,K}}, {k,K}} || K <- lists:seq(5, 3, -1)],
	Spec2b = Spec1#search_spec{indices=[{ik, [{range, {k,3}, {k,5}}]}], order=desc},
	{done, List2b} = nkbase_backend:search(Spec2b),
	List2b = [{{{d,1},{c,1},{k,K}}, {k,K}} || K <- lists:seq(3, 5)],

	Spec3 = Spec2#search_spec{next={{k,4}, {k,4}}},
	{done, List3} = nkbase_backend:search(Spec3),
	List3 = [{{{d,1},{c,1},{k,K}}, {k,K}} || K <- lists:seq(5, 4, -1)],

	{done, List3b} = nkbase_backend:search(Spec3#search_spec{order=desc}),
	List3b = [{{{d,1},{c,1},{k,K}}, {k,K}} || K <- lists:seq(3, 4)],

	Spec4 = Spec1#search_spec{
		domain = {d, ?BULK_DOMAINS},
		class = {c, ?BULK_CLASSES},
		indices = [{i1, [all]}]
	},
	{done, List4} = nkbase_backend:search(Spec4),
	?BULK_KEYS = length(List4),
	lists:foreach(
		fun({ExtKey, Obj}) ->
			{ok, DVV} = nkbase_backend:get({Bk, Ref}, ExtKey),
			[{_, #{f1:=Obj}}] = dvvset:values(DVV)
		end,
		List4),

	Spec5 = Spec1#search_spec{
		domain = '$nkbase',
		class = '$g',
		indices = [{i2, [all]}]
	},
	{done, List5} = nkbase_backend:search(Spec5),
	Total = ?BULK_DOMAINS * ?BULK_CLASSES * ?BULK_KEYS,
	Total = length(List5),
	lists:foreach(
		fun({{'$nkbase', '$g', ExtKey}, Obj}) ->
			{ok, DVV} = nkbase_backend:get({Bk, Ref}, ExtKey),
			[{_, #{f1:=Obj}}] = dvvset:values(DVV)
		end,
		List5).


search2({Bk, Ref}) ->
	Fun1 = fun(V, K, Body, Acc) -> {iter, [{K, V, Body}|Acc]} end,
	Spec1 = #search_spec{
		backend = Bk,
		db_ref = Ref,
		domain = {d, 1}, 
		class = {c, 1},
		indices = [{ik, [all]}, {i1, [{ge, 50}]}],
		fold_type = keys,
		order = asc,
		next = undefined,
		fold_fun = Fun1,
		fold_acc = [],
		max_items = 1000000
	},

	% Several indices
	{done, List1} = nkbase_backend:search(Spec1),
	K1 = [N || {{{d,1}, {c,1}, {k,N}}, {k,N}, []} <- lists:reverse(List1)],
	Spec2 = Spec1#search_spec{indices=[{i1, [{ge, 50}]}, {ik, [all]}]},
	{done, List2} = nkbase_backend:search(Spec2),
	K1 = [N || {{{d,1}, {c,1}, {k,N}}, V, []} <- lists:sort(List2), is_integer(V)],

	{done, List3} = nkbase_backend:search(Spec1#search_spec{order=desc}),
	List3 = lists:reverse(List1),
	{done, List4} = nkbase_backend:search(Spec2#search_spec{order=desc}),
	List4 = lists:reverse(List2),

	% Get values and indices
	{done, List5} = nkbase_backend:search(Spec1#search_spec{fold_type=values}),
	K1 = [N || {{{d,1}, {c,1}, {k,N}}, {k,N}, [{_ObjMeta, #{k:={k,N}, f1:=F1}}]}
				<- lists:reverse(List5), is_integer(F1)],

	{done, List6} = nkbase_backend:search(Spec1#search_spec{fold_type=values}),
	K1 = [
		N || 
		{{{d,1}, {c,1}, {k,N}}, {k,N}, [{#{idx:=[{i1, V}|_]}, #{k:={k,N}, f1:=V}}]}
		<- lists:reverse(List6)
	],

	% Conflicting and deleted values
	% Insert some conflicting values
	bulk_insert({Bk, Ref}, {d,1}, {c,1}, {k, 1}, 101),
	bulk_insert({Bk, Ref}, {d,1}, {c,1}, {k, 1}, 102),
	bulk_insert({Bk, Ref}, {d,1}, {c,1}, {k, 2}, 103),
	bulk_insert({Bk, Ref}, {d,1}, {c,1}, {k, 3}, 104),
	ExtKey1 = {{d,1}, {c,1}, {k,3}},
	ExtObj1 = nkbase:make_ext_obj(ExtKey1, '$nkdeleted', #{}),
	{ok, _, _} = nkbase_backend:put({Bk, Ref}, ExtKey1, dvvset:new(ExtObj1), #{}),

	Spec3 = Spec1#search_spec{indices = [{ik, [{lt, {k,4}}]}]},
	% We don't use get_values, and don't use secondary indices
	{done, [{_, {k,3}, []}, {_, {k,2}, []}, {_, {k,1}, []}]} = 
		nkbase_backend:search(Spec3),

	{ok, DVV1} = nkbase_backend:get({Bk, Ref}, {{d,1},{c,1},{k,1}}),
	[{_, #{f1:=I11}}, {_, #{f1:=101}}, {_, #{f1:=102}}] = lists:sort(dvvset:values(DVV1)),
	{ok, DVV2} = nkbase_backend:get({Bk, Ref}, {{d,1},{c,1},{k,2}}),
	[{_, #{f1:=I12}}, {_, #{f1:=103}}] = lists:sort(dvvset:values(DVV2)),
	{ok, DVV3} = nkbase_backend:get({Bk, Ref}, {{d,1},{c,1},{k,3}}),
	[{_, '$nkdeleted'}, {_, #{f1:=I13}}, {_, #{f1:=104}}] = lists:sort(dvvset:values(DVV3)),

	{done, List7} = 
		nkbase_backend:search(Spec3#search_spec{fold_type=values}),
	[
		{{k,3}, [I13, 104]},
 		{{k,2}, [I12, 103]},
		{{k,1}, [I11, 101, 102]}
	] 
	=
		[
			{K, lists:sort([F1 || {_, #{f1:=F1}} <- Vs])}
			||
			{_, K, Vs} <- List7
		],

    % Extract correct value from conflicting set
	Spec4 = Spec1#search_spec{
		indices = [{ik, [{lt, {k,2}}]}, {i1, [{gt, 100}]}],
		fold_type = values
	},
	{done, List8} = nkbase_backend:search(Spec4),
	[{{k,1}, [101, 102]}] = 
		[
			{K, lists:sort([I1 || {#{idx:=[{i1, I1}|_]}, _} <- Vs])}
			|| 
			{_, K, Vs} <- List8
		],

	% Holes
	Spec5 = Spec1#search_spec{
		indices = [{ik, [{eq, {k,2}}, {range, {k,2}, {k, 3}}, {eq, {k, 5}}, {gt, {k, ?BULK_KEYS-2}}]}],
		order = desc
	},
	{done, List9} = nkbase_backend:search(Spec5),
	List9 = [{{{d,1},{c,1},{k,P}},{k,P},[]} || P <- [2,3,5,?BULK_KEYS-1,?BULK_KEYS]],
	ok.



bulk_remove({Bk, Ref}) ->
	?debugFmt("Deleting ~p records...", [?NUM_ITEMS]),
	Domains = lists:seq(1, ?BULK_DOMAINS),
	Classes = lists:seq(1, ?BULK_CLASSES),
	Keys = lists:seq(1, ?BULK_KEYS),
	lists:foreach(
		fun(Domain) ->
			lists:foreach(
				fun(Class) ->
					lists:foreach(
						fun(Key) ->
							ExtKey = {{d, Domain}, {c, Class}, {k, Key}},
							nkbase_backend:del({Bk, Ref}, ExtKey)
						end,
						Keys)
				end,
				Classes)
		end,
		Domains),
	?debugMsg("...done").






%%%%%%%%%%%%%%%%%%%%%%%% Util %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
	


test_indices(New, Old, Bk, Key) ->
	lists:foreach(
		fun({IK,IV}) -> true = nkbase_backend:has_idx(Bk, Key, IK, IV) end,
		New),
	lists:foreach(
		fun({IK,IV}) -> false = nkbase_backend:has_idx(Bk, Key, IK, IV) end,
		Old -- New).





