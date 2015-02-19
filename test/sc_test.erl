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

-module(sc_test).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').

-compile([export_all]).
-include_lib("eunit/include/eunit.hrl").
-include("nkbase.hrl").

-define(BULK_CLASSES, 10).
-define(BULK_KEYS, 1000).


sc_test_() ->
  	{setup, spawn, 
    	fun() -> 
    		% Check every 1 secs
    		application:set_env(nkbase, expire_check, 1, [persistent]),
    		% Expiration timers resolution to 1 msec
    		application:set_env(nkbase, expire_resolution, 1, [persistent]),
			{ok, Dir} = file:get_cwd(),
			%% This test does not work if we don't have a clean data dir...
			case lists:reverse(filename:split(Dir)) of
				[".eunit"|_] -> 
					?debugMsg("Removing data/ensembles dir under .eunit"),
					os:cmd("rm -rf data/ensembles");
				_ -> 
					?debugMsg("Not running under .eunit")
			end,
			?debugMsg("Waiting for ensembles to start..."),
    		test_util:start(),
			nkbase_ensemble:enable(),
			?debugMsg("... ok")
    	end,
		fun(_) -> test_util:stop() end,
	    fun(_) ->
		    [
				{timeout, 60, fun() -> bulk_insert() end},
				fun() -> scan_keys() end,
				fun() -> basic() end,
				{timeout, 60, fun() -> ttl() end},
				fun() -> search() end,
				{timeout, 60, fun() -> bulk_delete() end}
			]
		end
  	}.


all() ->
 	application:set_env(nkbase, expire_check, 1, [persistent]),
	application:set_env(nkbase, expire_resolution, 1, [persistent]),
	ok = nkbase_cmds:cmd_all(update_config, #{}),
	?debugMsg("Waiting for ensembles to start..."),
	nkbase_ensemble:enable(),
	?debugMsg("... ok"),
	bulk_insert(),
	scan_keys(),
	basic(),
	ttl(),
	search(),
	bulk_delete().


bulk_insert() ->
	Start = now(),
	?debugFmt("Inserting ~p SC records...", [?BULK_CLASSES*?BULK_KEYS]),
	Classes = lists:seq(1, ?BULK_CLASSES),
	Keys = lists:seq(1, ?BULK_KEYS),
	lists:foreach(
		fun(Class) ->
			lists:foreach(
				fun(Key) ->
					bulk_insert({?MODULE, bulk}, {c, Class}, {k, Key})
				end,
				Keys)
		end,
		Classes),
	Diff = timer:now_diff(now(), Start),
	?debugFmt("done (~p secs)\n", [Diff/1000000]).


bulk_insert(Domain, Class, Key) ->
	Obj = #{
		k => Key, 
		f1 => crypto:rand_uniform(1, 100)
	},
	Meta = #{
		indices => [{ik, key}, {i1, {field, f1}}, {{'$g', i2}, {field, f1}}],
		eseq => overwrite
	},
	{ok, _} = nkbase_sc:kput(Domain, Class, Key, Obj, Meta).


scan_keys() ->
	Meta = #{backend=>leveldb}, 
	D = {?MODULE, bulk},
	?debugMsg("Starting SC scan test"),
	{ok, List1} = nkbase:list_keys(D, {c,1}, Meta),
	[{k, 1}|_] = List1,
	[{k, ?BULK_KEYS}|_] = lists:reverse(List1),

	{ok, []} = nkbase:list_keys(D, {c,0}, Meta),

	{ok, List2} = nkbase:list_keys(D,{c,1}, Meta#{start=>{k,5}}),
	[{k, 5}|_] = List2,
	[{k, ?BULK_KEYS}|_] = lists:reverse(List2),

	{ok, List3} = nkbase:list_keys(D, {c,1}, Meta#{start=>{k,2}, stop=>{k,5}}),
	List3 = [{k,2}, {k,3}, {k,4}, {k, 5}],

	{ok, List4} = nkbase:list_keys(D, {c,1}, Meta#{page_size=>8}),
	8 = length(List4),

	C5 = crypto:rand_uniform(1, ?BULK_CLASSES+1),
	Fun5a = fun(K, [#{k:=K}], Acc) -> Acc+1 end,
	Fun5b = fun(C, Acc) -> C+Acc end,
	{ok, ?BULK_KEYS} = nkbase:iter_objs(D, {c,C5}, Meta, Fun5a, 0, Fun5b, 0).


basic() ->
	Meta = #{backend=>leveldb}, 
	?debugMsg("Starting put test"),
	D = {?MODULE, put},
	nkbase:remove_all(D, Meta),
	C = {c, 1},
	K1 = {k, 1},
	
	ok = nkbase:register_class(D, C, #{strong_consistency=>true}),
	{error, strong_consistency} = nkbase:put(D, C, K1, data1),
	{error, strong_consistency} = nkbase:get(D, C, K1),

	{error, not_found} = nkbase_sc:kget(D, C, K1),
	{ok, ESeq1} = nkbase_sc:kput(D, C, K1, object_1),
	{ok, ESeq1, object_1} = nkbase_sc:kget(D, C, K1),
	
	% Rewrite then object without context -> conflict
	{error, failed} = nkbase_sc:kput(D, C, K1, object_2),
	{ok, ESeq2} = nkbase_sc:kput(D, C, K1, object_2, #{eseq=>ESeq1}),
	{ok, ESeq2, object_2} = nkbase_sc:kget(D, C, K1),
	% Seq1 is no longer valid
	{error, failed} = nkbase_sc:kput(D, C, K1, object_2b, #{eseq=>ESeq1}),
	% But we can overwrite
	{ok, ESeq3} = nkbase_sc:kput(D, C, K1, object_3, #{eseq=>overwrite}),
	{ok, ESeq3, object_3} = nkbase_sc:kget(D, C, K1),
	true = (ESeq1/=ESeq2) andalso (ESeq2/=ESeq3),

	% Delete is just a kind of put
	{error, failed} = nkbase_sc:kdel(D, C, K1),
	{ok, ESeq3, object_3} = nkbase_sc:kget(D, C, K1),
	{error, failed} = nkbase_sc:kdel(D, C, K1, #{eseq=>ESeq1}),
	ok = nkbase_sc:kdel(D, C, K1, #{eseq=>ESeq3}),
	{error, not_found} = nkbase_sc:kget(D, C, K1),
	
	% We can write again with new, and force a delete
	{ok, ESeq4} = nkbase_sc:kput(D, C, K1, object_4),
	true = (ESeq2/=ESeq3) andalso (ESeq3/=ESeq4),
	{ok, ESeq4, object_4} = nkbase_sc:kget(D, C, K1),
	ok = nkbase_sc:kdel(D, C, K1, #{eseq=>overwrite}),
	{error, not_found} = nkbase_sc:kget(D, C, K1),

	% Funs
	PreFun = fun({{?MODULE, put}, {c,1}, {k,1}}, {ObjMeta, object_5}, FMeta) ->
		{{{?MODULE, put}, {c,1}, {k,2}}, {ObjMeta, object_5b}, FMeta}
	end,
	Self = self(),
	Ref = make_ref(),
	PostFun = fun({{?MODULE, put}, {c, 1}, {k, 2}}, {_, object_5b}, _) ->
		Self ! Ref
	end,

	{ok, ESeq5} = nkbase_sc:kput(D, C, K1, object_5, 
						#{pre_write_hook=>PreFun, post_write_hook=>PostFun}),
	receive Ref -> ok after 2000 -> error(post_fun) end,
	K2 = {k, 2},
	{error, not_found} = nkbase_sc:kget(D, C, K1),
	{ok, ESeq5, object_5b} = nkbase_sc:kget(D, C, K2),
	ok = nkbase_sc:kdel(D, C, K2, #{eseq=>overwrite}),
	ok.


ttl() ->
	?debugMsg("Starting SC TTL test"),
	Meta = #{backend=>leveldb},
	D = {?MODULE, ttl},
	C = {c, 1},
	K1 = {k, 1},

	nkbase:remove_all(D, Meta),
	{ok, ESeq1} = nkbase_sc:kput(D, C, K1, data1, Meta#{ttl=>0.5}),
	{ok, ESeq1, data1} = nkbase_sc:kget(D, C, K1),
	TTL1 = nkbase_cmds:find_ttl({D, C, K1}, Meta),
	true = TTL1 > 0.4 andalso TTL1 < 0.5,
	timer:sleep(500),
	{error, not_found} = nkbase_sc:kget(D, C, K1, Meta),
	none = nkbase_cmds:find_ttl({D, C, K1}, Meta),

	%% Updating the object with a new TTL
	K2 = {k, 2},
	{ok, ESeq2a} = nkbase_sc:kput(D, C, K2, data2a, Meta#{ttl=>0.1}),
	{ok, ESeq2a, data2a} = nkbase_sc:kget(D, C, K2),
	TTL2 = nkbase_cmds:find_ttl({D, C, K2}, Meta),
	true = TTL2 > 0 andalso TTL2 < 0.1,
	{ok, ESeq2b} = nkbase_sc:kput(D, C, K2, data2b, Meta#{eseq=>ESeq2a, ttl=>0.3}),
	{ok, ESeq2b, data2b} = nkbase_sc:kget(D, C, K2),
	TTL2b = nkbase_cmds:find_ttl({D, C, K2}, Meta),
	true = TTL2b > 0.2 andalso TTL2b < 0.3,
	timer:sleep(110),
	{ok, ESeq2b, data2b} = nkbase_sc:kget(D, C, K2),
	timer:sleep(250),
	{error, not_found} = nkbase_sc:kget(D, C, K2),

	%% Removing the TTL
	{ok, ESeq3a} = nkbase_sc:kput(D, C, K2, data3a, Meta#{ttl=>0.1}),
	{ok, ESeq3a, data3a} = nkbase_sc:kget(D, C, K2),
	TTL2c = nkbase_cmds:find_ttl({D, C, K2}, Meta),
	true = TTL2c > 0 andalso TTL2c < 0.1,
	{ok, ESeq3b} = nkbase_sc:kput(D, C, K2, data3b, Meta#{eseq=>ESeq3a}),
	{ok, ESeq3b, data3b} = nkbase_sc:kget(D, C, K2),
	none = nkbase_cmds:find_ttl({D, C, K2}, Meta),
	timer:sleep(200),
	{ok, ESeq3b, data3b} = nkbase_sc:kget(D, C, K2),
	ok = nkbase_sc:kdel(D, C, K2, Meta#{eseq=>ESeq3b}),
	{error, not_found} = nkbase_sc:kget(D, C, K2),

	%% Long ttl
	K3 = {k, 3},
	{ok, ESeq4} = nkbase_sc:kput(D, C, K3, data4, Meta#{ttl=>2.1}),
	{ok, ESeq4, data4} = nkbase_sc:kget(D, C, K3),
	timer:sleep(2500),
	{error, not_found} = nkbase_sc:kget(D, C, K3),
	ok.


search() ->
	D = {?MODULE, bulk},
	C1 = {c, 1},
	Meta = #{backend=>leveldb},

	{ok, L1} = nkbase:search(D, C1, [{i1, all}], Meta),
	true = length(L1) == ?BULK_KEYS,

	{ok, L2} = nkbase:search(D, C1, [{i1, {ge, 50}}], Meta#{get_fields=>[f1]}),
	true = length(L2) > 1 andalso length(L2) < ?BULK_KEYS,
	lists:foreach(
		fun({V, K, [#{fields:=#{f1:=V}}]}) ->
			{ok, _, #{f1:=V}} = nkbase_sc:kget(D, C1, K)
		end,
		L2).


bulk_delete() ->
	D = {?MODULE, bulk},
	Meta = #{backend=>leveldb, filter_deleted=>true},
	?debugMsg("Deleting SC records..."),
	nkbase:remove_all(D, Meta#{timeout=>60}),
	?debugMsg("... ok"),
	lists:foreach(
		fun(C) ->
			{ok, []} = nkbase:list_keys(D, {c, C}, Meta),
			{ok, []} = nkbase:search(D, {c, C}, [{i1, all}], Meta)
		end,
		lists:seq(1, ?BULK_CLASSES)).



