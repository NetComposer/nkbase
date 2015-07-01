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

-module(dmap_test).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').

-compile([export_all]).
-include_lib("eunit/include/eunit.hrl").
-include("nkbase.hrl").


dmap_test_() ->
  	{setup, spawn, 
    	fun() -> 
    		% Check every 1 secs
    		application:set_env(nkbase, expire_check, 1, [persistent]),
    		% Expiration timers resolution to 1 msec
    		application:set_env(nkbase, expire_resolution, 1, [persistent]),
    		test_util:start(),
    		{#{backend=>ets}, #{backend=>leveldb}}
		end,
		fun(_) -> 
			test_util:stop() end,
	    fun({Meta1, Meta2}) ->
		    [
				fun() -> update(Meta1) end,
				fun() -> ttl_del(Meta1) end,
				fun() -> search(Meta1) end,
				fun() -> update(Meta2) end,
				fun() -> ttl_del(Meta2) end,
				fun() -> search(Meta2) end
			]
		end
  	}.


all() ->
	application:set_env(nkbase, expire_check, 1, [persistent]),
	application:set_env(nkbase, expire_resolution, 1, [persistent]),
	ok = nkbase_cmds:cmd_all(update_config, #{}),
	Meta = #{backend=>ets},
	update(Meta),
	ttl_del(Meta),
	search(Meta).



update(Meta) ->
	?debugMsg("Update test starting"),
	D = dmap_test,
	C = dmap_test,
	K = update,

	ok = nkbase:put(D, C, K, other, Meta#{reconcile=>lww}),
	{error, {invalid_dmap, _}} = nkbase_dmap:update(D, C, K, [{f1, {assign, a}}], Meta),
	ok = nkbase:del(D, C, K, Meta#{reconcile=>lww, ttl=>0}),
	timer:sleep(10),

	% If we try to remove a flag without precondition context, we can get an error
	ok = nkbase_dmap:update(D, C, K, [{f1, {assign, a}}], Meta),
	% 'f1' is a record, not a flag
	{error, {field_not_present, {f1, _}}} = 
		nkbase_dmap:update(D, C, K, [{f1, remove_flag}], Meta),

	% We get the precondition context (like saying, yeah, I know)
	{ok, Values1} = nkbase_dmap:get(D, C, K, Meta),
	#{'_context':=DtCtx1, f1:={register, a}} = Values1,
	ok = nkbase_dmap:update(D, C, K, [{'_context', DtCtx1}, {f1, remove_flag}], Meta),
	{ok, Values1} = nkbase_dmap:get(D, C, K, Meta),

	% We can also do a full get
	{ok, _, DMap1} = nkbase:get(D, C, K, Meta),
	Values1 = nkbase_dmap:values(DMap1),

	% We do several standard gets, generating conflicts
	ok = nkbase:put(D, C, K, DMap1, Meta),
	{ok, DtMap1} = riak_dt_map:from_binary(element(2, DMap1)),
	Upd2 = nkbase_dmap:get_updates([{f2, increment}, {f3, {add, t1}}]),
	{ok, DtMap2} = riak_dt_map:update(Upd2, actor1, DtMap1),
	DMap2 = {'$nkmap', riak_dt_map:to_binary(DtMap2)},
	Upd3 = nkbase_dmap:get_updates([{f3, {add, t2}}]),
	{ok, DtMap3} = riak_dt_map:update(Upd3, actor2, DtMap1),
	DMap3 = {'$nkmap', riak_dt_map:to_binary(DtMap3)},
	ok = nkbase:put(D, C, K, DMap2, Meta),
	ok = nkbase:put(D, C, K, DMap3, Meta),
	ok = nkbase:del(D, C, K, Meta),
	{multi, _, List1} = nkbase:get(D, C, K, Meta),
	['$nkdeleted', {'$nkmap', _}, {'$nkmap', _}, {'$nkmap', _}, {'$nkmap', _}] = List1,

	% We do a update with the right reconcile, and all changes are merged
	Upd4 = [{f2, increment}, {f3, {add, t3}}],
	ok = nkbase_dmap:update(D, C, K, Upd4, Meta),
	{ok, _, DMap4} = nkbase:get(D, C, K, Meta),
	#{
  		f1 := {register, a},
  		f2 := {counter, 2},
  		f3 := {set, [t1,t2,t3]}
  	} = nkbase_dmap:values(DMap4),
	ok = nkbase:del(D, C, K, Meta#{reconcile=>lww, ttl=>0}).


ttl_del(Meta) ->
	?debugMsg("TTL test starting"),
	D = dmap_test,
	C = dmap_test,
	K = ttl,

	ok = nkbase:del(D, C, K, Meta#{ttl=>0}),
	ok = nkbase_dmap:update(D, C, K, [{f1, {add, a}}], Meta#{ttl=>5}),
	TTL1 = nkbase_cmds:find_ttl({D, C, K}, Meta),
	true = TTL1 < 5 andalso TTL1 > 4.9,

	% DMAP updates also updated the TTL
	% Secondary vnodes has the right context, so they update the object without reconcile
	ok = nkbase_dmap:update(D, C, K, [{f1, {add, b}}], Meta#{ttl=>0.2}),
	TTL2 = nkbase_cmds:find_ttl({D, C, K}, Meta),
	true = TTL2 < 0.2 andalso TTL2 > 0.1,
	{ok, #{f1:={set, [a,b]}}} = nkbase_dmap:get(D, C, K, Meta),
	timer:sleep(300),
	{error, not_found} = nkbase_dmap:get(D, C, K, Meta),

	Up3 = [
		{f1, enable}, {f2, {assign, hi}}, {f3, {add, s1}}, {f4, increment},
		{f5, [{f5a, {assign, f5a}}]}
	],
	ok = nkbase_dmap:update(D, C, K, Up3, Meta),
	{ok, 
		#{
	      f1 := {flag,enabled},
	      f2 := {register,hi},
	      f3 := {set,[s1]},
	      f4 := {counter,1},
	      f5 := {map,#{f5a := {register,f5a}}}
	    }
    } = nkbase_dmap:get(D, C, K, Meta),
    ok = nkbase_dmap:del(D, C, K, Meta#{ttl=>0.5}),
    {ok, Values1} = nkbase_dmap:get(D, C, K, Meta),
    ['_context'] = maps:keys(Values1),
    timer:sleep(600),
	{error, not_found} = nkbase_dmap:get(D, C, K, Meta).


	
search(Meta) ->
	?debugMsg("Search test starting"),
	D = dmap_test,
	C = dmap_test,
	ok = nkbase:register_class(D, C, 
		Meta#{
			indices => [
				{i_flag, {field, f_flag}},
				{i_reg, {field, f_reg}},
				{i_counter, {field, f_counter}},
				{i_set, {field, f_set}},
				{i_map, {field, {f_map, f_map_reg}}}
			]
		}),

	ok = nkbase:remove_all(D, C, Meta),
	
	Up1 = [
		{f_flag, enable}, 
		{f_reg, {assign, "reg1"}}, 
		{f_counter, {increment, 10}},
		{f_set, {add_all, [{a,1}, {b,1}]}}, 
		{f_map, [{f_map_reg, {assign, "1-reg1"}}]}
	],
	ok = nkbase_dmap:update(D, C, 1, Up1, Meta),
	{ok, Value1} = nkbase_dmap:get(D, C, 1, Meta),
 	#{
 		f_counter := {counter,10},
      	f_flag := {flag, enabled},
      	f_map := {map, #{f_map_reg := {register, "1-reg1"}}},
      	f_reg := {register, "reg1"},
      	f_set := {set, [{a,1},{b,1}]}
    } = Value1,
	{ok, Value2} = nkbase_dmap:get(D, C, 1, Meta#{get_fields=>[f_flag, {f_map, f_map_reg}]}),
	true = #{fields => #{f_flag => enabled, {f_map, f_map_reg} => "1-reg1"}} == Value2,
	{ok, Value3} = nkbase_dmap:get(D, C, 1, Meta#{get_indices=>[i_counter, i_set]}),
	true = #{indices => #{i_counter => [10],i_set => [{a,1},{b,1}]}} == Value3,

	{ok, [{10, 1, []}]} = nkbase:search(D, C, [{i_counter, {le, 10}}], Meta),
	{ok, [{{a,1}, 1, []}]} = nkbase:search(D, C, [{i_set, {eq, {a,1}}}], Meta),
	{ok, [{{a,1}, 1, [DMap1]}]} = nkbase:search(D, C, [{i_set, {eq, {a,1}}}], 
												Meta#{get_values=>true}),
	#{f_counter:={counter, 10}} = nkbase_dmap:values(DMap1),
	{ok, [{{a,1}, 1, [Value4]}]} = nkbase:search(D, C, [{i_set, {eq, {a,1}}}], 
												Meta#{get_indices=>[i_counter]}),
	true = #{indices => #{i_counter => [10]}} == Value4,

	ok = nkbase_dmap:update(D, C, 1, Up1, Meta),
	{ok, Value5} = nkbase_dmap:get(D, C, 1, Meta),
	true = Value5 == Value1#{f_counter=>{counter,20}},
	{ok, []} = nkbase:search(D, C, [{i_counter, {le, 10}}], Meta),
	{ok, [{20, 1, []}]} = nkbase:search(D, C, [{i_counter, {le, 20}}], Meta),
	ok = nkbase:del(D, C, 1, Meta#{reconcile=>lww, ttl=>0}).



	