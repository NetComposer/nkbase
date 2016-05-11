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

-module(basic_test).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').

-compile([export_all]).
-include_lib("eunit/include/eunit.hrl").
-include("nkbase.hrl").

-define(BULK_DOMAINS, 5).
-define(BULK_CLASSES, 10).
-define(BULK_KEYS, 10).


basic_test_() ->
  	{setup, spawn, 
    	fun() -> 
    		% Check every 1 secs
    		application:set_env(nkbase, expire_check, 1, [persistent]),
    		% Expiration timers resolution to 1 msec
    		application:set_env(nkbase, expire_resolution, 1, [persistent]),
    		test_util:start(),
    		{#{backend=>ets}, #{backend=>leveldb}}
		end,
		fun(_) -> test_util:stop() end,
	    fun({Meta1, Meta2}) ->
		    [
				fun() -> classes() end,
				fun() -> bulk_insert(Meta1) end,
				fun() -> get_spec(Meta1) end,
				fun() -> scan_domains(Meta1) end,
				fun() -> scan_classes(Meta1) end,
				fun() -> scan_keys(Meta1) end,
				fun() -> put(Meta1) end,
				{timeout, 60, fun() -> ttl(Meta1) end},
				fun() -> bulk_delete(Meta1) end,
				fun() -> bulk_insert(Meta2) end,
				fun() -> get_spec(Meta2) end,
				fun() -> scan_domains(Meta2) end,
				fun() -> scan_classes(Meta2) end,
				fun() -> scan_keys(Meta2) end,
				fun() -> put(Meta2) end,
				{timeout, 60, fun() -> ttl(Meta2) end},
				fun() -> bulk_delete(Meta2) end
			]
		end
  	}.


all() ->
 	application:set_env(nkbase, expire_check, 1, [persistent]),
	application:set_env(nkbase, expire_resolution, 1, [persistent]),
	ok = nkbase_cmds:cmd_all(update_config, #{}),
	classes(),
	Meta = #{backend=>ets},
	bulk_insert(Meta),
	get_spec(Meta),
	scan_domains(Meta),
	scan_classes(Meta),
	scan_keys(Meta),
	put(Meta),
	ttl(Meta),
	bulk_delete(Meta).


classes() ->
	?debugMsg("Starting classes test"),
	Ref = erlang:phash2(make_ref()),
	D = {?MODULE, Ref},
	0 = map_size(nkbase:get_class(D, class1)),
	[] = nkbase:get_classes(D),
	
	% No version, it is always updated
	ok = nkbase:register_class(D, class1, #{test=>1}),
	#{test:=1} = nkbase:get_class(D, class1),
	ok = nkbase:register_class(D, class1, #{test=>2}),
	#{test:=2} = nkbase:get_class(D, class1),
	[class1] = nkbase:get_classes(D), 
	true = lists:member({D, class1}, nkbase:get_classes()),

	% Using a version
	ok = nkbase:register_class(D, class1, #{vsn=>1, test=>3}),
	not_updated = nkbase:register_class(D, class1, #{vsn=>0, test=>4}),
	not_updated = nkbase:register_class(D, class1, #{vsn=>1, test=>4}),
	#{test:=3} = nkbase:get_class(D, class1),

	% Using an alias
	ok = nkbase:register_class(D, class2, #{vsn=>1, alias=>class1, test=>5}),
	#{vsn:=1, alias:=class1, test:=3} = nkbase:get_class(D, class2),
	not_updated = nkbase:register_class(D, class2, #{vsn=>0, alias=>class1}),

	% Using an alias to another domain
	D2 = {dom_test2, {alias, Ref}},
	ok = nkbase:register_class(D2, class3, #{vsn=>1, alias=>class1, test=>6}),
	#{vsn:=1, alias:=class1} = Class6 = nkbase:get_class(D2, class3),
	false = maps:is_key(test, Class6),	% Thereis no aliased class, so it is empty
	ok = nkbase:register_class(D2, class3, #{vsn=>2, alias=>{D,class1}, test=>7}),
	#{vsn:=2, alias:={D, class1}, test:=3} = nkbase:get_class(D2, class3),

	[class1, class2] = lists:sort(nkbase:get_classes(D)),
	[class3] = lists:sort(nkbase:get_classes(D2)),
	true = lists:member({D, class1}, nkbase:get_classes()),
	true = lists:member({D, class2}, nkbase:get_classes()),
	true = lists:member({D2, class3}, nkbase:get_classes()),

	% Remove the class
	nkbase:unregister_class(D, class1),
	nkbase:unregister_class(D, class2),
	nkbase:unregister_class(D2, class3),
	[] = nkbase:get_classes(D), 
	false = lists:member({D, class1}, nkbase:get_classes()),
	false = lists:member({D, class2}, nkbase:get_classes()),
	false = lists:member({D2, class3}, nkbase:get_classes()),
	ok.


bulk_insert(Meta) ->
	lists:foreach(
		fun(D) -> nkbase:remove_all({?MODULE, D}, Meta) end,
		lists:seq(1, ?BULK_DOMAINS)),
	Start = nklib_util:l_timestamp(),
	?debugFmt("Inserting ~p records in ~p...", 
			 [?BULK_DOMAINS*?BULK_CLASSES*?BULK_KEYS, 
			   maps:get(backend, Meta, ets)]),
	Domains = lists:seq(1, ?BULK_DOMAINS),
	Classes = lists:seq(1, ?BULK_CLASSES),
	Keys = lists:seq(1, ?BULK_KEYS),
	lists:foreach(
		fun(Domain) ->
			lists:foreach(
				fun(Class) ->
					lists:foreach(
						fun(Key) ->
							bulk_insert({?MODULE, Domain}, 
										{c, Class}, {k, Key}, Meta)
						end,
						Keys)
				end,
				Classes)
		end,
		Domains),
	Diff = nklib_util:l_timestamp()-Start,
	?debugFmt("done (~p secs)\n", [Diff/1000000]).


bulk_insert(Domain, Class, Key, Meta) ->
	Obj = #{
		k => Key, 
		f1 => crypto:rand_uniform(1, 100)
	},
	Meta1 = Meta#{
		reconcile => lww,
		indices => [{ik, key}, {i1, {field, f1}}, {{'$g', i2}, {field, f1}}]
	},
	ok = nkbase:put(Domain, Class, Key, Obj, Meta1).


get_spec(Meta) ->
	D = {?MODULE, 1},
	{ok, _, Obj} = nkbase:get(D, {c,1}, {k,1}, Meta),
	#{f1:=F1, k:={k,1}} = Obj,
	{ok, _, Map1} = nkbase:get(D, {c,1}, {k,1}, Meta#{get_fields=>[f1, f2]}),
	#{fields:=#{f1:=F1, f2:='$undefined'}} = Map1,
	{ok, _, Map2} = nkbase:get(D, {c,1}, {k,1}, Meta#{get_indices=>[i1, i2]}),
	#{indices:=#{i1:=[F1], i2:=[]}} = Map2,
	{ok, _, Map3} = nkbase:get(D, {c,1}, {k,1}, Meta#{get_fields=>[f1], get_indices=>[i1]}),
	#{fields:=#{f1:=F1}, indices:=#{i1:=[F1]}} = Map3,

	% Insert two conflicting objects
	Meta1 = Meta#{indices => [{ik, key}, {i1, {field, f1}}]},
	Obj1 = Obj#{k:=0, f1:=-1},
	ok = nkbase:put(D, {c,1}, {k,1}, Obj1, Meta1),
	ok = nkbase:del(D, {c,1}, {k,1}, Meta),
	{multi, _, [_, _, _]} = nkbase:get(D, {c,1}, {k,1}, Meta),
	{multi, _, Maps4} = 
		nkbase:get(D, {c,1}, {k,1}, Meta#{get_fields=>[f1], get_indices=>[i1]}),
	[
		'$nkdeleted',
 		#{fields:=#{f1:=-1}, indices:=#{i1:=[-1]}},
 		#{fields:=#{f1:=F1}, indices:=#{i1:=[F1]}}
 	] = lists:sort(Maps4),

	% Restore
	bulk_insert(D, {c,1}, {k,1}, Meta).



scan_domains(Meta) ->
	{ok, List1} = nkbase:list_domains(Meta),
	lists:foreach(
		fun(D) -> true = lists:member({?MODULE, D}, List1) end,
		lists:seq(1, ?BULK_DOMAINS)).


scan_classes(Meta) ->
	{ok, List1} = nkbase:list_classes({?MODULE, 1}, Meta),
	List1 = [{c, C} || C <- lists:seq(1,?BULK_CLASSES)],

	{ok, List2} = nkbase:list_classes({?MODULE, 1}, Meta),
	List2 = [{c, C} || C <- lists:seq(1, ?BULK_CLASSES)],
	ok.


scan_keys(Meta) ->
	?debugFmt("Starting scan test (~p)", [maps:get(backend, Meta, ets)]),
	D = {?MODULE, 1},
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

	D5 = crypto:rand_uniform(1, ?BULK_DOMAINS+1),
	C5 = crypto:rand_uniform(1, ?BULK_CLASSES+1),
	Fun5a = fun(K, [#{k:=K}], Acc) -> Acc+1 end,
	Fun5b = fun(C, Acc) -> C+Acc end,
	{ok, ?BULK_KEYS} = 
		nkbase:iter_objs({?MODULE,D5}, {c,C5}, Meta, Fun5a, 0, Fun5b, 0).


put(Meta) ->
	?debugFmt("Starting put test (~p)", [maps:get(backend, Meta, ets)]),
	D1 = {?MODULE, put_1},
	C1 = {c, 1},
	K1 = {k, 1},
	{ok, []} = nkbase:list_classes(D1, Meta),
	{ok, []} = nkbase:list_keys(D1, C1, Meta),
	ok = nkbase:put(D1, C1, K1, object_1, Meta),
	{ok, _L2} = nkbase:list_domains(Meta),
	{ok, [C1]} = nkbase:list_classes(D1, Meta),
	{ok, [K1]} = nkbase:list_keys(D1, C1, Meta),

	{ok, _Ctx1, object_1} = nkbase:get(D1, C1, K1, Meta),
	
	% Rewrite then object without context -> conflict
	ok = nkbase:put(D1, C1, K1, object_2, Meta),
	{multi, Ctx2, [object_2, object_1]} = nkbase:get(D1, C1, K1, Meta),

	% We use the context to rewrite a new version
	ok = nkbase:put(D1, C1, K1, object_3, Meta#{ctx=>Ctx2}),
	{ok, _Ctx3, object_3} = nkbase:get(D1, C1, K1, Meta),

	% we could also force a reconcile method of 'last wins'
	ok = nkbase:put(D1, C1, K1, object_4, Meta#{reconcile=>lww}),
	{ok, _, object_4} = nkbase:get(D1, C1, K1, Meta),

	% or a function resolution
	Fun = fun(_ExtKey, List) -> {ok, hd(lists:reverse(lists:keysort(2, List)))} end,
	ok = nkbase:put(D1, C1, K1, object_0, Meta#{reconcile=>Fun}),
	{ok, _, object_4} = nkbase:get(D1, C1, K1, Meta),

	% we could also resolve the conflict on read
	ok = nkbase:put(D1, C1, K1, object_5, Meta),
	{multi, _, [object_4, object_5]} = nkbase:get(D1, C1, K1, Meta),
	lager:notice("Next message about a read repair is expected"),
	{ok, _, object_5} = nkbase:get(D1, C1, K1, Meta#{reconcile=>Fun}),
	% we have already read-repaired:
	timer:sleep(100),
	{ok, _Ctx5, object_5} = nkbase:get(D1, C1, K1, Meta),

	% Pre-write fun
	C2 = {c, 2},
	ok = nkbase:put(D1, C2, K1, #{id=>1}, Meta),
	{ok, Ctx6, #{id:=1}} = nkbase:get(D1, C2, K1, Meta),
	PreFun = fun(FExtKey, {_ObjMeta, #{id:=Id}}, FMeta) ->
		FExtKey = {D1, C1, K1},
		Obj1 = #{id=>Id+1},
		ExtObj1 = nkbase:make_ext_obj(FExtKey, Obj1, Meta),
		{{D1, C2, K1}, ExtObj1, FMeta#{ctx=>Ctx6}}
	end,
	Self = self(),
	Ref = make_ref(),
	PostFun = fun({FD1, {c,2}, FK1}, {_, #{id:=3}}, #{ctx:=FCtx}) ->
		FD1 = D1,
		FK1 = K1,
		FCtx = Ctx6,
		Self ! Ref
	end,
	% The PreFun has updated the context, otherwise a conflict would occur,
	% since it is not included in the put call
	ok = nkbase:put(D1, C1, K1, #{id=>2}, 
					 Meta#{pre_write_hook=>PreFun, post_write_hook=>PostFun}),
	{ok, _, object_5} = nkbase:get(D1, C1, K1, Meta),
	{ok, _, #{id:=3}} = nkbase:get(D1, {c,2}, K1, Meta),
	receive Ref -> ok after 2000 -> error(post_fun) end,
	nkbase:del(D1, C1, K1, Meta#{reconcile=>lww, ttl=>0}),
	nkbase:del(D1, C2, K1, Meta#{reconcile=>lww, ttl=>0}),
	ok.


ttl(Meta) ->
	?debugFmt("Starting TTL test (~p)", [maps:get(backend, Meta, ets)]),
	D = {?MODULE, ttl},
	C = {c, 1},

	K1 = {k, 1},
	{ok, DS1} = nkbase:list_domains(Meta),
	false = lists:member(D, DS1),
	ok = nkbase:put(D, C, K1, data1, Meta#{ttl=>0.5}),
	{ok, DS2} = nkbase:list_domains(Meta),
	true = lists:member(D, DS2),
	{ok, _, data1} = nkbase:get(D, C, K1, Meta),
	TTL1 = nkbase_cmds:find_ttl({D, C, K1}, Meta),
	true = TTL1 > 0.4 andalso TTL1 < 0.5,

	timer:sleep(500),
	{error, not_found} = nkbase:get(D, C, K1, Meta),
	none = nkbase_cmds:find_ttl({D, C, K1}, Meta),
	{ok, DS3} = nkbase:list_domains(Meta),
	false = lists:member(D, DS3),

	%% Updating the object with a new TTL
	K2 = {k, 2},
	ok = nkbase:put(D, C, K2, data2a, Meta#{ttl=>0.1}),
	{ok, Ctx2, data2a} = nkbase:get(D, C, K2, Meta),
	TTL2 = nkbase_cmds:find_ttl({D, C, K2}, Meta),
	true = TTL2 > 0 andalso TTL2 < 0.1,
	ok = nkbase:put(D, C, K2, data2b, Meta#{ctx=>Ctx2, ttl=>0.3}),
	{ok, _, data2b} = nkbase:get(D, C, K2, Meta),
	TTL2b = nkbase_cmds:find_ttl({D, C, K2}, Meta),
	true = TTL2b > 0.2 andalso TTL2b < 0.3,
	timer:sleep(150),
	{ok, _, data2b} = nkbase:get(D, C, K2, Meta),
	timer:sleep(250),
	{error, not_found} = nkbase:get(D, C, K2, Meta),

	%% Removing the TTL
	ok = nkbase:put(D, C, K2, data2a, Meta#{ttl=>0.1}),
	{ok, Ctx2c, data2a} = nkbase:get(D, C, K2, Meta),
	TTL2c = nkbase_cmds:find_ttl({D, C, K2}, Meta),
	true = TTL2c > 0 andalso TTL2c < 0.1,
	ok = nkbase:put(D, C, K2, data2b, Meta#{ctx=>Ctx2c}),
	{ok, _, data2b} = nkbase:get(D, C, K2, Meta),
	none = nkbase_cmds:find_ttl({D, C, K2}, Meta),
	timer:sleep(200),
	{ok, Ctx2d, data2b} = nkbase:get(D, C, K2, Meta),
	ok = nkbase:del(D, C, K2, Meta#{ctx=>Ctx2d, ttl=>0.1}),
	timer:sleep(100),
	{error, not_found} = nkbase:get(D, C, K2, Meta),

	%% Several conflicting objects
	K3 = {k, 3},
	ok = nkbase:put(D, C, K3, data3a, Meta#{ttl=>0.1}),
	TTL3a = nkbase_cmds:find_ttl({D, C, K3}, Meta),
	true = TTL3a > 0 andalso TTL3a < 0.1,
	ok = nkbase:put(D, C, K3, data3b, Meta#{ttl=>0.3}),
	{multi, _, Data3} = nkbase:get(D, C, K3, Meta),
	[data3a, data3b] = lists:sort(Data3),
	TTL3b = nkbase_cmds:find_ttl({D, C, K3}, Meta),
	true = TTL3b > 0.2 andalso TTL3b < 0.3,
	ok = nkbase:put(D, C, K3, data3c, Meta#{ttl=>0.2}),
	{multi, _, [data3c, data3b, data3a]} = nkbase:get(D, C, K3, Meta),
	TTL3c = nkbase_cmds:find_ttl({D, C, K3}, Meta),
	true = TTL3c > 0.2 andalso TTL3c < 0.3,
	timer:sleep(200),
	{multi, _, [data3c, data3b, data3a]} = nkbase:get(D, C, K3, Meta),
	timer:sleep(100),
	{error, not_found} = nkbase:get(D, C, K3, Meta),
	
	ok = nkbase:put(D, C, K3, data3a, Meta#{ttl=>0.1}),
	TTL3d = nkbase_cmds:find_ttl({D, C, K3}, Meta),
	true = TTL3d > 0 andalso TTL3d < 0.1,
	ok = nkbase:put(D, C, K3, data3b, Meta),
	{multi, _, [data3b, data3a]} = nkbase:get(D, C, K3, Meta),
	none = nkbase_cmds:find_ttl({D, C, K3}, Meta),
	timer:sleep(100),
	{multi, Ctx3d, [data3b, data3a]} = nkbase:get(D, C, K3, Meta),
	ok = nkbase:del(D, C, K3, Meta#{ctx=>Ctx3d, ttl=>0.1}),
	timer:sleep(100),
	{error, not_found} = nkbase:get(D, C, K3, Meta),
	ok = nkbase:put(D, C, K3, data3a, Meta),
	none = nkbase_cmds:find_ttl({D, C, K3}, Meta),
	ok = nkbase:put(D, C, K3, data3b, Meta#{ttl=>0.1}),
	{multi, Ctx3e, [data3b, data3a]} = nkbase:get(D, C, K3, Meta),
	none = nkbase_cmds:find_ttl({D, C, K3}, Meta),
	ok = nkbase:del(D, C, K3, Meta#{ctx=>Ctx3e, ttl=>0.1}),
	timer:sleep(100),
	{error, not_found} = nkbase:get(D, C, K3, Meta),

	%% Long ttl
	%% we set a timer where the time (at vnode, a bit < 2.1) is greater
	%% (using div, not /) than 1 (the current long check time), so it is 
	%% delayed (raise the log level line 794 at nkbase_vnode to check)
	ok = nkbase:put(D, C, K3, data4a, Meta#{ttl=>2.1}),
	{ok, _, data4a} = nkbase:get(D, C, K3, Meta),
	timer:sleep(2500),
	{error, not_found} = nkbase:get(D, C, K3, Meta),
	ok.

bulk_delete(Meta) ->
	lists:foreach(
		fun(D) -> nkbase:remove_all({?MODULE, D}, Meta) end,
		lists:seq(1, ?BULK_DOMAINS)),
	{ok, Domains} = nkbase:list_domains(Meta),
	lists:foreach(
		fun(Domain) ->
			case Domain of
				{?MODULE, _} -> error(not_deleted);
				_ -> ok
			end
		end,
		Domains).





