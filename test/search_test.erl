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

-module(search_test).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').

-compile([export_all]).
-include_lib("eunit/include/eunit.hrl").
-include("nkbase.hrl").

-define(RECORDS, 10000).


search_test_() ->
  	{setup, spawn, 
    	fun() -> 
    		test_util:start(),
    		{#{backend=>ets}, #{backend=>leveldb}}
		end,
		fun(_) -> test_util:stop() end,
	    fun({Meta1, Meta2}) ->
		    [
				{timeout, 60, fun() -> bulk_insert(Meta1) end},
				{timeout, 60, fun() -> ranges(Meta1) end},
				{timeout, 60, fun() -> text(Meta1) end},
				{timeout, 60, fun() -> multi_index(Meta1) end},
				{timeout, 60, fun() -> pagination(Meta1) end},
				{timeout, 60, fun() -> conflict_delete(Meta1) end},
				{timeout, 60, fun() -> reindex(Meta1) end},
				{timeout, 60, fun() -> bulk_delete(Meta1) end},
				{timeout, 60, fun() -> bulk_insert(Meta2) end},
				{timeout, 60, fun() -> ranges(Meta2) end},
				{timeout, 60, fun() -> text(Meta2) end},
				{timeout, 60, fun() -> multi_index(Meta2) end},
				{timeout, 60, fun() -> pagination(Meta2) end},
				{timeout, 60, fun() -> conflict_delete(Meta2) end},
				{timeout, 60, fun() -> reindex(Meta2) end},
				{timeout, 60, fun() -> bulk_delete(Meta2) end}
			]
		end
  	}.


all(Backend) ->
	Meta = #{backend=>Backend},
	bulk_insert(Meta),
	ranges(Meta),
	text(Meta),
	multi_index(Meta),
	pagination(Meta),
	conflict_delete(Meta),
	reindex(Meta),
	bulk_delete(Meta),
	ok.
	
t() ->
	nkbase:register_class(tutorial, test2, #{
		backend => ets,
		indices => [
			{i_key, key},
			{i_name, {field, name}},
			{i_surname, {field, surname}},
			{i_names, {field, fullname}, [normalize, words]}
		],
		reconcile => lww
	  }).

t1() ->
lists:foreach(
	fun(Pos) ->
		{Name1, Name2, Name3} = test_util:get_name(),
		Obj = #{
			name => Name1,
			surname => list_to_binary([Name2, " ", Name3]),
			fullname => list_to_binary([Name1, " ", Name2, " ", Name3])
		},
		ok = nkbase:put(tutorial, test2, {k, Pos}, Obj)
	end,
	lists:seq(1, 1000)).


bulk_insert(Meta) ->
	nkbase:register_class(search_test, search_test, 
		#{
			n => 3,
			page_size => 10*?RECORDS,
			indices => [
				{i_name, {field, name}},
				{i_surname, {field, name}},
				{i_names, {field, fullname}, [normalize, words]},
				{i_pos, {field, pos}},
				{i_10, {field, 10}},
				{i_100, {field, 100}},
				{i_1000, {field, 1000}}
			]
		}),
	nkbase:remove_all(search_test, search_test, Meta),
	Start = now(),
	?debugFmt("Inserting ~p records in ~p...", [?RECORDS, maps:get(backend, Meta, ets)]),
	lists:foreach(
		fun(Pos) ->
			{Name1, Name2, Name3} = test_util:get_name(),
			Obj = #{
				name => Name1,
				surname => list_to_binary([Name2, " ", Name3]),
				fullname => list_to_binary([Name1, " ", Name2, " ", Name3]),
				pos => Pos,
				10 => in_n(10),
				100 => in_n(100),
				1000 => in_n(1000)
			},
			Meta1 = Meta#{reconcile=>lww},
			ok = nkbase:put(search_test, search_test, {k, Pos}, Obj, Meta1)
		end,
		lists:seq(1, ?RECORDS)),
	Diff = timer:now_diff(now(), Start),
	?debugFmt("done (~p secs)", [Diff/1000000]).

	
ranges(Meta) ->
	?debugFmt("Starting ranges test (~p)", [maps:get(backend, Meta, ets)]),
	List1 = search([{i_pos, all}], Meta),
	?RECORDS = length(List1),
	[{1, {k, 1}, []}|_] = List1,
	[{?RECORDS, {k, ?RECORDS}, []}|_] = lists:reverse(List1),

	List2 = search([{i_pos, all}], Meta#{order=>desc}),
	?RECORDS = length(List2),
	[{1, {k, 1}, []}|_] = lists:reverse(List2),
	[{?RECORDS, {k, ?RECORDS}, []}|_] = List2,

	Pos1 = get_pos(),
	[{Pos1, {k, Pos1}, []}] = search([{i_pos, [{eq, Pos1}]}], Meta),

	Pos2 = ?RECORDS div 3,
	List3 = search([{i_pos, [{ge, Pos2}]}], Meta),
	?RECORDS = length(List3) + Pos2 - 1,
	[{Pos2, {k, Pos2}, []}|Rest3] = List3,
	List3 = search([{i_pos, [{range, Pos2, ?RECORDS}]}], Meta),
	Rest3 = search([{i_pos, [{gt, Pos2}]}], Meta),
	List4 = search([{i_pos, [{le, Pos2}]}], Meta),
	Pos2 = length(List4),
	[{Pos2, {k, Pos2}, []}|Rest4] = lists:reverse(List4),
	List4 = search([{i_pos, [{range, 1, Pos2}]}], Meta),
	List5 = search([{i_pos, [{lt, Pos2}]}], Meta),
	List5 = lists:reverse(Rest4),

	[
		{1, {k, 1}, []}, 
		{3, {k, 3}, []}, 
		{6, {k, 6}, []}
	] = 
		search([{i_pos, [{eq, 1}, {eq, 3}, {eq, 6}]}], Meta),
	List1 = search([{i_pos, [{le, Pos2}, {gt, Pos2}]}], Meta).

text(Meta) ->
	?debugFmt("Starting text test (~p)", [maps:get(backend, Meta, ets)]),
	List1 = search([{i_names, "e-f"}], Meta#{get_values=>true}),
	Fun1 = fun(V) -> nkbase_search:test_filter(V, {re, "^[ef].*"}) end,
	test_names(List1, Fun1),
	true = lists:keymember(<<"fernando">>, 1, List1),
	true = lists:keymember(<<"elias">>, 1, List1),

	List2 = search([{i_names, "e-f|z*"}], Meta#{get_values=>true}),
	true = length(List2) > length(List1),
	Fun2 = fun(V) -> nkbase_search:test_filter(V, {re, "^[efz].*"}) end,
	test_names(List2, Fun2),
	false = lists:keymember(<<"zapata">>, 1, List1),
	true = lists:keymember(<<"zapata">>, 1, List2),

	List3 = search([{i_names, "*z"}], Meta#{get_values=>true}),
	Fun3 = fun(V) -> nkbase_search:test_filter(V, {re, ".*z$"}) end,
	test_names(List3, Fun3),
	true = lists:keymember(<<"alvarez">>, 1, List3),

	List4 = search([{i_names, "*ab*"}], Meta#{get_values=>true}),
	List4 = search([{i_names, "re(ab)"}], Meta#{get_values=>true}),
	Fun4 = fun(V) -> nkbase_search:test_filter(V, {re, "ab"}) end,
	test_names(List4, Fun4),
	true = lists:keymember(<<"zabaleta">>, 1, List4),

	List5 = search([{i_names, "<> zabaleta"}], Meta#{get_values=>true}),
	true = length(List5) > ?RECORDS * 99 div 100,
	false = lists:keymember(<<"zabaleta">>, 1, List5).


multi_index(Meta) ->
	?debugFmt("Starting multi index test (~p)", [maps:get(backend, Meta, ets)]),
	List10 = search([{i_10, true}], Meta),
	List100 = search([{i_100, true}], Meta),
	List1000 = search([{i_1000, true}], Meta),
	true = length(List10) > length(List100),
	true = length(List100) > length(List1000),
	List1 = search([{i_names, "a*"}], Meta#{get_values=>true}),
	List2 = search([{i_names, "a*"}, {i_10, true}], Meta#{get_values=>true}),
	lists:foreach(fun({_, _, [#{10:=true}]}) -> ok end, List2),
	List3 = search([{i_names, "a*"}, {i_10, false}], Meta#{get_values=>true}),
	lists:foreach(fun({_, _, [#{10:=false}]}) -> ok end, List3),
	true = length(List1) > length(List2),
	true = length(List3) > length(List2),
	true = length(List1) == length(List2) + length(List3),
	List4 = search([{i_names, "a*"}, {i_100, true}], Meta),
	List5 = search([{i_names, "a*"}, {i_100, false}], Meta),
	true = length(List5) > length(List4),
	true = length(List2) > length(List4),
	true = length(List1) == length(List4) + length(List5).


pagination(Meta) ->
	?debugFmt("Starting pagination test (~p)", [maps:get(backend, Meta, ets)]),
	List1 = search([{i_names, "a*"}], Meta),
	Length = length(List1),
	{Iters1, List1} = search_pages([{i_names, "a*"}], Meta#{order=>asc, page_size=>10}),
	Iters1 = Length div 10,
	{Iters2, List1} = search_pages([{i_names, "a*"}], Meta#{order=>asc, page_size=>100}),
	Iters2 = Length div 100,
	{Iters3, List1} = search_pages([{i_names, "a*"}], Meta#{order=>asc, page_size=>1000}),
	Iters3 = Length div 1000,

	List2 = search([{i_names, "a*"}], Meta#{order=>desc}),
	Length = length(List2),
	{Iters4, List2} = search_pages([{i_names, "a*"}], Meta#{order=>desc, page_size=>10}),
	Iters4 = Length div 10,
	{Iters5, List2} = search_pages([{i_names, "a*"}], Meta#{order=>desc, page_size=>100}),
	Iters5 = Length div 100,
	{Iters6, List2} = search_pages([{i_names, "a*"}], Meta#{order=>desc, page_size=>1000}),
	Iters6 = Length div 1000,

	List3 = search([{i_names, [all]}], Meta),
	Total = length(List3),	% All key will show several times (for different words)
	true = Total > ?RECORDS,
	{Iters7, List3} = search_pages([{i_names, all}], Meta#{order=>asc, page_size=>1000}),
	Iters7 = Total div 1000,
	List4 = lists:reverse(List3),
	{Iters8, List4} = search_pages([{i_names, all}], Meta#{order=>desc, page_size=>1000}),
	Iters8 = Total div 1000,

	Test = maps:from_list([{{k, P}, true} || P<-lists:seq(1, ?RECORDS)]),
	test_pages(undefined, 100, Test, asc, Meta),
	test_pages(undefined, 1000, Test, asc, Meta),

	test_pages(undefined, 100, Test, desc, Meta),
	test_pages(undefined, 1000, Test, desc, Meta),
	ok.


conflict_delete(Meta) ->
	?debugFmt("Starting conflict test (~p)", [maps:get(backend, Meta, ets)]),
	Pos = crypto:rand_uniform(1, ?RECORDS+1),
	{ok, _, Obj} = nkbase:get(search_test, search_test, {k, Pos}, Meta), 
	#{name:=Name} = Obj,
	NormName = nkbase_util:normalize(Name),
	timer:sleep(100),
	[{Pos, {k, Pos}, [Map1]}] = 
		search([{i_pos, Pos}], Meta#{get_fields=>[name], get_indices=>[i_names]}),
	#{fields:=#{name:=Name}, indices:=#{i_names:=Ind1}} = Map1,
	true = lists:member(NormName, Ind1),
	List1 = search([{i_names, NormName}], Meta),
	true = lists:member({NormName, {k, Pos}, []}, List1),

	%% Add a conflicting value and a conflicting delete
	Obj1 = Obj#{fullname:= <<"aaaa bbbb cccc">>, pos:=-1},
	ok = nkbase:put(search_test, search_test, {k, Pos}, Obj1, Meta),
	{multi, _, [Obj1, Obj]} = nkbase:get(search_test, search_test, {k, Pos}, Meta),
	ok = nkbase:del(search_test, search_test, {k, Pos}, Meta),
	{multi, Ctx, ['$nkdeleted', Obj1, Obj]} = 
		nkbase:get(search_test, search_test, {k, Pos}, Meta),

	[{Pos, {k, Pos}, [Obj]}] = search([{i_pos, Pos}], Meta#{get_values=>true}),
	List1 = search([{i_names, NormName}], Meta),
	[{-1, {k, Pos}, [Obj1]}] = search([{i_pos, -1}], Meta#{get_values=>true}),
	[
		{<<"aaaa">>, {k, Pos}, []},
		{<<"bbbb">>, {k, Pos}, []},
		{<<"cccc">>, {k, Pos}, []}
	] = search([{i_names, <<"aaaa|bbbb|cccc">>}], Meta),

	%% Delete the object
	ok = nkbase:del(search_test, search_test, {k, Pos}, Meta#{ctx=>Ctx, ttl=>0}),
	[] = search([{i_pos, Pos}], Meta),
	[] = search([{i_pos, -1}], Meta),
	List2 = search([{i_names, NormName}], Meta),
	true = length(List2) < length(List1).


reindex(Meta) ->
	D = search_test,
	C = search_test,

	All = search([{i_pos, all}], Meta),
	?RECORDS = length(All) + 1,					% Last test deleted one
	[] = search([{i_pos2, {eq, 1}}], Meta),

	NewIndices = [
		{i_name, {field, name}},
		{i_surname, {field, name}},
		{i_names, {field, fullname}, [normalize, words]},
		{i_pos2, {field, pos}}
	],
	?debugFmt("Reindexing ~p records...", [?RECORDS]),
	nkbase:reindex(D, C, Meta#{indices=>NewIndices}),
	?debugMsg("... done"),

	[{1, {k, 1}, []}] = search([{i_pos2, {eq, 1}}], Meta),
	[{?RECORDS, {k, ?RECORDS}, []}] = search([{i_pos2, {eq, ?RECORDS}}], Meta),
	[] = search([{i_pos, all}], Meta),
	All = search([{i_pos2, all}], Meta),
	ok.

bulk_delete(Meta) ->
	Start = now(),
	?debugFmt("Deleting ~p records...", [?RECORDS]),
	lists:foreach(
		fun(Pos) ->
			Meta1 = Meta#{reconcile=>lww},
			ok = nkbase:del(search_test, search_test, {k, Pos}, Meta1#{ttl=>0})
		end,
		lists:seq(1, ?RECORDS)),
	Diff = timer:now_diff(now(), Start),
	?debugFmt("done (~p secs)", [Diff/1000000]),
	[] = search([{i_pos, all}], Meta),
	[] = search([{i_names, all}], Meta).







%%%%%%%%%%%%%%%%%%%%%%%%%% utilities %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

test_names([], _Fun) ->
	ok;
test_names([{V, {k, Pos}, [Obj]}|Rest], Fun) ->
	Pos = maps:get(pos, Obj),
	true = Fun(V),
	#{fullname:=FullName} = Obj,
	Names = nkbase_util:words(nkbase_util:normalize(FullName)),
	true = lists:member(V, Names),
	test_names(Rest, Fun).


search(Spec, Meta) ->
	{ok, List} = nkbase:search(search_test, search_test, Spec, Meta),
	List.


search_pages(Spec, Meta) ->
	search_pages(Spec, maps:merge(#{order=>asc, page_size=>100}, Meta), undefined, [], 0).


search_pages(Spec, Meta, Next, Acc, Iters) ->
	#{order:=Order, page_size:=PageSize} = Meta,
	List = search(Spec, Meta#{next=>Next}),
	Acc1 = case Order of
		asc -> lists:umerge(List, Acc);
		desc -> lists:umerge(lists:reverse(List), Acc)
	end,
	case length(List) == PageSize of
		true ->
			[{LastV, {k, LastK}, []}|_] = lists:reverse(List),
			% lager:warning("Next ~p, ~p", [LastV, LastK]),
			Next1 = case Order of
				asc -> {LastV, {k, LastK+1}};
				desc -> {LastV, {k, LastK-1}}
			end,
			search_pages(Spec, Meta, Next1, Acc1, Iters+1);
		false when Order==asc ->
			{Iters, Acc1};
		false when Order==desc ->
			{Iters, lists:reverse(Acc1)}
	end.


test_pages(Next, PageSize, Test, Order, Meta) ->
	List = search([{i_pos, all}], 
				  Meta#{page_size=>PageSize, next=>Next, order=>Order}),
	Test1 = lists:foldl(
		fun({_V, K, []}, Acc) -> true=maps:get(K, Acc), maps:remove(K, Acc) end,
		Test,
		tl(List)),
	case length(List) == PageSize of
		true ->
			[{LastV, LastK, []}|_] = lists:reverse(List),
			test_pages({LastV, LastK}, PageSize, Test1, Order, Meta);
		false when Order==asc ->
			[{{k,1}, true}] = maps:to_list(Test1),
			ok;
		false when Order==desc ->
			[{{k, ?RECORDS}, true}] = maps:to_list(Test1),
			ok
	end.


get_pos() ->
	crypto:rand_uniform(1, ?RECORDS+1).


in_n(N) ->
	crypto:rand_uniform(1, N-1)==1.
