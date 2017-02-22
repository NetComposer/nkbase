%% -------------------------------------------------------------------
%%
%% Copyright (c) 2017 Carlos Gonzalez Florido.  All Rights Reserved.
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

%% @private Search Module
-module(nkbase_search).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').

-export([expand_search_spec/1]).
-export([test_filter/2, any_filter/2, test_indices/2]).
-export([get_start_stop/1, get_ranges/1]).
-export_type([ext_search_spec/0]).

-type ext_search_spec() ::
	[{nkbase:index_name(), [nkbase:search_filter()]}].

-include("nkbase.hrl").


%%%===================================================================
%%% API
%%%===================================================================

%% @private
-spec expand_search_spec(nkbase:search_spec()|[nkbase:search_spec()]) ->
	ext_search_spec().

expand_search_spec(SearchSpec) when is_list(SearchSpec) ->
	[{Index, expand(Filter, [])} || {Index, Filter} <- SearchSpec];

expand_search_spec({Index, Filter}) ->
	expand_search_spec([{Index, Filter}]).


%% @private
expand([L|_]=Filter, []) when is_integer(L), L>=32, L=<127 ->
	expand(list_to_binary(Filter), []);

expand(Filter, []) when not is_list(Filter) ->
	expand([Filter], []);

expand([], Acc) ->
	lists:reverse(Acc);

expand([all|Rest], Acc) ->
	expand(Rest, [all|Acc]);

expand([{Op, Term}|Rest], Acc) 
	when Op==eq; Op==ne; Op==gt; Op==ge; Op==lt; Op==le ->
	expand(Rest, [{Op, Term}|Acc]);

expand([{range, A, B}|Rest], Acc) ->
	expand(Rest, [{range, A, B}|Acc]);

expand([[L|_]=Term|Rest], Acc) when is_integer(L), L>=32, L=<127 ->
	expand([list_to_binary(Term)|Rest], Acc);


expand([Term|Rest], Acc) when is_binary(Term) ->
	expand(Rest, expand_text(Term)++Acc);

expand([Term|Rest], Acc) ->
	expand(Rest, [{eq, Term}|Acc]).


%% @private
% expand_text(Filter) when is_list(Filter) ->
% 	expand_text(list_to_binary(Filter));

expand_text(Filter) when is_binary(Filter) ->
	Parts = binary:split(Filter, <<"|">>, [global]),
	expand_text(Parts, []).


% @private
expand_text([], Acc) ->
	Acc;

expand_text([Part|Rest], Acc) ->
	Spec = spec_pattern(),
	%% Match captures
	%% - 1: *|>=|<=|<>|>|<
	%% - 2: Not * or -
	%% - 3: * or -
	%% - 4: Not * or -
	Filter = case re:run(Part, Spec, [{capture, all_but_first, binary}]) of
		{match, [<<"*">>, <<>>, <<>>, <<>>]} -> 
			all;
		{match, [<<"*">>, <<>>, <<>>, T]} -> 
			{re, <<T/binary, "$">>};
		{match, [<<"*">>, T, <<"*">>, <<>>]} -> 
			{re, T};
		{match, [<<">=">>, <<>>, <<>>, T]} -> 
			{ge, T};
		{match, [<<">">>, <<>>, <<>>, T]} -> 
			{gt, T};
		{match, [<<"<=">>, <<>>, <<>>, T]} -> 
			{le, T};
		{match, [<<"<">>, <<>>, <<>>, T]} -> 
			{lt, T};
		{match, [<<"<>">>, <<>>, <<>>, T]} -> 
			{ne, T};
		{match, [<<>>, T, <<"*">>, <<>>]} -> 
			{range, T, <<T/binary, 255>>};
		{match, [<<>>, T1, <<"-">>, T2]} -> 
			{range, T1, <<T2/binary, 255>>};
		{match, [<<>>, <<>>, <<>>, T]} -> 
			case 
				re:run(Part, "^\\s*re\\((.*)\\)\\s*$", 
					   [{capture, all_but_first, binary}]) 
			of
				{match, [ReSpec]} -> 
					{re, ReSpec};
				nomatch ->
					{eq, T}
			end
	end,
	expand_text(Rest, [Filter|Acc]).
		

%% @private
-spec test_filter(term()|[term()], nkbase:search_filter()) ->
	boolean().

test_filter([], _Filter) ->
	false;

test_filter([Val|Rest], Filter) ->
	case test_filter(Val, Filter) of
		true -> true;
		false -> test_filter(Rest, Filter)
	end;

test_filter(_Val, all) -> true;
test_filter(Val, {eq, Val}) -> true;
test_filter(Val, {ne, Term}) when Val/=Term-> true;
test_filter(Val, {lt, Term}) when Val<Term -> true;
test_filter(Val, {le, Term}) when Val=<Term -> true;
test_filter(Val, {gt, Term}) when Val>Term -> true;
test_filter(Val, {ge, Term}) when Val>=Term -> true;
test_filter(Val, {range, Term1, Term2}) when Val>=Term1, Val=<Term2 -> true;
test_filter(Val, {re, Term}) when is_binary(Val); is_list(Val) -> 
	re:run(Val, Term, [{capture, none}])==match;
test_filter(_, _) -> false.



%% @private
-spec any_filter(term(), [nkbase:search_filter()]) ->
	boolean().

any_filter(_Val, []) ->
	false;
any_filter(Val, [Filter|Rest]) ->
	case test_filter(Val, Filter) of
		true -> true;
		false -> any_filter(Val, Rest)
	end.
	

%% @private Finds out if an object's indices comply with all specified filters 
-spec test_indices(ext_search_spec(), [{nkbase:index_name(), term()}]) ->
	boolean().

test_indices([], _Indices) -> 
	true;

test_indices([{Index, Filters}|Rest], Indices) -> 
	Values = proplists:get_all_values(Index, Indices),
	% lager:warning("Values: ~p, F: ~p", [Values, Filters]),
	case test_indices_values(Values, Filters) of
		true -> test_indices(Rest, Indices);
		false -> false
	end.


%% @private
-spec test_indices_values([term()], [nkbase:search_spec()]) ->
	boolean().

test_indices_values([], _Filters) ->
	false;

test_indices_values([Value|Rest], Filters) ->
	case any_filter(Value, Filters) of
		true -> true;
		false -> test_indices_values(Rest, Filters)
	end.


%% @private
-spec get_start_stop(nkbase:search_filter()) ->
	{term(), term()}.

get_start_stop(Filter) ->
	Start = case Filter of 
		{eq, S} -> S;
		{gt, S} -> S;
		{ge, S} -> S;
		{range, S, ST} when ST >=S -> S;
		_ -> ?ERL_LOW
	end,
	Stop = case Filter of 
		{eq, T} -> T;
		{lt, T} -> T;
		{le, T} -> T;
		{range, TS, T} when T >= TS -> T;
		_ -> ?ERL_HIGH
	end,
	{Start, Stop}.



%% @private
-spec get_ranges([nkbase:search_filter()]) ->
	{term(), term(), [{term(), term()}]}.

get_ranges([]) ->
	{?ERL_LOW, ?ERL_LOW, []};

get_ranges(Filters) when is_list(Filters) ->
	All = lists:sort([get_start_stop(Filter) || Filter <- Filters]),
	[{Start, _}|_] = All,
	Stop = lists:max([T || {_, T} <-All]),
	{Start, Stop, find_holes(Start, Stop, All, [])}.


% @private
find_holes(Point, Point, [], Acc) ->
	lists:reverse(Acc);

find_holes(Point, GStop, [], Acc) ->
	lists:reverse([{Point, GStop}|Acc]);

% find_holes(Point, Stop, [{Point, Stop}|Rest], Acc) ->
% 	find_holes(Stop, Rest, Acc);

find_holes(Point, GStop, [{Start, Stop}|Rest], Acc) when Point < Start ->
	find_holes(Stop, GStop, Rest, [{Point, Start}|Acc]);

find_holes(Point, GStop, [{_Start, Stop}|Rest], Acc) ->
	find_holes(max(Point, Stop), GStop, Rest, Acc).


%% @private See spec_pattern_test()
spec_pattern() ->
	{re_pattern,4,0,0,
        <<69,82,67,80,225,0,0,0,16,0,0,0,1,0,0,0,255,255,255,
          255,255,255,255,255,0,0,0,0,0,0,4,0,0,0,64,0,0,0,0,0,
          0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,125,0,
          157,25,85,9,140,127,0,7,0,1,29,42,113,0,7,29,62,29,61,
          113,0,7,29,60,29,61,113,0,7,29,60,29,62,113,0,5,29,62,
          113,0,5,29,60,114,0,38,85,9,127,0,39,0,2,107,255,255,
          255,255,255,219,255,255,255,255,255,255,255,255,255,
          255,255,255,255,255,255,255,255,255,255,255,255,255,
          255,255,255,255,99,114,0,39,85,9,140,127,0,7,0,3,29,
          42,113,0,5,29,45,114,0,12,85,9,127,0,39,0,4,107,255,
          255,255,255,255,219,255,255,255,255,255,255,255,255,
          255,255,255,255,255,255,255,255,255,255,255,255,255,
          255,255,255,255,255,99,114,0,39,85,9,27,114,0,157,0>>}.





%% ===================================================================
%% EUnit tests
%% ===================================================================

% -define(TEST, true).
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

spec_pattern_test() ->
	?debugFmt("Starting ~p tests", [?MODULE]),
	{ok, Patt} = re:compile(
		<<"^\\s*(\\*|>=|<=|<>|>|<)?\\s*([^\\*\\-]*?)",
		   "\\s*(\\*|\\-)?\\s*([^\\*\\-]*?)\\s*$">>),
	Patt = spec_pattern().


expand_test() ->
	[{i, [{eq, val}]}] = expand_search_spec({i, val}),
	[{i, [{ne, val}]}] = expand_search_spec({i, [{ne, val}]}),
	[
		{i1, [{ne, val1}, {gt, val2}]},
 		{i2, [{range, a, b}]},
 		{i3, [{eq, c}, {ne, d}]}
 	] = 
		expand_search_spec(
	[
		{i1, [{ne, val1}, {gt, val2}]}, 
		{i2, {range, a, b}}, 
		{i3, [{eq, c}, {ne, d}]}
	]),
	[{i, [{eq, <<"hi">>}]}] = expand_search_spec({i, "hi"}),
	[{i, [{eq, <<"hi1">>}, {eq, <<"hi2">>}]}] = 
		expand_search_spec({i, <<"hi1|hi2">>}),
	[{i, [{eq, <<"hi1">>}, {eq, <<"hi2">>}, {eq, b}]}] = 
		expand_search_spec({i, [<<"hi1|hi2">>, {eq, b}]}),
	[{i, [{eq, a}, {eq, <<"hi1">>}, {eq, <<"hi2">>}, {eq, b}]}] = 
		expand_search_spec({i, [{eq, a}, "hi1|hi2", {eq, b}]}),
	[
		{i1, [{eq, <<"text">>}]},
	 	{i2, [all, {re, <<"tail$">>}, {re,<<"tail">>}]},
	 	{i3,[
	 		{ge, <<"ge">>},
	      	{gt, <<"gt">>},
	      	{le, <<"le">>},
	      	{lt, <<"lt">>}
	    ]},
	 	{i4,[
	 		{ne, <<"ne">>},
	      	{range, <<"head">>, <<"head", 255>>},
	      	{range, <<"001">>,<<"003", 255>>}
	    ]},
	 	{i5,[{re, <<"myre.^$">>}]}
	] = 
		expand_search_spec(
	[
		{i1, "  text   "},
		{i2, " * | *tail  |* tail *"},
		{i3, ">=ge|>gt| <= le| < lt"},
		{i4, "<>ne |  head * |  001 -  003  "},
		{i5, <<" re(myre.^$) ">>}
	]).


indices_test() ->
	false = any_filter(5, [{ne, 5}]),
	true = any_filter(5, [{ne, 5}, {ge, 5}]),
	Indices = [
		{i1, 5},
		{i1, 10},
		{i2, v1}
	],
	false = test_indices([{i5, [{eq, 5}]}], Indices),
	true = test_indices([{i1, [{eq, 5}]}], Indices),
	false = test_indices([{i1, [{eq, 4}, {ge, 11}]}], Indices),
	true = test_indices([{i1, [{eq, 4}, {ge, 10}]}], Indices),
	false = test_indices(
		[
			{i1, [{eq, 4}, {ge, 10}]}, 
			{i5, [{eq, 5}]}
		], Indices),
	true = test_indices(
		[
			{i1, [{eq, 4}, {ge, 10}]}, 
			{i2, [{eq, vv}, {eq, v1}]}
		], Indices),
	ok.


ranges_test() ->
	{1, 100, [
		{1, 2},
		{2, 3},
		{3, 5},
		{8, 12},
		{20, 40},
		{80, 85},
		{86, 90}
	]} =
	get_ranges([
		{eq, 1},
		{eq, 2},
		{eq, 3},
		{range, 5, 8},
		{range, 5, 6},
		{range, 6, 7},
		{range, 12, 17},
		{eq, 15},
		{range, 15, 20},
		{range, 18, 19},
		{range, 40, 80},
		{range, 85, 86},
		{range, 90, 100}
	]),
	{?ERL_LOW, ?ERL_HIGH, [{4, 6}, {6, 10}]} = 
		get_ranges([{le, 4}, {eq, 6}, {gt, 10}, {eq, 12}]).


-endif.

