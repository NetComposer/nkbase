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

-module(nkbase_util).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').

-export([normalize/1, words/1]).
-export([get_spec/2]).
-export([get_value/2, get_value/3, rflatten_max/2]).
-export([expand_indices/3, reconcile/3]).


%% ===================================================================
%% Public
%% ===================================================================


%% @doc Ensure that an application and all of its transitive
%% @doc Extracts fields and values from an ext_obj() or [ext_obj()]
-spec get_spec(nkbase:get_spec(), nkbase:ext_obj()|[nkbase:ext_obj()]) ->
	nkbase:reply().

get_spec(_Spec, []) ->
	[];

get_spec(Spec, ExtObjs) when is_list(ExtObjs) ->
	[get_spec(Spec, ExtObj) ||ExtObj <- ExtObjs];

get_spec(_Spec, {_Meta, '$nkdeleted'}) ->
	'$nkdeleted';

get_spec(#{get_values:=true}, {_Meta, Obj}) ->
	Obj;

get_spec(Spec, {Meta, Obj}) ->
	Res1 = case maps:get(get_fields, Spec, []) of
		[] -> 
			#{};
		_ when element(1, Obj)=='$nkmap' -> 
			Values = nkbase_dmap:raw_values(Obj),
			get_spec(Spec, {Meta, Values});
		FieldsSpec ->
			#{fields=>get_spec_data(FieldsSpec, Obj, false, #{})}
	end,
	Res2 = case maps:get(get_indices, Spec, []) of
		[] ->
			 Res1;
		IndicesSpec -> 
			MetaIdxs = maps:get(idx, Meta, #{}),
			Res1#{indices=>get_spec_data(IndicesSpec, MetaIdxs, true, #{})}
	end,
	Res3 = case maps:get(get_metas, Spec, []) of
		[] ->
			Res2;
		MetasSpec ->
			Res2#{metas=>maps:with([MetasSpec], Meta)}
	end,
	Res3.


%% @private
-spec get_spec_data([term()|tuple()], map()|list(), boolean(), map()) ->
	map() | [map()].

get_spec_data([], _Obj, _Multi, Acc) ->
	Acc;

get_spec_data([Field|Rest], Obj, Multi, Acc) ->
	Value = get_spec_value(Field, Obj, Multi),
	get_spec_data(Rest, Obj, Multi, maps:put(Field, Value, Acc)).


%% @private
-spec get_spec_value(term()|tuple(), map()|list(), boolean()) ->
	term() | [term()] | '$undefined'.

get_spec_value(Field, Obj, Multi) when is_map(Obj) ->
	case maps:get(Field, Obj, '$undefined') of
		'$undefined' when is_tuple(Field) ->
			case maps:get(element(1, Field), Obj, '$undefined') of
				SubObj when is_map(SubObj); is_list(SubObj) ->
					case erlang:delete_element(1, Field) of
						{SubKey} -> ok;
						SubKey -> ok
					end,
					get_spec_value(SubKey, SubObj, Multi);
				_ ->
					'$undefined'
			end;
		'$undefined' ->
			'$undefined';
		Value ->
			Value
	end;

get_spec_value(Field, Obj, false) when is_list(Obj) ->
	case get_value(Field, Obj, '$undefined') of
		'$undefined' when is_tuple(Field) ->
			case get_value(element(1, Field), Obj, '$undefined') of
				SubObj when is_map(SubObj); is_list(SubObj) ->
					case erlang:delete_element(1, Field) of
						{SubKey} -> ok;
						SubKey -> ok
					end,
					get_spec_value(SubKey, SubObj, false);
				_ ->
					'$undefined'
			end;
		'$undefined' ->
			'$undefined';
		Value ->
			Value
	end;

get_spec_value(Field, Obj, true) when is_list(Obj) ->
	case proplists:get_all_values(Field, Obj) of
		[] when is_tuple(Field) ->
			case proplists:get_all_values(element(1, Field), Obj) of
				[SubObj] when is_map(SubObj); is_list(SubObj) ->
					case erlang:delete_element(1, Field) of
						{SubKey} -> ok;
						SubKey -> ok
					end,
					get_spec_value(SubKey, SubObj, true);
				_ ->
					[]
			end;
		[] ->
			[];
		Value ->
			Value
	end;

get_spec_value(_Field, _Obj, _Multi) ->
	'$undefined'.


%% @doc Expands a list of indices into final values
-spec expand_indices([nkbase:index_spec()], nkbase:ext_key(), nkbase:obj()) ->
	[{nkbase:index_name(), term()}].

expand_indices(Indices, ExtKey, {'$nkmap', BinMap}) ->
	DMap = nkbase_dmap:raw_values({'$nkmap', BinMap}),
	expand_indices(Indices, ExtKey, DMap);

expand_indices(Indices, ExtKey, Obj) ->
	expand_indices(ExtKey, Obj, Indices, []).


%% @private
-spec expand_indices(nkbase:ext_key(), nkbase:obj(), [nkbase:index_spec()], 
				     [{term(), term()}]) ->
	[{term(), term()}].

expand_indices(_ExtKey, _Data, [], Acc) ->
	lists:usort(Acc);

expand_indices(ExtKey, Data, [{Index, Spec, Opts}|Rest], Acc) ->
	{_Domain, _Class, Key} = ExtKey,
	Values1 = case Spec of
		key ->
			Key;
		{field, Field} when is_map(Data) ->
			maps:get(Field, Data, []);
		{field, Field} when is_list(Data) ->
			get_value(Field, Data, []);
		{field, _Field} ->
			[];
		{func, Fun} ->
			Fun(ExtKey, Data);
		Term ->
			Term
	end,
	Values2 = if
		is_binary(Values1) -> 
			[Values1];
		is_list(Values1), hd(Values1) >= $0, hd(Values1) =< $z ->
			[list_to_binary(Values1)];
		is_list(Values1) ->
			Values1;
		true ->
			[Values1]
	end,
	Values3 = case lists:member(words, Opts) of
		true -> words(Values2);
		false -> Values2
	end,
	Values4 = case lists:member(normalize, Opts) of
		true -> [normalize(V) || V <- Values3];
		false -> Values3
	end,
	Indices = [{Index, V} || V<-Values4],
	expand_indices(ExtKey, Data, Rest, Acc++Indices);

expand_indices(ExtKey, Data, [{Index, Spec}|Rest], Acc) ->
	expand_indices(ExtKey, Data, [{Index, Spec, []}|Rest], Acc).


%% @doc Performs a reconcile over a serie of values
%% Function will be called with all bodies, must select one of the or create 
%% a new one
-spec reconcile(nkbase:reconcile(), nkbase:ext_key(), dvvset:clock()) ->
	{ok, dvvset:clock()} | {error, term()}.

reconcile(lww, _ExtKey, DVV) ->
	Fun = fun({Meta1, _Obj1}, {Meta2, _Obj2}) -> 
		maps:get(time, Meta1, 0) =< maps:get(time, Meta2, 0)
	end,
	{ok, dvvset:lww(Fun, DVV)};

reconcile(Fun, ExtKey, DVV) when is_function(Fun, 2) ->
	% Fun takes all values and must return one of them or a new one
	Fun1 = fun(Values) ->
		case catch Fun(ExtKey, Values) of
			{ok, {Meta, _}=ExtObj} when is_map(Meta) -> 
				ExtObj;
			{error, Error} ->
				error(Error);
			Other ->
				lager:warning("Invalid return from reconcile function: ~p", [Other]),
				error({invalid_return, Other})
		end
	end,
 	case catch dvvset:reconcile(Fun1, DVV) of
		{'EXIT', Error} -> {error, {reconcile_error, Error}};
		DVV1 -> {ok, DVV1}
	end;

reconcile(undefined, _ExtKet, DVV) ->
	{ok, DVV};

reconcile(Term, _ExtKey, _DVV) ->
	{error, {invalid_reconcile, Term}}.


%% @doc Normalizes a value into a lower-case, using only a-z, 0-9 and spaces
%% All other values are converted into these if possible (Ä->a, é->e, etc.)
%% Utf8 and Latin-1 encodings are supported
%% Unrecognized values are converted into '#'
-spec normalize(term()) ->
	binary().

normalize(List) when is_list(List) -> norm(lists:flatten(List), []);
normalize(Atom) when is_atom(Atom) -> normalize(atom_to_list(Atom));
normalize(Bin) when is_binary(Bin) -> normalize(binary_to_list(Bin));
normalize(Integer) when is_integer(Integer) -> normalize(integer_to_list(Integer));
normalize(Other) -> normalize(msg("~p", [Other])).


%% @doc Serializes any erlang term into a iolist()
-spec msg(string(), [term()]) -> 
    iolist().

msg(Msg, Vars) ->
    case catch io_lib:format(Msg, Vars) of
        {'EXIT', _} -> 
            lager:warning("MSG PARSE ERROR: ~p, ~p", [Msg, Vars]),
            "Msg parser error";
        Result -> 
            Result
    end.


%% @private
norm([], Acc) -> 
	list_to_binary(lists:reverse(string:strip(Acc)));

norm([32|T], Acc) ->
	norm(T, [32|Acc]);

norm([H|T], Acc) when H >= $0, H =< $9 ->
	norm(T, [H|Acc]);

norm([H|T], Acc) when H >= $a, H =< $z ->
	norm(T, [H|Acc]);

norm([H|T], Acc) when H >= $A, H =< $Z ->
	norm(T, [H+32|Acc]);

%% UTF-8
norm([16#c3, U|T], Acc) when U >= 16#80, U =< 16#bc-> 
	L = if
		U >= 16#80, U =< 16#86 -> $a;	
		U == 16#87 -> $c;				
		U >= 16#88, U =< 16#8b -> $e;
		U >= 16#8c, U =< 16#8f -> $i;
		U == 16#90 -> $d;
		U == 16#91 -> $n;				
		U >= 16#92, U =< 16#96 -> $o;
		U == 16#98 -> $o;
		U >= 16#99, U =< 16#9c -> $u;
		U == 16#9D -> $y;
		U == 16#9F -> $b;
		U >= 16#a0, U =< 16#a6 -> $a;
		U == 16#a7 -> $c;				
		U >= 16#a8, U =< 16#ab -> $e;
		U >= 16#ac, U =< 16#af -> $i;
		U == 16#b0 -> $d;
		U == 16#b1 -> $n;				
		U >= 16#b2, U =< 16#b6 -> $o;
		U == 16#b8 -> $o;
		U >= 16#b9, U =< 16#bc -> $u;
		U == 16#bd -> $y;
		U == 16#bf -> $y;
		true -> $#
	end,
	norm(T, [L|Acc]);

%% Latin-1
%% (16#c3 is Atilde in latin-1, it could be confused as such)
norm([H|T], Acc) when H >= 16#c0 -> 
	L = if
		H >= 16#c0, H =< 16#c6 -> $a;
		H == 16#c7 -> $c;
		H >= 16#c8, H =< 16#cb -> $e;
		H >= 16#cc, H =< 16#cf -> $i;
		H == 16#d0 -> $d;
		H == 16#d1 -> $n;
		H >= 16#d2, H =< 16#d6 -> $o;
		H == 16#d8-> $o;
		H >= 16#d9, H =< 16#dc -> $u;
		H == 16#dd-> $y;
		H == 16#df-> $b;
		H >= 16#e0, H =< 16#e6 -> $a;
		H == 16#e7 -> $c;
		H >= 16#e8, H =< 16#eb -> $e;
		H >= 16#ec, H =< 16#ef -> $i;
		H == 16#f0 -> $d;
		H == 16#f1 -> $n;
		H >= 16#f2, H =< 16#f6 -> $o;
		H == 16#f8-> $o;
		H >= 16#f9, H =< 16#fc -> $u;
		H == 16#fd -> $y;
		H == 16#ff -> $y;
		true -> $#
	end,
	norm(T, [L|Acc]);

norm([_|T], Acc) ->
	norm(T, [$#|Acc]).


%% @doc Splits a string() or binary() into a list if binaries, using spaces
%% as separators.
-spec words(string()|binary()) ->
	[binary()].

words(Bin) when is_binary(Bin) ->
	[W || W <- binary:split(Bin, <<" ">>, [global]), W /= <<>>];

words([Hd|_]=List) when Hd>=$0, Hd=<$z ->
	words(list_to_binary(List));

words(List) when is_list(List) ->
	lists:flatten([words(Term) || Term <- List]);

words(Term) ->
	Term.


%% @doc Flattens and reverses a list with a maximum length
-spec rflatten_max(DeepList, integer()) ->
	{Length::integer(), Result::[term()]}
	when DeepList :: [term() | DeepList].

rflatten_max(DeepList, Max) ->
	do_rflatten_max(DeepList, {0, []}, Max).
	

%% @private 
-spec do_rflatten_max(DeepList, {integer(), [term()]}, integer()) ->
	{integer(), [term()]} 
	when DeepList :: [term() | DeepList].

do_rflatten_max(_, {Pos, Acc}, Max) when Pos>=Max ->
	{Pos, Acc};

do_rflatten_max([H|T], {Pos, Acc}, Max) when is_list(H) ->
	{Pos1, Acc1} = do_rflatten_max(H, {Pos, Acc}, Max),
	do_rflatten_max(T, {Pos1, Acc1}, Max);

do_rflatten_max([H|T], {Pos, Acc}, Max) ->
	do_rflatten_max(T, {Pos+1, [H|Acc]}, Max);

do_rflatten_max([], {Pos, Acc}, _Max) ->
	{Pos, Acc}.


%% @doc Like `proplists:get_value/2' but faster
get_value(Key, List) ->
	get_value(Key, List, undefined).

%% @doc Like `proplists:get_value/3' but faster
get_value(Key, List, Default) ->
	case lists:keyfind(Key, 1, List) of
		{_, Value} -> Value;
		_ -> Default
	end.


% %% @doc Converts a (long) Idx to a short pos
% idx2pos() ->
% 	'mochiglobal:nkbase_idx2pos':term().



% idx2pos(Idx) ->
% 	{Pos, Idx} = lists:keyfind(Idx, 2, idx2pos()),
% 	Pos.
	
% pos2idx(Num) ->	
% 	{Num, Idx} = lists:keyfind(Num, 1, idx2pos()),
% 	Idx.
	



% %%%%%%%%%%%%%%%%%%%%%%%%% Others  %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% status() ->
% 	[
% 		{stats, riak_core_stat:get_stats()},
% 		{ring_ready, riak_core_status:ringready()},
% 		{all_active_transfers, riak_core_status:all_active_transfers()},
% 		{transfers, riak_core_status:transfers()},
% 		{vnodes, riak_core_vnode_manager:all_index_pid(nkbase_vnode)}
% 	].



% print_ring_info() ->
% 	{ok, Ring} = riak_core_ring_manager:get_my_ring(),
% 	io:format("===============================================================================\n", []),
% 	io:format("UP services: ~p\n", [riak_core_node_watcher:services()]),
% 	io:format("All members: ~p\n", [riak_core_ring:all_members(Ring)]),
% 	io:format("Active members: ~p\n", [riak_core_ring:active_members(Ring)]),
% 	io:format("Ready members: ~p\n", [riak_core_ring:ready_members(Ring)]),
% 	io:format("------------------------3-- Idx2Num --------------------------------------------\n", []),
% 	io:format("~p\n", [riak_core_mochiglobal:get(nkbase_idx2pos)]),
% 	OwnersData = riak_core_ring:all_owners(Ring),
% 	Owners = [{idx2pos(Idx), Node} || {Idx, Node} <- OwnersData],
% 	io:format("--------------------------- Owners --------------------------------------------\n", []),
% 	io:format("~p\n", [Owners]),
% 	% AllIndexPid = 
% 	% 	[{idx2pos(Idx), Pid} ||
% 	% 	{Idx, Pid} <- riak_core_vnode_master:all_index_pid(nkserver_vnode)],
% 	% io:format("Cache Vnode Index Pid: ~p\n", [lists:keysort(1, AllIndexPid)]),
% 	AllVNodes = 
% 		[{Srv, idx2pos(Idx), Pid} || 
% 		{Srv, Idx, Pid} <- riak_core_vnode_manager:all_vnodes()],
% 	io:format("----------------------------- All VNodes --------------------------------------\n", []),
% 	io:format("~p\n", [lists:sort(AllVNodes)]),

% 	riak_core_console:member_status([]),
% 	riak_core_console:ring_status([]),
% 	ok.


%% ===================================================================
%% EUnit tests
%% ===================================================================

% -define(TEST, true).
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

get_test() ->
	?debugFmt("Starting ~p tests", [?MODULE]),
	Meta1 = #{idx=>[{idx1, v_idx1a}, {idx1, v_idx1b}, {idx2, v_idx2}]},
	Obj1 = #{
		field_a => va,
		field_b => #{
			field_b1 => #{field_b11 => v_b11, field_b12 => v_b12},
			field_b2 => v_b2
		},
		field_c => v_c
	},
	Spec = #{
		get_fields => [
			field_a,
			{field_b, field_b1, field_b11},
			field_d
		],
		get_indices => [idx1, idx3]
	},
	#{
		fields := #{
			field_a := va,
          	field_d := '$undefined',
          	{field_b, field_b1, field_b11} := v_b11
        },
 		indices := #{
 			idx1 := [v_idx1a, v_idx1b],
 			idx3 := []
 		}
 	} = Res1 = get_spec(Spec, {Meta1, Obj1}),
	Obj2 = [
		{field_a, va},
		{field_b, [
			{field_b1, [{field_b11, v_b11}, {field_b12, v_b12}]},
			{field_b2, v_b2}
		]},
		{field_c, v_c},
		{field_c, v_c_2}
	],
 	Res1 = get_spec(Spec, {Meta1, Obj2}).



norm_test() ->
	S1 = <<"aáàäâeéèëêiíìïîoòóöôuúùüûñçAÁÀÄÂEÉÈËÊIÍÌÏÎOÒÓÖÔUÚÙÜÛÑÇ">>,
	<<"aaaaaeeeeeiiiiiooooouuuuuncaaaaaeeeeeiiiiiooooouuuuunc">> = normalize(S1),

	S2 = <<"aáàäâeéèëêiíìïîoòóöôuúùüûñçAÁÀÄÂEÉÈËÊIÍÌÏÎOÒÓÖÔUÚÙÜÛÑÇ"/utf8>>,
	<<"aaaaaeeeeeiiiiiooooouuuuuncaaaaaeeeeeiiiiiooooouuuuunc">> = normalize(S2).

index_test() ->
	Obj1 = #{
		field1 => a,
		<<"field2">> => 1,
		field3 => "bcÁd",
		field4 => <<"eFÁg">>,
		field5 => [],
		{f,field6} => {oTher},
		field7 => [1, <<"a">>, 2, c],
		field8 => "Abc dÉf ghi"
	},
	K = {domain, class, mykey},

	[] = expand_indices([], K, Obj1),
	S1 = [
		{i01, key}, 
		{<<"i02">>, key, [normalize]}, 
		{i03, {field, none}},
		{i04, {field, field1}, [normalize]},
		{i05, {field, field1}},
		{i06, {field, field2}, [normalize]},
		{i07, {field, field2}},
		{i08, {field, <<"field2">>}, [normalize]},
		{i09, {field, <<"field2">>}},
		{i10, {field, field3}, [normalize]},
		{i11, {field, field3}},
		{i12, {field, field4}, [normalize]},
		{i13, {field, field4}},
		{i14, {field, field5}, [normalize]},
		{i15, {field, field5}},
		{i16, {field, {f,field6}}, [normalize]},
		{i17, {field, {f,field6}}},
		{i18, {field, field7}, [normalize]},
		{i19, {field, field7}},
		{i20, my_i20, [normalize]},
		{i21, my_i21},
		{i22, {func, fun test_func1/2}},
		{i23, {func, fun test_func1/2}, [normalize]},
		{i24, {func, fun test_func2/2}},
		{i25, {func, fun test_func2/2}, [normalize]},
		{i26, {field, field8}},
		{i27, {field, field8}, [normalize]},
		{i28, {field, field8}, [normalize, words]},
		{i29, ["abC   def", <<" ghI kk">>], [normalize, words]}
	],
	[
		{i01, mykey},
		{i04, <<"a">>},
		{i05, a},
		{i08, <<"1">>},
		{i09, 1},
		{i10, <<"bcad">>},
		{i11, <<"bcÁd">>},
		{i12, <<"efag">>},
		{i13, <<"eFÁg">>},
		{i16, <<"#other#">>},
		{i17, {oTher}},
		{i18, <<"1">>},
		{i18, <<"2">>},
		{i18, <<"a">>},
		{i18, <<"c">>},
		{i19, 1},
		{i19, 2},
		{i19, c},
		{i19, <<"a">>},
		{i20, <<"my#i20">>},
		{i21, my_i21},
		{i22, a},
		{i23, <<"a">>},
		{i24, 1},
		{i24, 2},
		{i24, c},
		{i24, <<"a">>},
		{i25, <<"1">>},
		{i25, <<"2">>},
		{i25, <<"a">>},
		{i25, <<"c">>},
		{i26, <<"Abc dÉf ghi">>},
		{i27, <<"abc def ghi">>},
		{i28, <<"abc">>},
		{i28, <<"def">>},
		{i28, <<"ghi">>},
		{i29, <<"abc">>},
		{i29, <<"def">>},
		{i29, <<"ghi">>},
		{i29, <<"kk">>},
		{<<"i02">>, <<"mykey">>}
	] = R1 = expand_indices(S1, K, Obj1),
	Obj2 = maps:to_list(Obj1),
	R1 = expand_indices(S1, K, Obj2),

	[
		{i01, mykey},
		{i20, <<"my#i20">>},
		{i21, my_i21},
		{i29, <<"abc">>},
		{i29, <<"def">>},
		{i29, <<"ghi">>},
		{i29, <<"kk">>},
		{<<"i02">>, <<"mykey">>}
	] = expand_indices(S1, K, other_thing),
	ok.

test_func1(_, Map) when is_map(Map) -> maps:get(field1, Map);
test_func1(_, List) when is_list(List) -> get_value(field1, List);
test_func1(_, _) -> [].


test_func2(_, Map) when is_map(Map) -> maps:get(field7, Map);
test_func2(_, List) when is_list(List) -> get_value(field7, List);
test_func2(_, _) -> [].


-endif.



