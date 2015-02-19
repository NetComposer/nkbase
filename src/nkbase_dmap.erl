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

%% @doc Distributed Maps Management
-module(nkbase_dmap).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').

-export([get/3, get/4, update/4, update/5, del/3, del/4, values/1, raw_values/1]).
-export([reconcile/2, get_updates/1]).
-export_type([dmap/0, update_spec/0]).

-include("nkbase.hrl").


%% ===================================================================
%% Types
%% ===================================================================

% Available field types
-type type() :: flag | register | counter | set | map.


%% A DMap is a special, automatically convergent data structure similar to a 
%% typed erlang map. It is an opaque structure, but you can access to its fields
%% when calling get/3,4 or calling values().
%% Each field of the resulting map is a tuple {Type, Value}.
%% Current Types and possible Values are:
%%
%% - flag: Can be 'enabled' or 'disabled'
%% - register: Can be any Erlang term()
%% - counter: Any integer()
%% - set: A list of any unique Erlang term()
%% - map: A nested dmap()
%%
-type dmap() :: {'$nkmap', binary()}.


%% You can perform a number of operations on a dmap() to modify the value of
%% any of its fields, add new fields or remove them.
%%
%% Depending on the type of field, you can describe a set of operations to be 
%% performed on the field. If you perform an operation that is not allowed for
%% the current type of the field, it will fail.
%%
%% Flag
%% ----
%% - enable:      the field will switch to 'enabled' .
%% - disable:     the field will switch to 'disabled'. If you use a context, 
%%			      the flag will not switch to disabled until all enables 
%%				  have been disabled.
%% - remove_flag: remove the field (same note about using a context).
%% If a enable and disable is simultaneously ordered, the field will be enabled.
%%
%% Register
%% --------
%% - assign:		  assign a new value to the field. Last value wins.
%%					  You can use your own concept of 'last' using the version
%%					  with time, otherwise the system time will be used.
%% - remove_register: remove the field.
%%
%% Counter
%% -------
%% - increment, decrement: increments or decrements this amout
%%							 ("1" and "-1" by default).
%% - remove_counter:       removes the field.
%% If a operation and remove is simultaneously ordered, the removal is ignored.
%% 
%% Set
%% ---
%% - add, remove: 		  Adds or remove this element from the set.
%% - add_all, remove_all: Adds or remove this list of elements from the set.
%% - remove_set:		  Removes the full set
%% If an element is added and removed simultaneously, it remains in the set.
%% If you remove and element that does not exist, you get an error, unless you use
%% a context.
%% 
%% Map
%% ---
%%  - list():	  a new, nested map is created, and you can describe the opeations 
%%				  to apply to it. Each one has the previous types and behaviour.
%%  - remove_map: remove the map and all of its nested fields.
%% If an element in the map is updated and the map is removed at the same time,
%% the map continues but only with the updated elements.
%% If you remove and element that does not exist, you get an error, unless you use
%% a context.
%% 
%% Using contexts
%% --------------
%% Contexts are only used for disables and removes. You can obtain the current
%% context of a dmap() calling get/3,4 or values/1, and the the current context
%% will be present in the special '_context' field.
%% You can the use it in the update list, adding {'_context', {apply, Context}}.
%% This way, you are saying that you indeed know the object you are modifying, and
%% that it is safe to remove the fail, so it will be performed without more checks.
%% 
-type update_spec() :: [{update_field(), update_op()}].

-type update_field() :: term().

-type update_op() ::
	% Flag operations:
	enable | disable | remove_flag |
	% Register operations:
	{assign, term()} | {assign, term(), Time::integer()} | remove_register |
	% Counter operations:
	increment | {increment, integer()} | decrement | {decrement, integer()} | 
	remove_counter |
	% Set operations:
	{add, term()} | {add_all, [term()]} | {remove, term()} | {remove_all, [term()]} |
	remove_set |
	% Map operatios:
	[update_op()] | remove_map |
	% Context operations (only for field '_context'):
	{apply, riak_dt:context()}. 


%% Replied object
-type reply() :: 
	#{
		term() => {type(), term()}	
	}
	|
	#{
		fields => #{ term() => {type(), term()}},
		indices => #{ nkbase:index_name() => [term()]}
	}.



%% ===================================================================
%% API
%% ===================================================================


%% @doc Equivalent to get(Domain, Class, Key, #{})
-spec get(nkbase:domain(), nkbase:class(), nkbase:key()) ->
	{ok, reply()} | {error, term()}.

get(Domain, Class, Key) ->
	get(Domain, Class, Key, #{}).


%% @doc Gets a dmap from the database, returning and erlang map() with
%% the dmap values and context.
%%
%% This function is very similar to nkbase:get/4, but assumes 
%% that the requested object is a dmap(), resolving conflicts on read automatically.
%% Options in Meta override options in the stored class, if present.
%%
%% You can use the 'get_fields' and 'get_indices' options to extract specific
%% fields or indices.
%%
-spec get(nkbase:domain(), nkbase:class(), nkbase:key(), nkbase:get_meta()) ->
	{ok, nkbase:reply()} | {error, term()}.

get(Domain, Class, Key, Meta) ->
	Meta1 = nkbase:get_class(Domain, Class, Meta),
	Meta2 = Meta1#{reconcile=>fun reconcile/2},
	case nkbase_cmds:get({Domain, Class, Key}, Meta2) of
		{maps, {_Ctx, [Map]}} when is_map(Map) -> {ok, Map};
		{maps, {_Ctx, Maps}} -> {error, {invalid_dmap, Maps}};
		{values, {_Ctx, [{'$nkmap', BinMap}]}} -> {ok, values({'$nkmap', BinMap})};
		{values, {_Ctx, Objs}} -> {error, {invalid_dmap, Objs}};
		{error, Error} -> {error, Error}
 	end.


%% @doc Equivalent to update(Domain, Class, Key, UpdateSpec, #{})
-spec update(nkbase:domain(), nkbase:class(), nkbase:key(), update_spec()) ->
	ok | {error, term()}.

update(Domain, Class, Key, UpdateSpec) ->
	update(Domain, Class, Key, UpdateSpec, #{}).


%% @doc Applies a serie of modifications to a dmap() stored at the server.
%% 
%% This funcion is similar to nkbase:put/4,5, but, insted of sending an object,
%% sends the list of operations. The object is retrieved at the first vnode,
%% and after appling the modifications a new object is generated, that is indexed,
%% stored and sent to the rest of vnodes. 
%% You can't specify the object's context (it is read from the base object), but
%% you can include the dmap's context (see update_spec())
%%
%% Any conflict is automatically resolved. 
%% 
%% The object class can include any index specification.
%% Options in Meta override options in the stored class, if present.
%%
-spec update(nkbase:domain(), nkbase:class(), nkbase:key(), 
			 update_spec(), nkbase:put_meta()) ->
	ok | {error, term()}.

% Indices should be indicated in class, we don't have the object to index yet
update(_, _, _, _, #{indices:=Indices}) when Indices/=[] ->
	{error, indices_not_allowed};

update(Domain, Class, Key, UpdateSpec, Meta) when is_list(UpdateSpec) ->
	Meta1 = Meta#{reconcile=>fun reconcile/2},
	nkbase_cmds:update({Domain, Class, Key}, fun fun_update/4, UpdateSpec, Meta1).


%% @doc Same as del(Domain, Class, Key, #{})
-spec del(nkbase:domain(), nkbase:class(), nkbase:key()) ->
	ok | {error, term()}.

del(Domain, Class, Key) ->
	del(Domain, Class, Key, #{}).


%% @doc Removes all fields from a dmap and schedules its deletion.
%%
%% This function will find a existing dmap, remove all of this fields, 
%% and add a ttl to schedule its removal (see nkbase:del/3,4 for an explanation).
%%
%% Options in Meta override options in the stored class, if present.
%%
-spec del(nkbase:domain(), nkbase:class(), nkbase:key(), nkbase:put_meta()) ->
	ok | {error, term()}.

del(Domain,Class, Key, Meta) ->
	TTL = maps:get(ttl, Meta, ?DEFAULT_DEL_TTL),
	Meta1 = Meta#{ttl=>TTL, reconcile=>fun reconcile/2},
	nkbase_cmds:update({Domain, Class, Key}, fun fun_update/4, remove_all, Meta1).


%% @doc Gets a dmap() from a stored object
-spec values(dmap()) ->
	map().

values({'$nkmap', BinMap}) ->
	{ok, DtMap} = riak_dt_map:from_binary(BinMap),
	DMap = dtmap_to_values(riak_dt_map:value(DtMap)),
	DtCtx = riak_dt_map:precondition_context(DtMap),
	DMap#{'_context' => {context, DtCtx}}.


%% @private
-spec raw_values(dmap()) ->
	map().

raw_values({'$nkmap', BinMap}) ->
	{ok, DtMap} = riak_dt_map:from_binary(BinMap),
	dtmap_to_raw_values(riak_dt_map:value(DtMap)).



%% ===================================================================
%% Internal
%% ===================================================================



%% @private
-spec fun_update(nkbase:ext_key(), [nkbase:ext_obj()], 
	             update_spec()|remove_all, term()) ->
	{ok, nkbase:obj()} | {error, term()}.

fun_update(ExtKey, [], Update, VNode) ->
	BinMap = riak_dt_map:to_binary(riak_dt_map:new()),
	fun_update(ExtKey, [{#{}, {'$nkmap', BinMap}}], Update, VNode);

fun_update(ExtKey, [{_, '$nkdeleted'}], Update, VNode) ->
	fun_update(ExtKey, [], Update, VNode);

fun_update(_ExtKey, [{_ObjMeta, {'$nkmap', BinMap}}], remove_all, VNode) ->
	{ok, DtMap} = riak_dt_map:from_binary(BinMap),
	DtUpdate = get_remove_all(riak_dt_map:value(DtMap)),
	DtCtx = riak_dt_map:precondition_context(DtMap),
	{ok, DtMap1} = riak_dt_map:update(DtUpdate, VNode, DtMap, DtCtx),
	BinMap1 = riak_dt_map:to_binary(DtMap1),
	{ok, {'$nkmap', BinMap1}};

fun_update(_ExtKey, [{_ObjMeta, {'$nkmap', BinMap}}], Update, VNode) ->
	case get_updates(Update) of
		{update, _} = DtUpdate ->
			{ok, DtMap} = riak_dt_map:from_binary(BinMap),
			DtCtx = case nkbase_util:get_value('_context', Update) of
				undefined -> undefined;
				{context, DtCtx0} -> DtCtx0
			end,
			case riak_dt_map:update(DtUpdate, VNode, DtMap, DtCtx) of
				{ok, DtMap1} ->
					BinMap1 = riak_dt_map:to_binary(DtMap1),
					{ok, {'$nkmap', BinMap1}};
				{error, {precondition, {not_present, {Field, _Type}}}} ->
					{error, {field_not_present, Field}}
			end;
		{error, Error} ->
			{error, Error}
	end;

fun_update(_ExtKey, [T], _Update, _VNode) ->
	{error, {invalid_dmap, T}};

fun_update(ExtKey, Values, Update, VNode) ->
	case reconcile(ExtKey, Values) of
		{ok, Merged} -> fun_update(ExtKey, [Merged],  Update, VNode);
		{error, Error} -> {error, Error}
	end.



%% @private Reoncile function for DMaps
-spec reconcile(nkbase:ext_key(), [nkbase:ext_obj()]) ->
	{ok, nkbase:ext_obj()} | {error, term()}.

reconcile(ExtKey, [{_, '$nkdeleted'}, {ObjMeta, Value}|Rest]) ->
	reconcile(ExtKey, [{ObjMeta, Value}|Rest]);

reconcile(ExtKey, [{ObjMeta, Value}, {_, '$nkdeleted'}|Rest]) ->
	reconcile(ExtKey, [{ObjMeta, Value}|Rest]);

reconcile(_ExtKey, [{ObjMeta, {'$nkmap', BinMap}}]) ->
	{ok, {ObjMeta, {'$nkmap', BinMap}}};

reconcile(ExtKey, [{Meta1, {'$nkmap', BinMap1}}, {Meta2, {'$nkmap', BinMap2}}|Rest]) ->
	{ok, DtMap1} = riak_dt_map:from_binary(BinMap1),
	{ok, DtMap2} = riak_dt_map:from_binary(BinMap2),
	BinMerged = riak_dt_map:to_binary(riak_dt_map:merge(DtMap1, DtMap2)),
	Obj = {'$nkmap', BinMerged},
	% Update time, indices
	{Meta3, Obj} = nkbase:make_ext_obj(ExtKey, Obj, #{}),
	Exp1 = maps:get(exp, Meta1, undefined), 
	Exp2 = maps:get(exp, Meta2, undefined),
	Meta4 = case is_integer(Exp1) andalso is_integer(Exp2) of
		true -> Meta3#{exp=>max(Exp1, Exp2)};
		false -> Meta3
	end,
	reconcile(ExtKey, [{Meta4, Obj}|Rest]);

reconcile(_, Values) ->
	{error, {invalid_dmap, Values}}.


%% ===================================================================
%% Internal
%% ===================================================================

-define(MFLAG, riak_dt_od_flag).
-define(MREG, riak_dt_lwwreg).
-define(MCNTR, riak_dt_emcntr).
-define(MSET, riak_dt_orswot).
-define(MMAP, riak_dt_map).


%% @private
-spec get_updates(update_spec()) ->
	{update, riak_dt_map:map_op()} | {error, term()}.

get_updates(Update) ->
	try lists:foldl(fun do_update/2, [], Update) of
		Result -> {update, lists:reverse(Result)}
	catch
		throw:Error -> {error, Error}
	end.


%% @private
do_update({'_context', _V}, Acc) -> Acc;
do_update({K, enable}, Acc) -> [{update, {K, ?MFLAG}, enable}|Acc];
do_update({K, disable}, Acc) -> [{update, {K, ?MFLAG}, enable}|Acc];
do_update({K, remove_flag}, Acc) -> [{remove, {K, ?MFLAG}}|Acc];
do_update({K, {assign, V}}, Acc) -> [{update, {K, ?MREG}, {assign, V}}|Acc];
do_update({K, {assign, V, T}}, Acc) -> [{update, {K, ?MREG}, {assign, V, T}}|Acc];
do_update({K, remove_register}, Acc) -> [{remove, {K, ?MREG}}|Acc];
do_update({K, increment}, Acc) -> [{update, {K, ?MCNTR}, increment}|Acc];
do_update({K, {increment, V}}, Acc) -> [{update, {K, ?MCNTR}, {increment, V}}|Acc];
do_update({K, decrement}, Acc) -> [{update, {K, ?MCNTR}, decrement}|Acc];
do_update({K, {decrement, V}}, Acc) -> [{update, {K, ?MCNTR}, {decrement, V}}|Acc];
do_update({K, remove_counter}, Acc) -> [{remove, {K, ?MCNTR}}|Acc];
do_update({K, {add, V}}, Acc) -> [{update, {K, ?MSET}, {add, V}}|Acc];
do_update({K, {add_all, [_|_]=V}}, Acc) -> [{update, {K, ?MSET}, {add_all, V}}|Acc];
do_update({K, {remove, V}}, Acc) -> [{update, {K, ?MSET}, {remove, V}}|Acc];
do_update({K, {remove_all, [_|_]=V}}, Acc) -> [{update, {K, ?MSET}, {remove_all, V}}|Acc];
do_update({K, remove_set}, Acc) -> [{remove, {K, ?MSET}}|Acc];
do_update({K, [_|_]=V}, Acc) -> [{update, {K, ?MMAP}, get_updates(V)}|Acc];
do_update({K, remove_map}, Acc) -> [{remove, {K, ?MMAP}}|Acc];
do_update(Term, _Acc) -> throw({invalid_value, Term}).


%% @private
-spec get_remove_all(update_spec()) ->
	{update, MapOp::list()}.

get_remove_all(Values) ->
	Fun = fun({K, _}, Acc) ->
		case K of
			{F, ?MFLAG} -> [{remove, {F, ?MFLAG}}|Acc];
			{F, ?MCNTR} -> [{remove, {F, ?MCNTR}}|Acc];
			{F, ?MREG} -> [{remove, {F, ?MREG}}|Acc];
			{F, ?MSET} -> [{remove, {F, ?MSET}}|Acc];
			{F, ?MMAP} -> [{remove, {F, ?MMAP}}|Acc]
		end
	end,
	{update, lists:foldl(Fun, [], Values)}.


%% @private
-spec dtmap_to_values(RiakDtMapValues::list()) ->
	map().

dtmap_to_values(Values) ->
	Fun = fun({K, V}, Acc) ->
		% lager:warning("K: ~p, ~p", [K, V]),
		case K of
			{F, ?MFLAG} when V==true-> [{F, {flag, enabled}}|Acc];
			{F, ?MFLAG} when V==false-> [{F, {flag, disabled}}|Acc];
			{F, ?MCNTR} -> [{F, {counter, V}}|Acc];
			{F, ?MREG} -> [{F, {register, V}}|Acc];
			{F, ?MSET} -> [{F, {set, V}}|Acc];
			{F, ?MMAP} -> [{F, {map, dtmap_to_values(V)}}|Acc]
		end
	end,
	maps:from_list(lists:foldl(Fun, [], Values)).


%% @private
-spec dtmap_to_raw_values(RiakDTMapValues::list()) ->
	map().

dtmap_to_raw_values(Values) ->
	Fun = fun({K, V}, Acc) ->
		% lager:warning("K: ~p, ~p", [K, V]),
		case K of
			{F, ?MFLAG} when V==true-> [{F, enabled}|Acc];
			{F, ?MFLAG} when V==false-> [{F, disabled}|Acc];
			{F, ?MCNTR} -> [{F, V}|Acc];
			{F, ?MREG} -> [{F, V}|Acc];
			{F, ?MSET} -> [{F, V}|Acc];
			{F, ?MMAP} -> [{F, dtmap_to_raw_values(V)}|Acc]
		end
	end,
	maps:from_list(lists:foldl(Fun, [], Values)).



%% ===================================================================
%% EUnit tests
%% ===================================================================

% -define(TEST, true).
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

update_test() ->
	Up1 = [
		{'_context', any},
		{f1, enable},
		{f2, disable},
		{f2, remove_flag},
		{r1, {assign, value1}},
		{r2, {assign, value2}},
		{r2, remove_register},
		{c1, increment},
		{c2, {increment, 5}},
		{c3, decrement},
		{c3, remove_counter},
		{s1, {add, e1}},
		{s1, {add_all, [e2, e3, e4, e5]}},
		{s1, {remove, e3}},
		{s1, {remove_all, [e4, e5]}},
		{s2, {add, e6}},
		{s2, remove_set},
		{m1, [
			{m1f1, enable},
			{m1c1, increment}
		]},
		{m2, [
			{m2f1, disable}
		]},
		{m2, remove_map}
	],
	Updates1 = get_updates(Up1),
	{ok, DtMap1} = riak_dt_map:update(Updates1, actor1, riak_dt_map:new()),
	Values1 = riak_dt_map:value(DtMap1),
	DMap1 = dtmap_to_values(Values1),
	DMap2 = #{
		c1 => {counter, 1},
  		c2 => {counter, 5},
  		f1 => {flag, enabled},
  		m1 => {map, #{m1c1 => {counter,1}, m1f1 => {flag,enabled}}},
  		r1 => {register,value1},
  		s1 => {set,[e1,e2]}
  	},
  	true = DMap1 == DMap2,
	DMap3 = dtmap_to_raw_values(Values1),
	DMap4 = #{
		c1 => 1,
  		c2 => 5,
		f1 => enabled,
		m1 => #{m1c1 => 1, m1f1 => enabled},
		r1 => value1,
		s1 => [e1,e2]
	},
  	true = DMap3 == DMap4,
  	Updates2 = get_remove_all(Values1),
  	% {Updates2, Values1}.
  	{ok, DtMap2} = riak_dt_map:update(Updates2, actor1, DtMap1),
	[] = riak_dt_map:value(DtMap2),
	#{} = dtmap_to_raw_values([]).


%% From this point, this is more an example/tutorial on CRDTs more than
%% a test...

flag_test() ->
	M = riak_dt_od_flag,
	F1 = M:new(),
	false = M:value(F1),
	% Update never fails
	{ok, F2} = M:update(enable, actor1, F1),
	% VClock, Dot, Deferred
	{[{actor1,1}], [{actor1,1}], []} = F2,

	{ok, F3} = M:update(enable, actor1, F2),
	{[{actor1,2}], [{actor1,2}], []} = F3,
	{ok, F4} = M:update(enable, actor2, F3),
	{[{actor2,1},{actor1,2}],[{actor1,2},{actor2,1}],[]} = F4,
	% Using a dot() instead of an actor()
	{ok, F5} = M:update(enable, {actor2, 3}, F4),
	{[{actor1,2},{actor2,3}],[{actor1,2},{actor2,3}],[]} = F5,
	[{actor_count,2},{dot_length,2},{deferred_length,0}] = M:stats(F5),

	% Disable removed the dot(), does not increment vclock()
	{ok, F6} = M:update(disable, any, F5),
	{[{actor1,2},{actor2,3}], [], []} = F6,
	{ok, F7} = M:update(disable, any, F6),
	{[{actor1,2},{actor2,3}], [], []} = F7,
	[{actor_count,2},{dot_length,0},{deferred_length,0}] = M:stats(F7),

	% Context is only used for disable. 
	% Any disable without context removed all the dots
	% With context, only removes dots that are 'older' (seen by the context)

	% First A enables, then B enables, then C enables
	{ok, A1} = M:update(enable, actorA, M:new()),
	{ok, B1} = M:update(enable, actorB, A1),
	{ok, C1} = M:update(enable, actorC, B1),
	[{actorA, 1}] = CtxA1 = M:precondition_context(A1),
	[{actorB, 1}, {actorA, 1}] = _CtxB1 = M:precondition_context(B1),
	[{actorC, 1}, {actorB, 1}, {actorA, 1}] = CtxC1 = M:precondition_context(C1),
	
	% Without context, we remove all dots
	{ok, A2a} = M:update(disable, any, C1),
	{CtxC1, [],[]} = A2a,
	false = M:value(A2a),
	
	% With context, only 'seen' dots are removed, so it is not really disabled yet
	% 'A' gets the context, and meanwhile 'B' and 'C' enable.
	% A sends the disable, but has not seen the enables from 'B' and 'C'.
	% If someone with CtxC1 disables, it would be really disabled.
	{ok, A2b} = M:update(disable, any, C1, CtxA1),
	{CtxC1, [{actorB,1},{actorC,1}],[]} = A2b,
	true = M:value(A2b),

	% If the context is newer thant the stored vclock, the context is added
	% to the deferred list, to be processed at merge, where, if now 
	% the context is ok, the disable is processed again
	% For example, a replica vnode appears again with an old value, and 'A' updates it
	{ok, A3} = M:update(enable, actorA, A1),
	[{actorA, 2}] = M:precondition_context(A3),
	% Before merging, any actor with the latest context send a disable
	{ok, Z1} = M:update(disable, any, A3, CtxC1),
	% The vclock does not change
	% All known dots are removed, but the last enable from A remains
	% The context is added for later processing as deferred
 	{[{actorA,2}], [{actorA,2}], [[{actorC,1},{actorB,1},{actorA,1}]]} = Z1,

 	% If we merge, the known situation is normalized
 	% The vclock is the merge of both vclocks
 	% The dot is the sum of common, and not dominated by the other's vclock
 	% The deferred list to process is the sum of both, and it is applied 
 	% (tried the disables) again if possible, if not any can remain
 	{[{actorA,2},{actorB,1},{actorC,1}],[{actorA,2}],[]} = M:merge(Z1, C1).


lww_test() ->
	M = riak_dt_lwwreg,
	L1 = M:new(),
	% Assign without time (an automatic time-based is used)
	% Update never fails. Actors are not used in this type
	{ok, L2} = M:update({assign, val1}, any, L1),
	% Value, Timestamp
	{val1, Epoch} = L2,
	% Lower timestmaps are ignored
 	{ok, L2} = M:update({assign, val2, Epoch-1}, any, L2),
 	{ok, L3} = M:update({assign, val3, Epoch+1}, any, L2),
 	true = {val3, Epoch+1} == L3,

 	% Merge only takes the latest value
 	L3 = M:merge(L2, L3).


counter_1_test() ->
	%% Base PN-Counter
	M = riak_dt_pncounter,
	C1 = M:new(),
	{ok, C2} = M:update(increment, actor1, C1),
	{ok, C3} = M:update(increment, actor2, C2),
	{ok, C4} = M:update(decrement, actor3, C3),
	1 = M:value(C4),

	%% Now we increment and decrement at the same time:
	{ok, C5a} = M:update(increment, actor1, C4),
	{ok, C5b} = M:update(decrement, actor2, C4),
	2 = M:value(C5a),
	0 = M:value(C5b),
	C5f = M:merge(C5a, C5b),
	1 = M:value(C5f).


counter_2_test() ->
	%% Counter based on PN-Counter to embed in maps
	%% No precondition_context is used
	%% Update never fails
	M = riak_dt_emcntr,
	C1 = M:new(),
	% For this type, we must use dots instead of actors
	{ok, C2} = M:update(increment, {actorA, 1}, C1),
	% VClock, 
	{[{actorA,1}], [{actorA, {1,1,0}}]} = C2,
	{ok, C3a} = M:update({decrement, 2}, {actorB, 1}, C2),
	{[{actorA,1},{actorB,1}], [{actorA,{1,1,0}},{actorB,{1,0,2}}]} = C3a,

	{ok, C3b} = M:update({increment, 5}, {actorA, 2}, C2),
	{[{actorA,2}], [{actorA,{2,6,0}}]} = C3b,

	C3 = M:merge(C3a, C3b),
	{[{actorA,2},{actorB,1}], [{actorA,{2,6,0}},{actorB,{1,0,2}}]} = C3.


set_test() ->
	% If an add and remove are concurrent, add wins


	% Updates can fail only with {error, {precondition ,{not_present, member()}}}
	M = riak_dt_orswot,
	S1 = M:new(),
	{ok, S2} = M:update({add, elem1}, actor1, S1),
	% Whole vclock, each element with a vclock also, and deferred list
	{[{actor1,1}], D2, Empty} = S2,
	D2 = dict:from_list([{elem1, [{actor1,1}]}]),
	Empty = dict:new(),

	{ok, S3} = M:update({add_all, [elem1, elem2, elem3]}, actor2, S2),
	[elem1, elem2, elem3] = M:value(S3),
	{[{actor2,3},{actor1,1}], D3, Empty} = S3,
	D3 = dict:from_list([
		{elem1,[{actor2,1}]},
		{elem2,[{actor2,2}]},
		{elem3,[{actor2,3}]}
	]),
	
	%% Now we add and remove elements at the same time.
	%% elem1 is removed by actor1
	%% elem3 is added by actor1 and removed by actor2, but add wins
	%% elem4 is added by actor2
	{ok, S4a} = M:update({update, [{remove, elem1}, {add, elem3}, {add, elem5}]}, actor1, S3),
	[elem2, elem3, elem5] = M:value(S4a),
	{[{actor1,3},{actor2,3}], D4a, Empty} = S4a,
 	D4a = dict:from_list([
		{elem2,[{actor2,2}]},
		{elem3,[{actor1,2}]},
		{elem5,[{actor1,3}]}
	]),

	{ok, S4b} = M:update({update, [{remove, elem3}, {add, elem4}, {add, elem5}]}, actor2, S3),
	[elem1, elem2, elem4, elem5] = M:value(S4b),
	{[{actor2,5},{actor1,1}], D4b, Empty} = S4b,
 	D4b = dict:from_list([
		{elem1,[{actor2,1}]},
		{elem2,[{actor2,2}]},
		{elem4,[{actor2,4}]},
		{elem5,[{actor2,5}]}
	]),

	S4 = M:merge(S4a, S4b),
	[elem2, elem3, elem4, elem5] = M:value(S4),
	{[{actor1,3},{actor2,5}], D4, Empty} = S4,
 	D4 = dict:from_list([
		{elem2,[{actor2,2}]},
		{elem3,[{actor1,2}]},
		{elem4,[{actor2,4}]},
		{elem5,[{actor1,3},{actor2,5}]}
	]),

 	% If we try to remove an element we don't have, without context,
 	% we get an error
 	% (Actors are not used for removals)
 	{error,{precondition,{not_present,elem4}}} = M:update({remove, elem4}, any, S2),

 	% If we really know the object we are updating, we can use its context:
 	CtxS2 = M:precondition_context(S2),
 	{ok, Z1} = M:update({remove, elem4}, any, S2, CtxS2),
	{[{actor1,1}], DZ1, Empty} = S2 = Z1,
 	DZ1 = dict:from_list([{elem1,[{actor1,1}]}]),
	% If we have a new context, but and old version appears, the operation is
	% delayed
 	CtxS4 = M:precondition_context(S4),
 	{ok, Z2} = M:update({remove, elem4}, any, S2, CtxS4),
	{[{actor1,1}], DZ2a, DZ2b} = Z2,
	DZ2a = dict:from_list([{elem1,[{actor1,1}]}]),
	DZ2b = dict:from_list([{[{actor1,3},{actor2,5}],[elem4]}]),
	
	% Then, at merge time, we can really apply the removal
	Z3 = M:merge(S4, Z2),
	{[{actor1,3},{actor2,5}], DZ3, Empty} = Z3,
	DZ3 = dict:from_list([
		{elem2,[{actor2,2}]},
		{elem3,[{actor1,2}]},
		{elem5,[{actor1,3},{actor2,5}]}
	]),
	ok.


map_test() ->
	M1 = riak_dt_map:new(),
	Up1 = [
		{update, {field1, riak_dt_emcntr}, increment},
		{update, {field2, riak_dt_orswot}, {add, <<"A">>}},
		{update, {field3, riak_dt_lwwreg}, {assign, <<"a">>, 1}},
		{update, {field4, riak_dt_od_flag}, enable}
	],
	{ok, M2} = riak_dt_map:update({update, Up1}, actor1, M1),
	[
        {{field1,riak_dt_emcntr},1},
		{{field2,riak_dt_orswot},[<<"A">>]},
        {{field3,riak_dt_lwwreg},<<"a">>},
        {{field4,riak_dt_od_flag},true}
    ] = lists:sort(riak_dt_map:value(M2)),
 	Up2 = [
 		{remove, {field4, riak_dt_od_flag}},
 		{update, {field5, riak_dt_map}, 
 			{update, [
 				{update, {field6, riak_dt_lwwreg}, {assign, <<"b">>}}
			]}
		}
 	],
 	{ok, M3} = riak_dt_map:update({update, Up2}, actor2, M2),
	[
 		{{field1,riak_dt_emcntr},1},
		{{field2,riak_dt_orswot},[<<"A">>]},
 		{{field3,riak_dt_lwwreg},<<"a">>},
 		{{field5,riak_dt_map}, [
 			{{field6,riak_dt_lwwreg},<<"b">>}
 		]}
 	] = 
 		lists:sort(riak_dt_map:value(M3)).

-endif.
