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

%% @private Get Command processor FSM
-module(nkbase_vnode_get_fsm).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-behavior(gen_fsm).

-include("nkbase.hrl").

-export([get/2]).
-export([prepare/2, execute/2, waiting/2, waiting_put/2]).
-export([start_link/1, init/1, code_change/4, handle_event/3, handle_info/3,
         handle_sync_event/4, terminate/3]).


%%%===================================================================
%%% Internal
%%%===================================================================


-record(state, {
	ext_key :: nkbase:ext_key(), 
	get_meta :: nkbase:put_meta(),
	put_meta :: nkbase:put_meta(),
	chash_key :: chash:index(),
	from :: {reference(), pid()},
	n :: pos_integer(),
	r :: pos_integer(),
	timeout :: pos_integer(),
	preflist = [] :: [{chash:index_as_int(), node()}],
	replies = [] :: [{chash:index_as_int(), node(), none|dvvset:clock()}],
	replied :: boolean()
}).


%% @doc Performs a command and wait for answer
-spec get(nkbase:ext_key(), nkbase:put_meta()) ->
	{maps, {nkbase:ctx(), [nkbase:obj()]}} |
	{values, {nkbase:ctx(), [nkbase:obj()]}} |
	{error, term()}.

get(ExtKey, Meta) ->
	ReqId = make_ref(),
	Timeout = 1000 * maps:get(timeout, Meta, ?DEFAULT_TIMEOUT),
	{_Domain, Class, Key} = ExtKey,
    CHashKey = case maps:get(hash, Meta, id) of
		id -> chash:key_of({Class, Key});
		class -> chash:key_of({Class, same_key})
	end,
	N = maps:get(n, Meta, ?DEFAULT_N),
	R = maps:get(r, Meta, ?DEFAULT_N),
	State = #state{
		ext_key = ExtKey,
		get_meta = maps:with([get_fields, get_indices], Meta),
		put_meta = maps:with([backend, reconcile], Meta),
		chash_key = CHashKey,
		from = {ReqId, self()},
		n = N,
		r = min(R, N),
		timeout = Timeout,
		preflist = [],
		replies = [],
		replied = false
	},
	{ok, _} = supervisor:start_child(nkbase_vnode_get_fsm_sup, [State]),
	get_reply(ReqId, Timeout).


%% @private
-spec get_reply(reference(), pos_integer()) ->
	term() | {error, timeout}.

get_reply(ReqId, Timeout) ->
  	receive 
		{?MODULE, ReqId, Value} -> Value;
		{?MODULE, _, _} -> get_reply(ReqId, Timeout)	% Clean old messages
	after 
		Timeout -> {error, timeout}
	end.




%%%===================================================================
%%% States
%%%===================================================================

%% @private
start_link(InitState) ->
    gen_fsm:start_link(?MODULE, InitState, []).


%% @private
init(InitState) ->
	{ok, prepare, InitState, 0}.


%% @private
-spec prepare(timeout, #state{}) ->
	{next_state, execute|wait_remote, #state{}} | {stop, term(), #state{}}.

prepare(timeout, #state{chash_key=CHashKey, n=N, r=R}=State) ->
	Nodes = riak_core_node_watcher:nodes(nkbase),
 	case riak_core_apl:get_apl_ann(CHashKey, N, Nodes) of
		[] -> 
			lager:error("NkBASE: No vnode available for read"),
			State1 = reply({error, no_vnode_available}, State),
			{stop, normal, State1};
		AnnPreflist ->
			lager:debug("GET preflist: ~p", [
				[{nkdist_util:idx2pos(Idx), Node, Type} ||
				{{Idx, Node}, Type} <- AnnPreflist]
			]),
			Preflist = [{Idx, Node} || {{Idx, Node}, _} <- AnnPreflist],
			N1 = length(Preflist),
			R1 = min(R, N1),
			State1 = State#state{preflist=Preflist, n=N1, r=R1},
			{next_state, execute, State1, 0}
	end.


%% @private
-spec execute(timeout, #state{}) ->
	{next_state, waiting, #state{}, pos_integer()}.

execute(timeout, State) ->
	#state{preflist=PrefList, put_meta=Meta, ext_key=ExtKey, timeout=Timeout} = State,
	Backend = maps:get(backend, Meta, ?DEFAULT_BACKEND),
    riak_core_vnode_master:command(
		PrefList,
		{get, Backend, ExtKey},
		{fsm, undefined, self()},
		nkbase_vnode_master),
    {next_state, waiting, State, Timeout}.


%% @private
-spec waiting({vnode, integer(), atom(), {ok, none|dvvset:clock()}|{error, term()}},
					#state{}) ->
	{next_state, waiting, #state{}, pos_integer()} | {stop, term(), #state{}}.

waiting({vnode, Idx, Node, {error, not_found}}, State) ->
	waiting({vnode, Idx, Node, {ok, none}}, State);

waiting({vnode, _Idx, _Node, {error, Error}}, State) ->
	State1 = reply({error, Error}, State),
	{stop, normal, State1};

waiting({vnode, Idx, Node, {ok, DVV}}, State) ->
	#state{replies=Replies, r=R, n=N, timeout=Timeout} = State,
	Replies1 = [{Idx, Node, DVV}|Replies],
	State1 = State#state{replies=Replies1},
	case length(Replies1) of
		N ->
			UserReply = get_sync(State1),
			State2 = reply(UserReply, State1),
			case UserReply of
				{ok, DVV1} -> execute_repair(DVV1, State2);
				{error, not_found} -> {stop, normal, State}
			end;
		R ->
			UserReply = get_sync(State1),
			State2 = reply(UserReply, State1),
			{next_state, waiting, State2, Timeout};
		_ -> 
			{next_state, waiting, State1, Timeout}
	end;

waiting(timeout, State) ->
	{stop, timeout, State}.


%% @private
-spec get_sync(#state{}) ->
	{ok, dvvset:clock()} | {error, term()}.

get_sync(#state{ext_key=ExtKey, replies=Replies, put_meta=Meta}) ->
	ToSync = [D || {_, _, D} <- Replies, D/=none],
	case ToSync of
		[] ->
			{error, not_found};
		_ ->
			DVV1 = dvvset:sync(ToSync),
			case dvvset:size(DVV1) of
				1 -> 
					{ok, DVV1};
				_ ->
					Reconcile = maps:get(reconcile, Meta, undefined),
					nkbase_util:reconcile(Reconcile, ExtKey, DVV1)
			end
	end.


%% @private
-spec execute_repair(dvvset:clock(), #state{}) ->
	{next_state, waiting_put, #state{}, pos_integer()} | {stop, term(), #state{}}.

execute_repair(DVV, State) ->
	#state{ext_key=ExtKey, put_meta=Meta, timeout=Timeout, replies=Replies} = State,
	Updates = [{Idx, Node} ||
			   {Idx, Node, D} <- Replies, D==none orelse not dvvset:equal(DVV, D)],
	case Updates of
		[] ->
			{stop, normal, State};
		_ ->
			lager:notice("Sending Read Repair to vnodes: ~p", 
				[[{nkdist_util:idx2pos(Idx), Node} || {Idx, Node} <- Updates]]),
			riak_core_vnode_master:command(
				Updates,
				{put, ExtKey, DVV, Meta},
				{fsm, undefined, self()},
				nkbase_vnode_master),
			State1 = State#state{replies=[], n=length(Updates)},
		    {next_state, waiting_put, State1, Timeout}
	end.


%% @private
-spec waiting_put({vnode, integer(), atom(), {ok, dvvset:clock()}|{error, term()}},
				   #state{}) ->
	{next_state, waiting, #state{}, pos_integer()} | {stop, term(), #state{}}.

waiting_put({vnode, Idx, Node, _Reply}, State) ->
	#state{replies=Replies, n=N, timeout=Timeout} = State,
	Replies1 = [{Idx, Node, none}|Replies],
	case length(Replies1) of
		N -> 
			{stop, normal, State};
		_ -> 
			{next_state, waiting_put, State#state{replies=Replies1}, Timeout}
	end;

waiting_put(timeout, State) ->
	{stop, timeout, State}.


%% @private
handle_info(Info, StateName, #state{timeout=Timeout}=State) ->
	lager:warning("Module ~p received unexpeced info ~p at ~p",
				  [?MODULE, Info, StateName]),
	{next_state, StateName, State, Timeout}.


%% @private
handle_event(Event, StateName, #state{timeout=Timeout}=State) ->
	lager:warning("Module ~p received unexpeced event ~p at ~p",
				  [?MODULE, Event, StateName]),
	{next_state, StateName, State, Timeout}.


%% @private
handle_sync_event(Event, _From, StateName, #state{timeout=Timeout}=State) ->
	lager:warning("Module ~p received unexpeced sync event ~p at ~p",
				  [?MODULE, Event, StateName]),
	{next_state, StateName, State, Timeout}.


%% @private
code_change(_OldVsn, StateName, State, _Extra) -> 
	{ok, StateName, State}.


%% @private
terminate(_Reason, _SN, _State) ->
    ok.


%%%===================================================================
%%% Internal Functions
%%%===================================================================

reply(Value, #state{replied=false, from={ReqId, Pid}}=State) when is_pid(Pid) ->
	Value1 = case Value of
		{ok, DVV} ->
			Ctx = dvvset:join(DVV),
			ExtObjs = dvvset:values(DVV),
			#state{get_meta=Meta} = State,
			case 
				maps:is_key(get_fields, Meta) orelse 
				maps:is_key(get_indices, Meta) 
			of
				true ->
					Maps = nkbase_util:get_spec(Meta, ExtObjs),
					{maps, {Ctx, Maps}};
				false ->
					{values, {Ctx, [Obj || {_, Obj} <- ExtObjs]}}
			end;
		{error, Error} ->
			{error, Error}
	end,
	Pid ! {?MODULE, ReqId, Value1},
	State#state{replied=true};

reply(_, State)->
	State.

