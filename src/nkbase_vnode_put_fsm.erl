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

%% @private Put Command processor FSM
-module(nkbase_vnode_put_fsm).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-behavior(gen_fsm).

-include("nkbase.hrl").

-export([put/3]).
% -export([launch_remote/1]).
-export([prepare/2, execute/2, waiting_coord/2, waiting_rest/2, wait_remote/2]).
-export([start_link/1, init/1, code_change/4, handle_event/3, handle_info/3,
         handle_sync_event/4, terminate/3]).


%%%===================================================================
%%% Internal
%%%===================================================================


-record(state, {
	ext_key :: nkbase:ext_key(), 
	ext_obj :: nkbase:ext_obj(),
	put_meta :: nkbase:put_meta(),
	chash_key :: chash:index(),
	from :: {reference(), pid()},
	n :: pos_integer(),
	w :: pos_integer(),
	timeout :: pos_integer(),
	preflist = [] :: [{chash:index_as_int(), node()}],
	replies = [] :: [{chash:index_as_int(), node()}],
	replied :: boolean()
}).


%% @doc Performs a command and wait for answer
-spec put(nkbase:ext_key(), nkbase:ext_obj(), nkbase:put_meta()) ->
	ok | {error, term()}.

put(ExtKey, ExtObj, Meta) ->
	ReqId = make_ref(),
	Timeout = 1000 * maps:get(timeout, Meta, ?DEFAULT_TIMEOUT),
	{_Domain, Class, Key} = ExtKey,
    CHashKey = case maps:get(hash, Meta, id) of
		id -> chash:key_of({Class, Key});
		class -> chash:key_of({Class, same_key})
	end,
	N = maps:get(n, Meta, ?DEFAULT_N),
	W = maps:get(w, Meta, ?DEFAULT_N),
	State = #state{
		ext_key = ExtKey,
		ext_obj = ExtObj,
		put_meta = maps:with([backend, ctx, reconcile], Meta), 
		chash_key = CHashKey,
		from = {ReqId, self()},
		n = N,
		w = min(W, N),
		timeout = Timeout,
		preflist = [],
		replies = [],
		replied = false
	},
	{ok, _} = supervisor:start_child(nkbase_vnode_put_fsm_sup, [State]),
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

prepare(timeout, #state{chash_key=CHashKey, n=N, w=W}=State) ->
	Nodes = riak_core_node_watcher:nodes(nkbase),
 	case riak_core_apl:get_apl_ann(CHashKey, N, Nodes) of
		[] -> 
			lager:error("NkBASE: No vnode available for write"),
			State1 = reply({error, no_vnode_available}, State),
			{stop, normal, State1};
		AnnPreflist ->
			%% Annotated preflist, having the type (primary or fallbackº)
			lager:debug("PUT preflist: ~p", [
				[{nkbase_util:idx2pos(Idx), Node, Type} ||
				{{Idx, Node}, Type} <- AnnPreflist]
			]),
			Preflist = [{Idx, Node} || {{Idx, Node}, _} <- AnnPreflist],
            case lists:keytake(node(), 2, Preflist) of
                {value, MyVNode, Rest} ->
					case length(Preflist) of
						N1 when N1 < N -> 
							lager:warning("NkBASE: Not enough nodes for N=~p (~p)", 
										  [N, N1]);
						N1 -> 
							ok
					end,
					W1 = min(W, N1),
					State1 = State#state{preflist=[MyVNode|Rest], n=N1, w=W1},
					{next_state, execute, State1, 0};
            	false ->
                    %% This node is not in the preference list
                    %% forward on to a random node
                    Pos = nkbase_util:l_timestamp() rem length(Preflist),
                    {_Idx, CoordNode} = lists:nth(Pos+1, Preflist),
                    proc_lib:spawn(CoordNode, supervisor, start_child,
                    			   [nkbase_vnode_put_fsm_sup, [State]]),
                    lager:debug("PUT spawning to node ~p", [CoordNode]),
                    {stop, normal, State}
			end
	end.


%% @private
-spec execute(timeout, #state{}) ->
	{next_state, waiting_coord, #state{}, pos_integer()}.

execute(timeout, State) ->
	#state{
		preflist = [{Idx, Node}|Rest], 
		ext_key = ExtKey,
		put_meta = Meta,
		ext_obj = ExtObj,
		timeout = Timeout
	} = State,
	lager:debug("PUT Execute to ~p, ~p", [nkbase_util:idx2pos(Idx), Node]),
    riak_core_vnode_master:command(
		{Idx, Node},
		{put_coord, ExtKey, ExtObj, Meta},
		{fsm, undefined, self()},
		nkbase_vnode_master),
	State1 = State#state{preflist=Rest, ext_obj=undefined},
    {next_state, waiting_coord, State1, Timeout}.


%% @private
-spec waiting_coord({vnode, integer(), atom(), {ok, dvvset:clock()}|{error, term()}},
					#state{}) ->
	{next_state, waiting_rest, #state{}, pos_integer()} | {stop, term(), #state{}}.

waiting_coord({vnode, Idx, Node, {ok, DVV}}, State) ->
	#state{
		w = W,
		preflist = PrefList, 
		ext_key = ExtKey,
		put_meta = Meta,
		replies = Replies, 
		timeout = Timeout
	} = State,
	State1 = case W==1 of
		true -> reply(ok, State);
		false -> State
	end,
	case PrefList of
		[] ->
			{stop, normal, State1};
		_ ->
		    riak_core_vnode_master:command(
				PrefList,
				{put, ExtKey, DVV, Meta},
				{fsm, undefined, self()},
				nkbase_vnode_master),
			Replies1 = [{Idx, Node}|Replies],
			State2 = State1#state{replies=Replies1},
		    {next_state, waiting_rest, State2, Timeout}
	end;

waiting_coord({vnode, _Idx, _Node, {error, Error}}, State) ->
	State1 = reply({error, Error}, State),
	{stop, normal, State1};

waiting_coord(timeout, State) ->
	{stop, timeout, State}.


%% @private
-spec waiting_rest({vnode, integer(), atom(), {ok, dvvset:clock()}|{error, term()}},
				   #state{}) ->
	{next_state, waiting, #state{}, pos_integer()} | {stop, term(), #state{}}.

waiting_rest({vnode, Idx, Node, ok}, State) ->
	#state{replies=Replies, n=N, w=W, timeout=Timeout} = State,
	Replies1 = [{Idx, Node}|Replies],
	State1 = State#state{replies=Replies1},
	case length(Replies1) of
		N ->
			State2 = reply(ok, State1),
			{stop, normal, State2};
		W ->
			State2 = reply(ok, State1),
			{next_state, waiting_rest, State2, Timeout};
		_ ->
			{next_state, waiting_rest, State1, Timeout}
	end;

waiting_rest({vnode, _Idx, _Node, {error, Error}}, State) ->
	State1 = reply({error, Error}, State),
	{stop, normal, State1};

waiting_rest(timeout, State) ->
	{stop, timeout, State}.


%% @private
-spec wait_remote(timeout, #state{}) ->
	{stop, timeout, #state{}}.

wait_remote(timeout, State) ->
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
	Pid ! {?MODULE, ReqId, Value},
	State#state{replied=true};
reply(_, State)->
	State.

