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

%% @private Coverage management module
-module(nkbase_coverage_fsm).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').

-behavior(riak_core_coverage_fsm).

-include("nkbase.hrl").

-export([launch/5]).
-export([init/2, process_results/2, finish/2]).

-type cmd() :: term().


%% @doc Launches a new coverage job
-spec launch(cmd(), pos_integer(), pos_integer(), 
			 fun((Data::term(), Acc::term()) -> NewAcc::term()), Acc0::term()) ->
	{ok, MSecs::integer(), term()} | {error, term()}.

launch(Cmd, N, Secs, FoldFun, FoldAcc) ->
	Start = os:timestamp(),
	ReqId = erlang:phash2(make_ref()),
	From = {raw, ReqId, self()},
	Timeout = 1000 * Secs,
	{ok, _} = supervisor:start_child(nkbase_coverage_fsm_sup, [From, {Cmd, N, Timeout}]),
	case wait_results(ReqId, Timeout, FoldFun, FoldAcc) of
		{ok, Data} ->
			Elapsed = timer:now_diff(os:timestamp(), Start)/1000,
			{ok, round(Elapsed), Data};
		{error, Error} ->
			{error, Error}
	end.

%% @private
wait_results(ReqId, Timeout, Fun, Acc) ->
    receive
        {ReqId, {data, Data}} -> 
        	wait_results(ReqId, Timeout, Fun, Fun(Data, Acc));
        {ReqId, done} -> 
        	{ok, Acc};
        {ReqId, {error, Error}} -> 
        	{error, Error}
    after Timeout ->
       	{error, {timeout, Acc}}
    end.


%%%===================================================================
%%% Internal
%%%===================================================================

-record(state, {
	cmd :: cmd(),
	from :: {raw, reference(), pid()}
}).


%% @private
%% - Request: Cmd,
%% - NodeSelector: Either the atom `all' to indicate that enough VNodes 
%%   must be available to achieve a minimal covering set or 'allup' to use 
%%	 whatever VNodes are available even if they  do not represent a fully covering set.
%% - NVal: N,
%% - PrimaryVNodeCoverage: The number of primary VNodes from the preference list 
%%	 to use in creating the coverage plan.
%% - NodeCheckService: The service to use to check for available nodes
%% - VNodeMaster: The atom to use to reach the vnode master module.
%% - Timeout - The timeout interval for the coverage request.
%% - State - The initial state for the module
%%
init(From, {Cmd, N, Timeout}) ->
	{
		Cmd, 
		all,
		N,
		1, 								
		nkbase,
		nkbase_vnode_master, 		
		Timeout,
		riak_core_coverage_plan,
		#state{cmd=Cmd, from=From}
	}.


%% @private
process_results({vnode, _Idx, _Node, {data, Data}}, State) ->
	% lager:warning("DATA: ~p, ~p", [_Idx, Data]),
	reply({data, Data}, State),
	{ok, State};

process_results({vnode, _Idx, _Node, {done, Data}}, State) ->
	% lager:warning("DONE: ~p, ~p", [_Idx, Data]),
	reply({data, Data}, State),
	{done, State};

process_results({vnode, _Idx, _Node, done}, State) ->
	{done, State};

process_results({vnode, _Idx, _Node, {error, Error}}, State) ->
	reply({error, Error}, State),
	{done, State}.
	

%% @private
finish(clean, State) ->
	reply(done, State),
    {stop, normal, State};

finish({error, Error}, #state{cmd=Cmd}=State) ->
	lager:error("Process results finish error in cmd ~p: ~p", [Cmd, Error]),
    reply({error, Error}, State),
    {stop, normal, State}.


%% ===================================================================
%% Internal functions
%% ===================================================================


%% @private
reply(Reply, #state{from={raw, ReqId, Pid}}) -> 
	Pid ! {ReqId, Reply}.


