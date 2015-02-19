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
%% ------------------------------------------------------------------

%% @doc Ensemble subsystem management
-module(nkbase_ensemble).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-behaviour(gen_server).

-export([enable/0, cluster/0, ensembles/0, local_ensembles/0, members/1]).
-export([check_quorum/0, count_quorum/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).
-export([start_link/0]).

-include("nkbase.hrl").
-include("../deps/riak_ensemble/include/riak_ensemble_types.hrl").


%% ===================================================================
%% public
%% ===================================================================

%% @doc Forces enabling of ensemble subsystem. 
%% This should only be performed in a single node in the cluster.
%% The recommended way is letting enable the subsystem using the 
%% 'enable_consensus' riak_core parameter, and waiting to have three nodes 
%% in the cluster.
%%
%% After the subsystem is enabled, riak_core will take care of add every 
%% new cluster member to the ensemble cluster and as a member of the 'root'
%% ensemble, and also removing leaving nodes.
-spec enable() ->
    ok | {error, term()}.

enable() ->
    case whereis(riak_ensemble_sup) of
        undefined ->
            {error, not_started};
        _ ->
            case riak_ensemble_manager:enabled() of
                true ->
                    ok;
                false ->
                    case riak_ensemble_manager:enable() of
                        ok -> 
                            gen_server:cast(?MODULE, enable),
                            lager:notice("Waiting for ensemble leaders..."),
                            wait_for_ready(30);
                        error -> 
                            {error, could_not_enable}
                    end
            end
    end.


%% @doc Gets the current nodes of the cluster
-spec cluster() ->
    [node()].

cluster() ->
    riak_ensemble_manager:cluster().


%% @doc Gets all known ensembles
-spec ensembles() ->
    [ensemble_id()].

ensembles() ->
    case riak_ensemble_manager:known_ensembles() of
        {ok, Known} -> [Name || {Name, _Info} <- Known];
        _ -> []
    end.


%% @doc Gets ensembles local to this node
-spec local_ensembles() ->
    [ensemble_id()].

local_ensembles() ->
    Node = node(),
    lists:foldl(fun(Ensemble, Acc) ->
        Members = members(Ensemble),
        case lists:keymember(Node, 2, Members) of
            true -> [Ensemble | Acc];
            false -> Acc
        end
    end, [], ensembles()).


%% @doc Gets the current members for a ensemble
-spec members(ensemble_id()) ->
    [peer_id()].

members(Ensemble) ->
    riak_ensemble_manager:get_members(Ensemble).


%% @private
-spec check_quorum() ->
    [boolean()].

check_quorum() ->
    [riak_ensemble_manager:check_quorum(Ens, 2000) || Ens <- req_ensembles()].


%% @private
-spec count_quorum() ->
    [integer()].

count_quorum() ->
    [riak_ensemble_manager:count_quorum(Ens, 10000) || Ens <- req_ensembles()].


%% @private
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


%% ===================================================================
%%  gen_server
%% ===================================================================

-record(state, {}).

init([]) ->
    erlang:send_after(500, self(), tick),
    {ok, #state{}}.

handle_call(Msg, _From, State) ->
    lager:error("Module ~p received unexpected call ~p", [?MODULE, Msg]),
    {noreply, State}.

handle_cast(enable, State) ->
    maybe_bootstrap_ensembles(),
    {noreply, State};

handle_cast(Msg, State) ->
    lager:error("Module ~p received unexpected cast ~p", [?MODULE, Msg]),
    {noreply, State}.

handle_info(tick, State) ->
    maybe_bootstrap_ensembles(),
    erlang:send_after(10000, self(), tick),
    {noreply, State};

handle_info(Msg, State) ->
    lager:warning("Module ~p received unexpected info ~p", [?MODULE, Msg]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.



%% ===================================================================
%% Internal functions
%% ===================================================================

%% @private
wait_for_ready(0) ->
    error;
wait_for_ready(N) ->
    case ensembles() of
        Ens when is_list(Ens), length(Ens) > 1 ->
            Leaders = [is_pid(riak_ensemble_manager:get_leader_pid(E)) || E <- Ens],
            case lists:member(false, Leaders) of
                true -> 
                    timer:sleep(1000),
                    wait_for_ready(N-1);
                false ->
                    lager:notice("... ensemble system ok"),
                    ok
            end;
        _ ->
            timer:sleep(1000),
            wait_for_ready(N-1)
    end.


%% @private Try to create all missing ensembles, only if we are the
%% claimant node.
maybe_bootstrap_ensembles() ->
    case riak_ensemble_manager:enabled() of
        false ->
            ok;
        true ->
            {ok, Ring, CHBin} = riak_core_ring_manager:get_raw_ring_chashbin(),
            IsClaimant = (riak_core_ring:claimant(Ring) == node()),
            IsReady = riak_core_ring:ring_ready(Ring),
            case IsClaimant and IsReady of
                true ->
                    bootstrap_preflists(Ring, CHBin);
                false ->
                    ok
            end
    end.


%% @private
bootstrap_preflists(Ring, CHBin) ->
    Required = req_ensembles(Ring),
    Known = case riak_ensemble_manager:known_ensembles() of
        {ok, EnsList} -> [Name || {Name, _Info} <- EnsList];
        _ -> []
    end,
    Need = Required -- Known,
    lists:foreach(
        fun(Ensemble) ->
            Peers = required_members(Ensemble, CHBin),
            {nkv, EnsIdx, N} = Ensemble,
            R = riak_ensemble_manager:create_ensemble(Ensemble, undefined, Peers,
                                                      nkbase_ensemble_backend, []),
            lager:debug("Adding peers to ensemble ~p, (n=~p): ~p (~p)", 
                          [nkbase_util:idx2pos(EnsIdx), N, 
                          [{nkbase_util:idx2pos(Idx), Node} ||
                           {{nkv, _, _, Idx}, Node} <-Peers], R])
        end,
        Need).


%% @private
-spec required_members({nkv, EnsIdx::integer(), N::integer()}, term()) ->
    [{{nkv, EnsIdx::integer(), N::integer(), Idx::integer()}, node()}].

required_members({nkv, EnsIdx, N}, CHBin) ->
    % Gets N elements from the ring, starting at EnsIdx
    {PL, _} = chashbin:itr_pop(N, chashbin:exact_iterator(EnsIdx, CHBin)),
    [{{nkv, EnsIdx, N, Idx}, Node} || {Idx, Node} <- PL].


%% @private Gets a list of all required ensembles
-spec req_ensembles() ->
    [{nkv, Idx::integer(), N::integer()}].

req_ensembles() ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    req_ensembles(Ring).


%% @private Gets a list of all required ensembles
-spec req_ensembles(riak_core_ring:riak_core_ring()) ->
    [{nkv, EnsIdx::integer(), N::integer()}].

req_ensembles(Ring) ->
    Owners = riak_core_ring:all_owners(Ring),
    AllN = get_all_n(),
    [{nkv, EnsIdx, N} || {EnsIdx, _} <- Owners, N <- AllN].


%% @private Get all current N values
-spec get_all_n() ->
  [integer()].

get_all_n() ->
    List = lists:foldl(
        fun({{_Domain, _Name}, Meta}, Acc) ->
            Ns = [maps:get(n, Data, ?DEFAULT_N) || Data <- Meta, is_map(Data)],
            [Ns|Acc]
        end,
        [?DEFAULT_N],
        riak_core_metadata:to_list(?CLASS_PREFIX)),
    lists:usort(lists:flatten(List)).


