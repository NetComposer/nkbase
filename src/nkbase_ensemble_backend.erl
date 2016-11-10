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

-module(nkbase_ensemble_backend).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-behaviour(riak_ensemble_backend).

-include("nkbase.hrl").

-export([init/3, new_obj/4]).
-export([obj_epoch/1, obj_seq/1, obj_key/1, obj_value/1]).
-export([set_obj_epoch/2, set_obj_seq/2, set_obj_value/2]).
-export([get/3, put/4, tick/5, ping/2, ready_to_start/0]).
-export([synctree_path/2]).
-export([handle_down/4]).

-include_lib("riak_ensemble_ng/include/riak_ensemble_types.hrl").

-record(state, {
    ensemble   :: ensemble_id(),
    id         :: peer_id(),
    pos        :: integer(),
    proxy      :: atom(),
    proxy_mon  :: reference(),
    vnode_mon  :: reference(),
    async      :: pid()
}).

-type ens_obj() :: #nk_ens{}.


%% ===================================================================
%% Riak Ensemble Behaviour
%% ===================================================================


%% @private
-spec init(ensemble_id(), peer_id(), [any()]) -> 
    #state{}.

init(Ensemble, Id, []) ->
    {nkv, EnsIdx, N} = Ensemble,
    {{nkv, EnsIdx, N, Idx}, _Node} = Id,
    lager:notice("Started ensemble ~p (n=~p) peer ~p (~p)", 
                  [nkdist_util:idx2pos(EnsIdx), N, nkdist_util:idx2pos(Idx), self()]),
    Proxy = riak_core_vnode_proxy:reg_name(nkbase_vnode, Idx),
    ProxyRef = erlang:monitor(process, Proxy),
    {ok, Vnode} = riak_core_vnode_manager:get_vnode_pid(Idx, nkbase_vnode),
    VnodeRef = erlang:monitor(process, Vnode),
    #state{
        ensemble = Ensemble,
        id = Id,
        pos = nkdist_util:idx2pos(Idx),
        proxy = Proxy,
        proxy_mon = ProxyRef,
        vnode_mon = VnodeRef
    }.


%% @private
-spec new_obj(epoch(), seq(), nkbase:ext_key(), nkbase:ext_obj()|notfound) ->
    #nk_ens{}.

new_obj(Epoch, Seq, ExtKey, Value) ->
    #nk_ens{eseq=make_eseq(Epoch, Seq), key=ExtKey, val=Value}.


%% @private
-spec obj_epoch(ens_obj()) -> epoch().
obj_epoch(#nk_ens{eseq=ESeq}) ->
    {Epoch, _} = get_epoch_seq(ESeq),
    Epoch.


%% @private
-spec obj_seq(ens_obj()) -> seq().
obj_seq(#nk_ens{eseq=ESeq}) ->
    {_, Seq} = get_epoch_seq(ESeq),
    Seq.


%% @private
-spec obj_key(ens_obj()) -> nkbase:ext_key().
obj_key(#nk_ens{key=ExtKey}) -> ExtKey.


%% @private
-spec obj_value(ens_obj()) -> nkbase:ext_obj().
obj_value(#nk_ens{val=Val}) -> Val.


%% @private
-spec set_obj_epoch(epoch(), ens_obj()) -> ens_obj().
set_obj_epoch(Epoch, #nk_ens{eseq=ESeq}=EnsObj) -> 
    {_, Seq} = get_epoch_seq(ESeq),
    EnsObj#nk_ens{eseq=make_eseq(Epoch, Seq)}.


%% @private
-spec set_obj_seq(seq(), ens_obj()) -> ens_obj().
set_obj_seq(Seq, #nk_ens{eseq=ESeq}=EnsObj) ->
    {Epoch, _} = get_epoch_seq(ESeq),
    EnsObj#nk_ens{eseq=make_eseq(Epoch, Seq)}.


%% @private
-spec set_obj_value(nkbase:ext_obj(), ens_obj()) -> ens_obj().
set_obj_value(Value, EnsObj) ->
    EnsObj#nk_ens{val=Value}.


%% @private
-spec get(nkbase:ext_key(), riak_ensemble_backend:from(), #state{}) -> 
          #state{}.
get(ExtKey, From, #state{proxy=Proxy}=State) ->
    catch Proxy ! {ensemble_get, ExtKey, From},
    State.


%% @private
-spec put(nkbase:ext_key(), ens_obj(), riak_ensemble_backend:from(), #state{}) ->
    #state{}.
put(ExtKey, EnsObj, From, #state{proxy=Proxy}=State) ->
    catch Proxy ! {ensemble_put, ExtKey, EnsObj, From},
    State.


%% @private
-spec tick(epoch(), seq(), peer_id(), views(), #state{}) -> 
    #state{}.

tick(_Epoch, _Seq, _Leader, Views, State) ->
    % lager:notice("E: ~p, S: ~p, L: ~p, V: ~p", [_Epoch, _Seq, _Leader, Views]),
    #state{
        id = {{nkv, EnsIdx, N, _}, _},
        async = Async
    } = State,
    Latest = hd(Views),
    {ok, CHBin} = riak_core_ring_manager:get_chash_bin(),
    {PL, _} = chashbin:itr_pop(N, chashbin:exact_iterator(EnsIdx, CHBin)),
    %% TODO: Make ensembles/peers use ensemble/peer as actual peer name so this is unneeded
    Peers = [{{nkv, EnsIdx, N, Idx}, Node} || {Idx, Node} <- PL],
    Add = Peers -- Latest,
    Del = Latest -- Peers,
    Changes = [{add, Peer} || Peer <- Add] ++ [{del, Peer} || Peer <- Del],
    case Changes of
        [] ->
            State;
        _ ->
            case is_pid(Async) andalso is_process_alive(Async) of
                true ->
                    State;
                false ->
                    Self = self(),
                    Async2 = spawn(
                        fun() ->
                            case riak_ensemble_peer:update_members(Self, Changes, 5000) of
                                ok ->
                                    lager:warning("Executed Ensemble Changes at ~p: ~p", 
                                        [State#state.pos, 
                                          [{Type, nkdist_util:idx2pos(Idx), Node} ||
                                           {Type, {{nkv, _, _, Idx}, Node}} <- Changes]]);
                                _ ->
                                    erro
                            end
                        end),
                        State#state{async=Async2}
            end
    end.


%% @private
-spec ping(pid(), #state{}) -> {ok | async | failed, #state{}}.
ping(From, #state{proxy=Proxy}=State) ->
    catch Proxy ! {ensemble_ping, From},
    {async, State}.


%% @private
-spec handle_down(reference(), pid(), term(), #state{}) -> 
    false | {reset, #state{}}.

handle_down(Ref, _Pid, Reason, #state{vnode_mon=Ref}=State) ->
    #state{
        id = {{nkv, EnsIdx, _N, Idx}, _},
        pos = Pos
    } = State,
    case Reason of
        normal -> 
            ok;
        _ -> 
            lager:warning("Ensemble Backend ~p (~p): vnode crashed with reason: ~p", 
                          [nkdist_util:idx2pos(EnsIdx), Pos, Reason])
    end,
    %% There are some races here (see riak_kv_ensemble_backend)
    {ok, Vnode} = riak_core_vnode_manager:get_vnode_pid(Idx, nkbase_vnode),
    VnodeRef2 = erlang:monitor(process, Vnode),
    %% Returning reset means that the backend subsystem will restart itself,
    %% electing new master, etc.
    {reset, State#state{vnode_mon=VnodeRef2}};

handle_down(Ref, _Pid, Reason, #state{proxy_mon=Ref}=State) ->
    #state{
        id = {{nkv, EnsIdx, _N, _Idx}, _},
        proxy = Proxy, 
        pos=Pos
    } = State,
    case Reason of
        normal ->
            ok;
        _ ->
            lager:warning("Ensemble Backend ~p (~p): vnode proxy crashed with reason: ~p", 
                          [nkdist_util:idx2pos(EnsIdx), Pos, Reason])
    end,
    % If the new proxy has not yet registered, a new 'DOWN' will be received
    ProxyRef2 = erlang:monitor(process, Proxy),
    {reset, State#state{proxy_mon=ProxyRef2}};

handle_down(_Ref, _Pid, _Reason, _State) ->
    false.
    

%% @private
-spec ready_to_start() -> boolean().
ready_to_start() ->
    lists:member(nkbase, riak_core_node_watcher:services(node())).


%% @private
-spec synctree_path(ensemble_id(), peer_id()) -> 
    default | {binary(), string()}.

synctree_path(_Ensemble, Id) ->
    {{nkv, EnsIdx, N, Idx}, _} = Id,
    Bin = term_to_binary({EnsIdx, N}),
    %% Use a prefix byte to leave open the possibility of different
    %% tree id encodings (eg. not term_to_binary) in the future.
    TreeId = <<0, Bin/binary>>,
    {TreeId, "nkv_" ++ integer_to_list(Idx)}.



%% ===================================================================
%% Internal
%% ===================================================================

%% @private
-spec make_eseq(integer(), integer()) ->  integer().
make_eseq(Epoch, Seq) -> 
    (Epoch bsl 32) + Seq.


%% @private
-spec get_epoch_seq(integer()) -> {integer(), integer()}.
get_epoch_seq(ESeq) ->
    <<Epoch:32/integer, Seq:32/integer>> = <<ESeq:64/integer>>,
    {Epoch, Seq}.








