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

%% @private Riak Core Vnode behaviour
-module(nkbase_vnode).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').

-behaviour(riak_core_vnode).

-export([start_vnode/1,
         init/1,
         terminate/2,
         handle_command/3,
         is_empty/1,
         delete/1,
         handle_handoff_command/3,
         handoff_starting/2,
         handoff_cancelled/1,
         handoff_finished/2,
         handle_handoff_data/2,
         encode_handoff_item/2,
         handle_coverage/4,
         handle_info/2,
		 handle_overload_command/3,
		 handle_overload_info/2,
         handle_exit/3,
         ready_to_exit/0,
         set_vnode_forwarding/2]).

-include("nkbase.hrl").
-include_lib("riak_core_ng/include/riak_core_vnode.hrl").


%% @private
start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).


%%%%%%%%%%%%%%%%%%%%%%%%%%%% VNode Behaviour %%%%%%%%%%%%%%%%%%%%%%%%%%%%

% -type exp_set() :: {nkbase:backend(), nkbase:ext_key(), nklib_util:timestamp()}.

-record(state, {
	idx :: chash:index_as_int(),				% vnode's index
	pos :: pos_integer(),						% short number
	ets_ref :: nkbase_backend:db_ref(),			% ETS table
	leveldb_ref :: nkbase_backend:db_ref(),		% LevelDB reference
	expire_check :: pos_integer(),				% secs
	expire_resolution :: pos_integer(),		    % msecs
    handoff_target :: {chash:index_as_int(), node()},
    forward :: node() | [{integer(), node()}]
}).


%% @private
init([Idx]) ->
	Pos = nkdist_util:idx2pos(Idx),
	EtsRef = ets:new(store, [ordered_set, public]),
	case nkbase_app:get_env(leveldb, true) of
		true ->
			Path = leveldb_path(Pos),
			{ok, LevelRef} = nkbase_backend:leveldb_open(Path);
		false ->
			Path = <<>>, 
			LevelRef = undefined
	end,
	ExpireCheck = nkbase_app:get_env(expire_check, 60),
	ExpireResolution = nkbase_app:get_env(expire_resolution, 1000),
    State = #state{
		idx = Idx,
		pos = Pos,
		ets_ref = EtsRef,
		leveldb_ref = LevelRef,
		expire_check = ExpireCheck,
		expire_resolution = ExpireResolution
	},
	lager:info("NkBASE vnode ~p started (ets:~p, leveldb:~s)", [Pos, EtsRef, Path]),
	case LevelRef of
		undefined ->
			ok;
		_ ->
			Now = nklib_util:l_timestamp(),
			del_all_expired(leveldb, Now, State),
			find_all_expires(leveldb, Now, State)
	end,
	Workers = nkbase_app:get_env(vnode_workers, 1),
	FoldWorkerPool = {pool, nkbase_vnode_worker, Workers, [Pos]},
	FirstShot = crypto:rand_uniform(1, 1000),
	riak_core_vnode:send_command_after(FirstShot, check_long_expire),
    {ok, State, [FoldWorkerPool]}.
		

%% @private
handle_command({get, Backend, ExtKey}, _Sender, State) ->
	lager:debug("GET at ~p for ~p", [State#state.pos, ExtKey]),
	Reply = do_get(Backend, ExtKey, State),
	reply(Reply, State);

handle_command({put_coord, ExtKey, {UpdMeta, {'$nkupdate', Fun, Arg}}, Meta}, 
				_Sender, #state{idx=Idx}=State) ->
	lager:debug("PUT UPDATE at ~p for ~p", [State#state.pos, ExtKey]),
	try
		Backend = maps:get(backend, Meta, ?DEFAULT_BACKEND),
		Ref = case get_ref(Backend, State) of
			{ok, Ref0} -> Ref0;
			{error, BkError} -> throw({error, BkError})
		end,
		{DVV0, Ctx, Values} = case catch 
			nkbase_backend:get({Backend, Ref}, ExtKey) 
		of
			{ok, GetDVV} ->  {GetDVV, dvvset:join(GetDVV), dvvset:values(GetDVV)};
			{error, not_found} -> {none, none, []};
			{error, GetError} -> throw({error, GetError});
			{'EXIT', GetError} -> throw({error, GetError})
		end,
		VNodeId = {Idx, node()},
		case catch Fun(ExtKey, Values, Arg, VNodeId) of
			{ok, Value1} ->
				% Get time, indices (ttl will not come in Meta)
				{ObjMeta1, Value1} = nkbase:make_ext_obj(ExtKey, Value1, Meta),
				% Take possible ttl in UpdMeta
				ObjMeta2 = maps:merge(UpdMeta, ObjMeta1),
				DVV1 = case Ctx of
					none -> dvvset:new({ObjMeta2, Value1});
					_ -> dvvset:new(Ctx, {ObjMeta2, Value1})
				end,
				Meta1 = Meta#{old_dvv=>DVV0, actor=>VNodeId},
				case do_put(Backend, ExtKey, DVV1, Meta1, State) of
					{ok, DVV2} ->
						reply({ok, DVV2}, State);
					{error, Error} -> 
						reply({error, Error}, State)
				end;
			{error, UpdateError} ->
				reply({error, UpdateError}, State);
			{'EXIT', UpdateError} ->
				reply({error, UpdateError}, State);
			UpdateError ->
				reply({error, {update_error, UpdateError}}, State)
		end
	catch
		throw:Throw -> reply(Throw, State)
	end;

handle_command({put_coord, ExtKey, ExtObj, Meta}, _Sender, #state{idx=Idx}=State) ->
	lager:debug("PUT COORD at ~p for ~p", [State#state.pos, ExtKey]),
	DVV = case maps:get(ctx, Meta, undefined) of
		undefined -> dvvset:new(ExtObj);
		Ctx -> dvvset:new(Ctx, ExtObj)
	end,
	Backend = maps:get(backend, Meta, ?DEFAULT_BACKEND),
	Meta1 = Meta#{actor=>{Idx, node()}},
	case do_put(Backend, ExtKey, DVV, Meta1, State) of
		{ok, DVV1} ->
			reply({ok, DVV1}, State);
		{error, Error} -> 
			reply({error, Error}, State)
	end;
	
handle_command({put, ExtKey, DVV, Meta}, _Sender, State) ->
	lager:debug("PUT at ~p for ~p", [State#state.pos, ExtKey]),
	Backend = maps:get(backend, Meta, ?DEFAULT_BACKEND),
	case do_put(Backend, ExtKey, DVV, Meta, State) of
		{ok, _DVV1} ->
			reply(ok, State);
		{error, Error} -> 
			reply({error, Error}, State)
	end;
	
handle_command({reindex, Backend, ExtKey, Spec}, Sender, State) ->
	lager:debug("REINDEX at ~p for ~p", [State#state.pos, ExtKey]),
	Reply = do_reindex(Backend, ExtKey, Spec, State),
	case Sender of
		ignore -> {noreply, State};
		_ -> reply(Reply, State)
	end;
	
handle_command(check_long_expire, ignore, #state{expire_check=ExpCheck}=State) ->
	lager:warning("CHECK LONG EXPIRE"),
	Now = nklib_util:l_timestamp(),
	% Check up to ExpCheck secs ago
	del_all_expired(ets, Now-1000000*ExpCheck, State),
	% Check from now
	find_all_expires(ets, Now, State),
	case get_ref(leveldb, State) of
		{ok, _} -> 
			del_all_expired(leveldb, Now-1000000*ExpCheck, State),
			find_all_expires(leveldb, Now, State);
		{error, _} -> 
			ok
	end,
	riak_core_vnode:send_command_after(1000*ExpCheck, check_long_expire),
	{noreply, State};

handle_command({check_expire, Backend, Slot}, ignore, State) ->
	Keys = get_meta({exp, Backend, Slot}, [], State),
	del_expired(Keys, Backend, Slot, State),
	del_meta({exp, Backend, Slot}, State),
	{noreply, State};


handle_command(Message, _Sender, State) ->
    lager:warning("NkBase vnode: Unhandled command: ~p, ~p", [Message, _Sender]),
    reply({error, unhandled_command}, State).


%% @private
handle_coverage({fold_domains, Spec}, _KeySpaces, Sender, State) ->
	case get_ref(Spec#fold_spec.backend, State) of
		{ok, Ref} ->
			Spec1 = Spec#fold_spec{db_ref=Ref},
			{async, {fold_domains, Spec1}, Sender, State};
		{error, Error} ->
			reply({error, Error}, State)
	end;

handle_coverage({fold_classes, Spec}, _KeySpaces, Sender, State) ->
	case get_ref(Spec#fold_spec.backend, State) of
		{ok, Ref} ->
			Spec1 = Spec#fold_spec{db_ref=Ref},
			{async, {fold_classes, Spec1}, Sender, State};
		{error, Error} ->
			reply({error, Error}, State)
	end;

handle_coverage({fold_objs, Spec}, KeySpaces, Sender, State) ->
	KeyFilter = get_key_filter(KeySpaces, State),
	case get_ref(Spec#fold_spec.backend, State) of
		{ok, Ref} ->
			Spec1 = Spec#fold_spec{db_ref=Ref, keyfilter=KeyFilter},
			{async, {fold_objs, Spec1}, Sender, State};
		{error, Error} ->
			reply({error, Error}, State)
	end;

handle_coverage({search, Spec}, KeySpaces, Sender, State) ->
	KeyFilter = get_key_filter(KeySpaces, State),
	case get_ref(Spec#search_spec.backend, State) of
		{ok, Ref} ->
			Spec1 = Spec#search_spec{db_ref=Ref, keyfilter=KeyFilter},
			{async, {search, Spec1}, Sender, State};
		{error, Error} ->
			reply({error, Error}, State)
	end;

%% Warning: do not use with strong consistency!!
%% Riak ensemble will not know
handle_coverage({remove_all, Backend}, _KeySpaces, _Sender, State) ->
	case get_ref(Backend, State) of
		{ok, EtsRef} when Backend==ets ->
			ets:delete_all_objects(EtsRef),
			reply(done, State);
		{ok, LevelRef} when Backend==leveldb ->
			File = leveldb_path(State#state.pos),
			ok = nkbase_backend:leveldb_destroy(LevelRef, File),
			{ok, LevelRef1} = nkbase_backend:leveldb_open(File),
			State1 = State#state{leveldb_ref=LevelRef1},
			reply(done, State1);
		{error, Error} ->
			reply({error, Error}, State)
	end;

handle_coverage({remove_all, Backend, Domain, Class}, _KeySpaces, Sender, State) ->
	case get_ref(Backend, State) of
		{ok, Ref} ->
			Fun = fun(ExtKey, Values, _) -> 
				do_del({Backend, Ref}, ExtKey, Values, false) 
			end,
			Spec = #fold_spec{
				backend = Backend, 
				db_ref = Ref, 
				domain = Domain,
				class = Class,
				fold_type = values,
				fold_fun = Fun,
				fold_acc = ok
			},
			{async, {fold_objs, Spec}, Sender, State};
		{error, Error} ->
			reply({error, Error}, State)
	end;

handle_coverage({reindex, Backend, Domain, Class, IndexSpec}, 
				_KeySpaces, Sender, State) ->
	Self = self(),
	case get_ref(Backend, State) of
		{ok, Ref} ->
			Fun = fun(ExtKey, [], ok) -> 
				throttle(Self, 100),
				Msg = {reindex, Backend, ExtKey, IndexSpec},
				riak_core_vnode:send_command(Self, Msg),
				ok
			end,
			Spec = #fold_spec{
				backend = Backend, 
				db_ref = Ref, 
				domain = Domain,
				class = Class,
				fold_type = keys,
				fold_fun = Fun,
				fold_acc = ok
			},
			{async, {fold_objs, Spec}, Sender, State};
		{error, Error} ->
			reply({error, Error}, State)
	end;

handle_coverage(update_config, _KeySpaces, _Sender, State) ->
	ExpireCheck = nkbase_app:get_env(expire_check, 60),
	ExpireResolution = nkbase_app:get_env(expire_resolution, 1000),
    State1 = State#state{
    	expire_check = ExpireCheck, 
    	expire_resolution = ExpireResolution
    },
	reply(done, State1);

handle_coverage(get_info, _KeySpaces, _Sender, #state{pos=Pos, ets_ref=Ets}=State) ->
	reply({done, {Pos, Ets}}, State);

handle_coverage(Cmd, _KeySpaces, _Sender, State) ->
	lager:error("Module ~p unknown coverage: ~p", [?MODULE, Cmd]),
	{noreply, State}.


%% @private
handle_handoff_command(?FOLD_REQ{foldfun=Fun, acc0=Acc0}, Sender, State) -> 
	lager:info("Handoff command FOLD_REQ at ~p", [State#state.pos]),
	{ok, EtsRef} = get_ref(ets, State),
	Refs1 = [{ets, EtsRef}],
	Refs2 = case get_ref(leveldb, State) of
		{ok, LevelRef}  -> Refs1 ++ [{leveldb, LevelRef}];
		{error, _} -> Refs1
	end,
	{async, {handoff, Fun, Acc0, Refs2}, Sender, State};

% Write locally AND send it to the new node
handle_handoff_command(Cmd, Sender, State) when 
		element(1, Cmd)==put_coord; element(1, Cmd)==put ->
	lager:info("Handoff command PUT at ~p", [State#state.pos]),
 	{reply, _, NewState} = handle_command(Cmd, Sender, State),
 	{forward, NewState};

% Rest of operarions locally
handle_handoff_command(Cmd, Sender, State) ->
	lager:info("Handoff command ~p at ~p", [Cmd, State#state.pos]),
	handle_command(Cmd, Sender, State).


%% @private
handoff_starting({Type, {Idx, Node}}, State) ->
	lager:info("Handoff (~p) starting at ~p to {~p, ~p}", 
				[Type, State#state.pos, nkdist_util:idx2pos(Idx), Node]),
    {true, State#state{handoff_target={Idx, Node}}}.


%% @private
handoff_cancelled(State) ->
	lager:notice("Handoff cancelled at ~p", [State#state.pos]),
    {ok, State#state{handoff_target=undefined}}.


%% @private
handoff_finished({Idx, Node}, State) ->
	lager:info("Handoff finished at ~p to ~p ~p", 
				[State#state.pos, nkdist_util:idx2pos(Idx), Node]),
    {ok, State#state{handoff_target=undefined}}.


%% @private
handle_handoff_data(BinObj, #state{pos=Pos}=State) ->
	case catch binary_to_term(zlib:unzip(BinObj)) of
		{h0, Backend, {Domain, Class, _} = ExtKey, DVV} ->
			Meta = nkbase:get_class(Domain, Class),
			case do_put(Backend, ExtKey, DVV, Meta, State) of
				{ok, _} -> 
					lager:info("NkBase vnode ~p stored handoff object ~p (~p)", 
							  [Pos, ExtKey, Backend]),
					{reply, ok, State};
				{error, Error} -> 
					lager:error("NkBase vnode ~p could not store handoff object "
								"~p (~p): ~p", [Pos, ExtKey, Backend, Error]),
					{reply, {error, Error}, State}
			end;	
		Other ->
			{reply, {error, {decode_error, Other}}, State}
	end.	


%% @private
encode_handoff_item({Backend, ExtKey}, DVV) ->
	% lager:info("Encode handoff data for ~p, ~p", [Backend, ExtKey]),
	zlib:zip(term_to_binary({h0, Backend, ExtKey, DVV})).


%% @private
is_empty(#state{pos=Pos}=State) ->
	{ok, EtsRef} = get_ref(ets, State),
	EtsEmpty = (ets:info(EtsRef, size))==0,
	LevelEmpty = case get_ref(leveldb, State) of
		{ok, LevelRef} -> (catch eleveldb:is_empty(LevelRef))==true;
		{error, _} -> true
	end,
	IsEmpty = EtsEmpty andalso LevelEmpty,
	lager:info("Nkbase vnode ~p is empty = ~p", [Pos, IsEmpty]),
	{IsEmpty, State}.
	

%% @private
delete(#state{pos=Pos}=State) ->
	lager:info("NkBase vnode ~p deleting", [Pos]),
	case get_ref(leveldb, State) of
		{ok, Ref} ->
			File = leveldb_path(Pos),
			ok = nkbase_backend:leveldb_destroy(Ref, File);
		{error, _} ->
			ok
	end,
    {ok, State}.


%% @private
handle_info({ensemble_ping, From}, State) ->
    riak_ensemble_backend:pong(From),
    {ok, State};

handle_info({ensemble_get, ExtKey, From}, State) ->
	#state{idx=Idx, forward=Fwd} = State,
	case Fwd of
		undefined ->
			%% We could be in "handoff" state, but we read from local anycase, like
			%% the normal get
			Reply = case do_get(leveldb, ExtKey, State) of
				{ok, DVV} ->
					case dvvset:join(DVV) of
						[{eseq, ESeq}] ->
							Val = case dvvset:values(DVV) of
								[{#{sc:=true}, '$nkdeleted'}] -> notfound;
								[{#{sc:=true}=ObjMeta, Obj}] -> {ObjMeta, Obj}
							end,
							#nk_ens{eseq=ESeq, key=ExtKey, val=Val};
						_ -> 
							notfound
					end;
				_ -> 
					notfound
			end,
			% io:format("ENS GET at ~p for ~p: ~p\n", 
						  % [State#state.pos, ExtKey, Reply]),
		    riak_ensemble_backend:reply(From, Reply);
		Node when is_atom(Node) ->
			%% We are in "forwarding" state, the handoff has completed and we
			%% are waiting for the new vnode to be marked as owner
		    Proxy = riak_core_vnode_proxy:reg_name(nkbase_vnode, Idx, Node),
		    BangMsg = {raw_forward_get, ExtKey, From},
    		riak_core_send_msg:bang_unreliable(Proxy, BangMsg)
    end,
    {ok, State};

handle_info({raw_forward_get, ExtKey, From}, State) ->
	lager:error("ENS FWD GET at ~p for ~p", [State#state.pos, ExtKey]),
	Reply = case do_get(leveldb, ExtKey, State) of
		{ok, DVV} -> 
			case dvvset:join(DVV) of
				[{eseq, ESeq}] ->
					Val = case dvvset:values(DVV) of
						[{#{sc:=true}, '$nkdeleted'}] -> notfound;
						[{#{sc:=true}=ObjMeta, Obj}] -> {ObjMeta, Obj}
					end,
					#nk_ens{eseq=ESeq, key=ExtKey, val=Val};
				_ -> 
					notfound
			end;
		_ -> 
			notfound
	end,
    riak_ensemble_backend:reply(From, Reply),
    {ok, State};

handle_info({ensemble_put, ExtKey, EnsObj, From}, State) ->
	#nk_ens{key=ExtKey, eseq=ESeq, val=Val} = EnsObj,
	#state{handoff_target=HOTarget, idx=Idx, forward=Fwd} = State,
	ExtObj = case Val of
		notfound -> {#{sc=>true}, '$nkdeleted'} ;
		{#{}=Meta, Obj} -> {Meta#{sc=>true}, Obj}
	end,
	DVV = dvvset:new([{eseq, ESeq}], ExtObj),
    case Fwd of
        undefined ->
			% io:format("ENS PUT at ~p for ~p: ~p\n", [State#state.pos, ExtKey, ExtObj]),
			Reply = case do_put(leveldb, ExtKey, DVV, #{actor=>ignore}, State) of
				{ok, _DVV1} when HOTarget==undefined ->
					EnsObj;
				{ok, _DVV1} ->
					%% We are in "handoff" state, so we write locally and in the
					%% new vnode, like the normal put
					{TIdx, TNode} = HOTarget,
				    Proxy = riak_core_vnode_proxy:reg_name(nkbase_vnode, TIdx, TNode),
    				%% Note: This cannot be bang_unreliable. Don't change.
    				Proxy ! {raw_forward_put, ExtKey, EnsObj, none},
    				EnsObj;
				{error, _} -> 
					failed
			end,
		    riak_ensemble_backend:reply(From, Reply);
        Node when is_atom(Node) ->
			%% We are in "forwarding" state, the handoff has completed and we
			%% are waiting for the new vnode to be marked as owner
		    Proxy = riak_core_vnode_proxy:reg_name(nkbase_vnode, Idx, Node),
		    BangMsg = {raw_forward_put, ExtKey, EnsObj, From},
		    riak_core_send_msg:bang_unreliable(Proxy, BangMsg)
    end,
    {ok, State};

handle_info({raw_forward_put, ExtKey, EnsObj, From}, State) ->
	#nk_ens{key=ExtKey, eseq=ESeq, val=Val} = EnsObj,
	ExtObj = case Val of
		notfound -> {#{sc=>true}, '$nkdeleted'} ;
		{#{}=Meta, Obj} -> {Meta#{sc=>true}, Obj}
	end,
	DVV = dvvset:new([{eseq, ESeq}], ExtObj),
	lager:error("ENS FWD PUT at ~p for ~p", [State#state.pos, ExtKey]),
	Reply = case do_put(leveldb, ExtKey, DVV, #{actor=>ignore}, State) of
		{ok, _DVV1} -> EnsObj;
		{error, _} -> failed
	end,
	case From of
		none -> ok;
    	_ -> riak_ensemble_backend:reply(From, Reply)
    end,
    {ok, State};

handle_info(Msg, State) ->
	lager:warning("Module ~p unexpected info: ~p", [?MODULE, Msg]),
	{ok, State}.


%% @private
handle_exit(Pid, Reason, State) ->
	lager:warning("NkBase vnode ~p: Unhandled EXIT ~p, ~p", 
				 [State#state.pos, Pid, Reason]),
	{noreply, State}.


%% @private
terminate(normal, _State) ->
	ok;

terminate(Reason, #state{pos=Pos}=State) ->
	lager:error("NkBase vnode ~p terminate: ~p", [Pos, Reason]),
	case get_ref(leveldb, State) of
		{ok, Ref} ->
			case catch eleveldb:close(Ref) of
				{'EXIT', Error} ->lager:error("LevelDB close error: ~p", [Error]);
				_ -> ok
			end;
		_ ->
			ok
	end.


%% Optional Callback. A node is about to exit. Ensure that this node doesn't
%% have any current ensemble members.
ready_to_exit() ->
    [] =:= nkbase_ensemble:local_ensembles().


%% @private Called from riak core on forwarding state, after the handoff has been
%% completed, but before the new vnode is marked as the owner of the partition
set_vnode_forwarding(Forward, State) ->
    State#state{forward=Forward}.


%% @private
%% Internal messages having Sender=ignore should not get here
handle_overload_command(_Cmd, Sender, Idx) ->
    send_reply({error, overload}, Sender, Idx).


%% @private
handle_overload_info({ensemble_ping, _From}, _Idx) ->
    %% Don't respond to pings in overload
    ok;
handle_overload_info({ensemble_get, _, From}, _Idx) ->
    riak_ensemble_backend:reply(From, {error, overload});
handle_overload_info({raw_forward_get, _, From}, _Idx) ->
    riak_ensemble_backend:reply(From, {error, overload});
handle_overload_info({ensemble_put, _, _, From}, _Idx) ->
    riak_ensemble_backend:reply(From, {error, overload});
handle_overload_info({raw_forward_put, _, _, From}, _Idx) ->
    riak_ensemble_backend:reply(From, {error, overload});
handle_overload_info(_, _) ->
    ok.


%% Resizing callbacks

% %% callback used by dynamic ring sizing to determine where
% %% requests should be forwarded. Puts/deletes are forwarded
% %% during the operation, all other requests are not
% request_hash(?KV_PUT_REQ{bkey=BKey}) ->
%     riak_core_util:chash_key(BKey);
% request_hash(?KV_DELETE_REQ{bkey=BKey}) ->
%     riak_core_util:chash_key(BKey);
% request_hash(_Req) ->
%     % Do not forward other requests
%     undefined.
%
% nval_map(Ring) ->
%     riak_core_bucket:bucket_nval_map(Ring).
%
% %% @private
% object_info({Bucket, _Key}=BKey) ->
%     Hash = riak_core_util:chash_key(BKey),
%     {Bucket, Hash}.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @private
-spec leveldb_path(integer()) ->
	string().

leveldb_path(Pos) ->
	File = lists:flatten(io_lib:format("~4.10.0B", [Pos])),
	Path = filename:join(nkbase_app:get_dir(), File),
	ok = filelib:ensure_dir(Path),
	Path.

	
%% @private
-spec get_ref(nkbase:backend(), #state{}) ->
	{ok, nkbase_backend:db_ref()} | {error, {invalid_backend, term()}}.

get_ref(ets, #state{ets_ref=Ref}) -> 
	{ok, Ref};
get_ref(leveldb, #state{leveldb_ref=Ref}) when Ref/=undefined -> 
	{ok, Ref};
get_ref(Other, _) -> 
	{error, {invalid_backend, Other}}.


%% @private
-spec do_get(nkbase:backend(), nkbase:ext_key(), #state{}) ->
	{ok, dvvset:clock()} | {error, term()}.

do_get(Backend, ExtKey, State) ->
	case get_ref(Backend, State) of
		{ok, Ref} ->
			case catch nkbase_backend:get({Backend, Ref}, ExtKey) of
				{ok, DVV} -> {ok, DVV};
				{error, Error} -> {error, Error};
				{'EXIT', Error} -> {error, Error}
			end;
		{error, Error} ->
			{error, Error}
	end.


%% @private
-spec do_put(nkbase:backend(), nkbase:ext_key(), dvvset:clock(), nkbase:put_meta(),
			 #state{}) ->
	{ok, dvvset:clock()} | {error, term()}.

do_put(Backend, ExtKey, DVV, Meta, State) ->
	case get_ref(Backend, State) of
		{ok, Ref} ->
			case catch nkbase_backend:put({Backend, Ref}, ExtKey, DVV, Meta) of
				{ok, Indices, DVV1} -> 
					check_expire(Backend, ExtKey, Indices, State),
					{ok, DVV1};
				{error, Error} -> 
					{error, Error};
				{'EXIT', Error} -> 
					{error, Error}
			end;
		{error, Error} ->
			{error, Error}
	end.


%% @doc Reindexes a DVV
-spec do_reindex(nkbase:backend(), nkbase:ext_key(), [nkbase:index_spec()], #state{}) ->
	ok | {error, term()}.

do_reindex(Backend, ExtKey, Spec, State) ->
	case do_get(Backend, ExtKey, State) of
		{ok, DVV} ->
			DVV1 = dvvset:map(
				fun({Meta, Obj}) ->
					Indices = nkbase_util:expand_indices(Spec, ExtKey, Obj),
					{Meta#{idx=>Indices}, Obj}
				end,
				DVV),
			Meta = #{old_dvv=>DVV, actor=>ignore},
			case do_put(Backend, ExtKey, DVV1, Meta, State) of
				{ok, _} -> ok;
				{error, Error} -> {error, Error}
			end
	end.



%% @private
-spec do_del({nkbase:backend(), nkbase_backend:db_ref()},
	          nkbase:ext_key(), [nkbase:ext_obj()], boolean()) ->
	ok | {error, term()}.

do_del(BackRef, ExtKey, Values, InProcess) ->
	case [{sc, Obj} || {#{sc:=true}, Obj} <- Values] of
		[] ->
			case catch nkbase_backend:del(BackRef, ExtKey) of
				ok -> ok;
				{error, Error} -> {error, Error};
				{'EXIT', Error} -> {error, Error}
			end;
		[{sc, '$nkdeleted'}] ->
			ok;
		[{sc, _}] ->
			{D, C, K} = ExtKey,
			DelFun = fun() -> nkbase_sc:kdel(D, C, K, #{eseq=>overwrite}) end,
			case InProcess of
				true ->	spawn(DelFun);
				false -> DelFun()
			end,
			ok;
		_ ->
			{error, mixed_consistency}
	end.


%% @private
-spec reply(term(), #state{}) ->
	{reply, {vnode, integer(), atom(), term()}, #state{}}.

reply(Reply, #state{idx=Idx}=State) ->
	{reply, {vnode, Idx, node(), Reply}, State}.


%% @private
-spec send_reply(term(), sender(), chash:index_as_int()) ->
	any().

send_reply(Reply, Sender, Idx) ->
	riak_core_vnode:reply(Sender, {vnode, Idx, node(), Reply}).


%% @private Filter function for coverage requests
-spec get_key_filter(term(), #state{}) ->
	fun((binary()) -> boolean()).

get_key_filter(KeySpaces, #state{idx=Idx}) ->
	case nkbase_util:get_value(Idx, KeySpaces) of
		undefined ->
			fun(_Key) -> true end;
		FilterVNode ->
			{ok, Ring} = riak_core_ring_manager:get_my_ring(),
            fun({_Domain, Class, Key}) ->
            	ChashKey = chash:key_of({Class, Key}),
            	PrefListIndex = riak_core_ring:responsible_index(ChashKey, Ring),
            	lists:member(PrefListIndex, FilterVNode)
            end
    end.


%% @private
-spec check_expire(nkbase:backend(), nkbase:ext_key(), [{term(), term()}], #state{}) ->
	ok.

check_expire(Backend, ExtKey, Indices, State) ->
	case nkbase_util:get_value(?EXP_IDX, Indices) of
		undefined -> 
			ok;
		Exp -> 
			Now = nklib_util:l_timestamp(),
			update_expire_timer(Backend, ExtKey, Exp, Now, State)
	end.


%% @private 
-spec update_expire_timer(nkbase:backend(), nkbase:ext_key(), nklib_util:timestamp(),
				    	  nklib_util:timestamp(), #state{}) ->
	ok.


update_expire_timer(_Backend, _ExtKey, Exp, Now, #state{expire_check=ExpCheck}) 
		when (Exp-Now) div 1000000 > ExpCheck ->
		lager:debug("Delaying timer: ~p>~p", [(Exp-Now) div 1000000, ExpCheck]),
	ok;

update_expire_timer(Backend, ExtKey, Exp, Now, State) ->
	#state{expire_resolution=Resolution} = State,
	Slot = Exp div (1000*Resolution),
	case get_meta({exp, Backend, Slot}, [], State) of
		[] ->
			Time = (Exp - Now) div 1000,		% msecs
			lager:debug("Set new timer for ~p (~p): ~p", [Exp, Slot, Time]),
			Msg = {check_expire, Backend, Slot},
			riak_core_vnode:send_command_after(max(Time, 1), Msg),
			put_meta({exp, Backend, Slot}, [ExtKey], State);
		List ->
			case lists:member(ExtKey, List) of
				false ->
					put_meta({exp, Backend, Slot}, [ExtKey|List], State);
				true ->
					ok
			end
	end.


%% @private
-spec del_expired([nkbase:ext_key()], nkbase:backend(), pos_integer(), #state{}) ->
	ok.

del_expired([], _Backend, _Slot, _State) ->
	ok;

del_expired([ExtKey|Rest], Backend, Slot, State) ->
	#state{pos=Pos, expire_resolution=Resolution} = State,
	{ok, Ref} = get_ref(Backend, State),
	case catch nkbase_backend:get({Backend, Ref}, ExtKey) of
		{ok, DVV} ->
			Values = dvvset:values(DVV),
			Exp = lists:max([maps:get(exp, Meta, undefined) || {Meta, _} <- Values]),
			case is_integer(Exp) andalso Exp div (1000*Resolution) of
				Slot ->
					Op = do_del({Backend, Ref}, ExtKey, dvvset:values(DVV), true),
					lager:debug("Deleting ~p at ~p (~p): ~p", 
								 [ExtKey, Pos, Slot, Op]);
				_ ->
					% This object has no longer this expiration
					lager:debug("NOT deleting ~p at ~p (~p): skipping", 
		 					   [ExtKey, Pos, Slot])
			end;
		{error, not_found} ->
			ok;
		Error ->
			lager:info("NOT Deleting ~p at ~p (~p): ~p", 
					   [ExtKey, Pos, Slot, Error])
	end,
	del_expired(Rest, Backend, Slot, State).


%% @private
-spec find_all_expires(nkbase:backend(), nklib_util:l_timestamp(), #state{}) ->
	ok.

find_all_expires(Backend, Now, #state{pos=Pos, expire_check=ExpCheck}=State) ->
	{ok, DbRef} = get_ref(Backend, State),
	Fun = fun(Exp, {_, _, ExtKey}, [], _) -> 
		case (Exp - Now) div 1000000 > ExpCheck of
			true -> {done, ok};
			false -> {iter, update_expire_timer(Backend, ExtKey, Exp, Now, State)} 
		end
	end,
	Spec = #search_spec{
		backend = Backend,
		db_ref = DbRef,
		domain = '$nkbase',
		class = '$g',
		indices = [{exp, [{ge, Now}]}],
		order = asc,
		fold_fun = Fun,
		fold_acc = undefined
	},
	case nkbase_backend:search(Spec) of
		{done, _} -> 
			ok;
		{error, Error} ->
			lager:error("NkBASE vnode ~p error in find_expires: (~p)",
						[Pos, Error]),
			ok
	end.


%% @private
-spec del_all_expired(nkbase:backend(), nklib_util:l_timestamp(), #state{}) ->
	ok.

del_all_expired(Backend, Stop, #state{pos=Pos}=State) ->
	% Start checking up to ExpCheck seconds ago
	{ok, DbRef} = get_ref(Backend, State),
	Fun = fun(_Exp, {_, _, ExtKey}, Values, Acc) -> 
		Op = do_del({Backend, DbRef}, ExtKey, Values, true),
		lager:warning("Deleting LONG EXPIRED ~p at ~p: ~p", [ExtKey, Pos, Op]),
		{iter, Acc}
	end,
	Spec = #search_spec{
		backend = Backend,
		db_ref = DbRef,
		domain = '$nkbase',
		class = '$g',
		indices = [{exp, [{le, Stop}]}],
		order = asc,
		fold_type = values,
		fold_fun = Fun,
		fold_acc = []
	},
	case nkbase_backend:search(Spec) of
		{done, _} -> 
			ok;
		{error, Error} -> 
			lager:warning("NkBASE vnode ~p: error deleting expired: ~p", 
						  [Pos, Error]),
			error
	end.



%% @doc Gets external metadata from database
-spec get_meta(term(), term(), #state{}) ->
	term().

get_meta(Key, Default, #state{ets_ref=Ref}) ->
	nkbase_backend:get_meta({ets, Ref}, Key, Default).


%% @doc Puts external metadata in database
-spec put_meta(term(), term(), #state{}) ->
	ok.

put_meta(Key, Value, #state{ets_ref=Ref}) ->
	nkbase_backend:put_meta({ets, Ref}, Key, Value).


%% @doc Deletes a external metadata from database
-spec del_meta(term(), #state{}) ->
	ok.

del_meta(Key, #state{ets_ref=Ref}) ->
	nkbase_backend:del_meta({ets, Ref}, Key).


%% @private
throttle(Pid, Max) ->
	{_, Length} = erlang:process_info(Pid, message_queue_len),
	case Length > Max of
		true ->
			timer:sleep(10),
			throttle(Pid, Max);
		false ->
			ok
	end.



