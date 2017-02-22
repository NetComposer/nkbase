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

%% @private Worker module for vnode
-module(nkbase_vnode_worker).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').

-behaviour(riak_core_vnode_worker).

-export([init_worker/3, handle_work/3]).

-include("nkbase.hrl").

-record(state, {
	idx :: chash:index_as_int(),
	pos :: pos_integer()
}).

%% ===================================================================
%% Public API
%% ===================================================================

%% @private
init_worker(VNodeIndex, [Pos], _Props) ->
    {ok, #state{idx=VNodeIndex, pos=Pos}}.


%% @private
handle_work({fold_domains, Spec}, Sender, State) ->
	Result = nkbase_backend:fold_domains(Spec),
	reply(Result, Sender, State);

handle_work({fold_classes, Spec}, Sender, State) ->
	Result = nkbase_backend:fold_classes(Spec),
	reply(Result, Sender, State);

handle_work({fold_objs, Spec}, Sender, State) ->
	Result = nkbase_backend:fold_objs(Spec),
	reply(Result, Sender, State);

handle_work({search, Spec}, Sender, State) ->
	case Spec#search_spec.indices of
		[] -> 
			reply({error, no_indices}, Sender, State);
		_ ->
			Result = nkbase_backend:search(Spec),
			reply(Result, Sender, State)
	end;

handle_work({handoff, Fun, Acc, Refs}, Sender, State) ->
	Result = do_handoff(Refs, Fun, Acc),
	riak_core_vnode:reply(Sender, Result),
	{noreply, State}.



%%%===================================================================
%%% Internal
%%%===================================================================


%% @private
reply(Result, Sender, #state{idx=Idx}=State) ->
    riak_core_vnode:reply(Sender, {vnode, Idx, node(), Result}),
    {noreply, State}.



%% @private
do_handoff([], _Fun, Acc) ->
	Acc;

do_handoff([{Backend, DbRef}|Rest], Fun, Acc) ->
	Fun1 = fun(ExtKey, [], Acc1) ->
		case nkbase_backend:get({Backend, DbRef}, ExtKey) of
			{ok, DVV} -> Fun({Backend, ExtKey}, DVV, Acc1);
			{error, _Error} -> Acc1
		end
	end,
	Spec = #fold_spec{
		backend = Backend,
		db_ref = DbRef,
		fold_fun = Fun1,
		fold_acc = Acc,
		max_items = undefined
	},
	case nkbase_backend:fold_objs(Spec) of
		{done, Acc2} -> 
			do_handoff(Rest, Fun, Acc2);
		{error, Error} ->
			{error, Error}
	end.



