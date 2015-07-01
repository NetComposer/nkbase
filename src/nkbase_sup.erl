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

%% @private NkBASE main OTP supervisor
-module(nkbase_sup).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-behaviour(supervisor).

-export([start_link/0, init/1]).
-export([start_vnode_get_fsm_sup/0, start_vnode_put_fsm_sup/0]).
-export([start_coverage_fsm_sup/0]).
-export([start_nkbase_searchs_sup/0, start_nkbase_lists_sup/0]).


%%====================================================================
%% External functions
%%====================================================================

start_link() ->
    Childs = [
		{nkbase_vnode_get_fsm_sup, 
			{?MODULE, start_vnode_get_fsm_sup, []},
			permanent,
			infinity,
			supervisor,
			[?MODULE]},	
		{nkbase_vnode_put_fsm_sup, 
			{?MODULE, start_vnode_put_fsm_sup, []},
			permanent,
			infinity,
			supervisor,
			[?MODULE]},	
		{nkbase_coverage_fsm_sup, 
			{?MODULE, start_coverage_fsm_sup, []},
			permanent,
			infinity,
			supervisor,
			[?MODULE]},
		{nkbase_searchs_sup, 
			{?MODULE, start_nkbase_searchs_sup, []},
			permanent,
			infinity,
			supervisor,
			[?MODULE]},	
		{nkbase_events,
			{gen_event, start_link, [{local, nkbase_events}]},
			permanent,
			brutal_kill,
			worker,
			dynamic},
		{nkbase_vnode_master,
            {riak_core_vnode_master, start_link, [nkbase_vnode]},
            permanent, 
			5000, 
			worker, 
			[riak_core_vnode_master]},
		{nkbase_ensemble,
            {nkbase_ensemble, start_link, []},
            permanent, 
			5000, 
			worker, 
			[nkbase_ensemble]}
    ],
    supervisor:start_link({local, ?MODULE}, ?MODULE, {{one_for_one, 3, 60}, Childs}).


init(ChildSpecs) ->
    {ok, ChildSpecs}.

start_vnode_get_fsm_sup() ->
	supervisor:start_link({local, nkbase_vnode_get_fsm_sup}, ?MODULE, 
		{{simple_one_for_one, 3, 60}, [
			{undefined,
				{nkbase_vnode_get_fsm, start_link, []},
				temporary,
				5000,
				worker,
				[nkbase_vnode_get_fsm]
		}]}).

start_vnode_put_fsm_sup() ->
	supervisor:start_link({local, nkbase_vnode_put_fsm_sup}, ?MODULE, 
		{{simple_one_for_one, 3, 60}, [
			{undefined,
				{nkbase_vnode_put_fsm, start_link, []},
				temporary,
				5000,
				worker,
				[nkbase_vnode_put_fsm]
		}]}).

start_coverage_fsm_sup() ->
	supervisor:start_link({local, nkbase_coverage_fsm_sup}, ?MODULE, 
		{{simple_one_for_one, 3, 60}, [
			{undefined,
				{riak_core_coverage_fsm, start_link, [nkbase_coverage_fsm]},
				temporary,
				5000,
				worker,
				[nkbase_coverage_fsm]
		}]}).


start_nkbase_searchs_sup() ->
	supervisor:start_link({local, nkbase_searchs_sup}, ?MODULE, 
		{{simple_one_for_one, 3, 60}, [
			{undefined,
				{nkbase_search, start_link, []},
				temporary,
				5000,
				worker,
				[nkbase_search]
		}]}).

start_nkbase_lists_sup() ->
	supervisor:start_link({local, nkbase_lists_sup}, ?MODULE, 
		{{simple_one_for_one, 3, 60}, [
			{undefined,
				{nkbase_list, start_link, []},
				temporary,
				5000,
				worker,
				[nkbase_list]
		}]}).



