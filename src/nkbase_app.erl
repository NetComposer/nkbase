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

%% @doc NkBASE main OTP application module
-module(nkbase_app).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-behaviour(application).

-export([start/0, start/1, start/2, stop/1]).
-export([get_env/1, get_env/2, set_env/2, get_dir/0]).

-include("nkbase.hrl").


%% @doc Starts NkBASE stand alone.
-spec start() -> 
    ok | {error, Reason::term()}.

start() ->
    start(temporary).


%% @doc Starts NkBASE stand alone.
-spec start(permanent|transient|temporary) -> 
    ok | {error, Reason::term()}.

start(Type) ->
    nkdist_util:ensure_dir(),
    case nklib_util:ensure_all_started(nkbase, Type) of
        {ok, _Started} ->
		    riak_core:wait_for_service(nkbase),
            ok;
        Error ->
            Error
    end.

   
%% @doc OTP callback
start(_Type, _Args) ->
	case nkbase_sup:start_link() of
		{ok, Pid} ->
			riak_core:register(nkbase, [{vnode_module, nkbase_vnode}]),
			{ok, Vsn} = application:get_key(vsn),
			lager:info("NkBASE (version ~s) has started", [Vsn]),
			ExpireCheck = nkbase_app:get_env(expire_check, 60),
			ExpireResolution = nkbase_app:get_env(expire_resolution, 1000),
			lager:info("ExpCheck: ~p secs  Resolution: ~p msecs",
					   [ExpireCheck, ExpireResolution]),
			%% Force the creation of vnodes before waiting for 
			%% 'vnode_management_timer' time
			{ok, Ring} = riak_core_ring_manager:get_my_ring(),
			riak_core_ring_handler:ensure_vnodes_started(Ring),
			case init:get_argument(force_master) of
				{ok, [[]]} -> 
					lager:notice("MASTER is FORCED"),
					nkbase_ensemble:enable();
				_ -> 
					ok
			end,
			{ok, Pid};
		{error, Error} ->
			{error, Error}
	end.
 
%% @doc OTP stop
stop(_) ->
    ok.


%% @doc Gets a key from the application config with a default
-spec get_env(term()) ->
	term() | undefined.

get_env(Key) ->
	get_env(Key, undefined).


%% @doc Gets a key from the application config
-spec get_env(term(), term()) ->
	term().

get_env(Key, Default) ->
	case application:get_env(nkbase, Key) of
		{ok, Value} -> Value;
		_ -> Default
	end.


%% @doc Stores a key in the application config
-spec set_env(term(), term()) ->
	ok.

set_env(Key, Value) ->
	ok = application:set_env(nkbase, Key, Value).


%% @doc Gets the data dir
-spec get_dir() ->
	string().

get_dir() ->
	{ok, Base} = application:get_env(riak_core, platform_data_dir),
	filename:join(Base, "nkbase_store").


