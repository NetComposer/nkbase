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

-module(nkbase_events).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').

-behaviour(gen_event).

-export([add_handler/2, add_sup_handler/2, add_guarded_handler/2,
         add_callback/1, add_sup_callback/1, add_guarded_callback/1]).

%% gen_event callbacks
-export([init/1, handle_event/2, handle_call/2, handle_info/2, terminate/2, code_change/3]).
-export([notify/1]).

-include("nkbase.hrl").

-record(state, {
    callback 
}).

%% ===================================================================
%% API functions
%% ===================================================================

add_handler(Handler, Args) ->
    gen_event:add_handler(?MODULE, Handler, Args).

add_sup_handler(Handler, Args) ->
    gen_event:add_sup_handler(?MODULE, Handler, Args).

add_guarded_handler(Handler, Args) ->
    riak_core:add_guarded_event_handler(?MODULE, Handler, Args).

% Adds a new handler using this same module as callback
add_callback(Fn) when is_function(Fn) ->
    gen_event:add_handler(?MODULE, {?MODULE, make_ref()}, [Fn]).

add_sup_callback(Fn) when is_function(Fn) ->
    gen_event:add_sup_handler(?MODULE, {?MODULE, make_ref()}, [Fn]).

add_guarded_callback(Fn) when is_function(Fn) ->
    riak_core:add_guarded_event_handler(?MODULE, {?MODULE, make_ref()}, [Fn]).

notify(Msg) ->
    gen_event:notify(?MODULE, Msg).



%% ===================================================================
%% gen_event callbacks
%% ===================================================================

init([Fn]) ->
    %% Get the initial list of available services
    Fn(riak_core_node_watcher:services()),
    {ok, #state { callback = Fn }}.

handle_event(Msg, State) ->
    (State#state.callback)(Msg),
    {ok, State}.

handle_call(_Request, State) ->
    {ok, ok, State}.

handle_info(_Info, State) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


% %%====================================================================
% %% External functions
% %%====================================================================
