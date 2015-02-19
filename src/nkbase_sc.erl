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

%% @doc Strong Consistency Client Module
%%
%% The only supported backend currently is 'leveldb', and it is shared
%% with the rest of nkbase access methods (using the functions in nkbase.erl
%% and nkbase_dmap.erl) so be use to use different domain/class. 
%% Memory backend is not supported as riak_ensemble uses persistent on disk.


-module(nkbase_sc).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').

-export([kget/3, kget/4, kput/4, kput/5, kdel/3, kdel/4]).
-export_type([eseq/0, kget_meta/0, kput_meta/0]).

-include("nkbase.hrl").
-include("../deps/riak_ensemble/include/riak_ensemble_types.hrl").


%% ===================================================================
%% Types
%% ===================================================================

-type eseq() :: integer().


%% Get Metadata. See kget/3,4 for details
-type kget_meta() ::	
	#{
		n => pos_integer(),
		hash => id | class,
		timeout => pos_integer(),
		get_fields => [term() | tuple()],
		get_indices => [nkbase:index_name()],
		read_repair => boolean()
	}.


%% Put Metadata. See kput/4,5 for details
-type kput_meta() ::
	#{
		n => pos_integer(),
		hash => id | class,
		eseq => new | eseq() | overwrite,
		indices => [nkbase:index_spec()],
		timeout => pos_integer(),
		pre_write_hook => nkbase:pre_write_fun(),
		post_write_hook => nkbase:post_write_fun()
	}.


%% ===================================================================
%% API
%% ===================================================================


%% @doc Same as kget(Domain, Class, Key).
-spec kget(nkbase:domain(), nkbase:class(), nkbase:key()) ->
	{ok, eseq(), nkbase:obj()} | {error, term()}.

kget(Domain, Class, Key) ->
	kget(Domain, Class, Key, #{}).


%% @doc Gets a key from the database, returning also the object's context,
-spec kget(nkbase:domain(), nkbase:class(), nkbase:key(), kget_meta()) ->
	{ok, eseq(), nkbase:obj()} | {error, term()}.

kget(Domain, Class, Key, Meta) ->
	Meta1 = nkbase:get_class(Domain, Class, Meta),
	Ensemble = get_ensemble(Class, Key, Meta1),
	Timeout = 1000*maps:get(timeout, Meta1, ?DEFAULT_TIMEOUT),
	ExtKey = {Domain, Class, Key},
	Opts = case Meta of
		#{read_repair:=true} -> [read_repair];
		_ -> []
	end,
	case riak_ensemble_client:kget(node(), Ensemble, ExtKey, Timeout, Opts) of
		{ok, #nk_ens{eseq=_ESeq, val=notfound}} -> {error, not_found};
		{ok, #nk_ens{eseq=ESeq, val={_Meta, Value}}} -> {ok, ESeq, Value};
		{error, Error} -> {error, Error}
	end.


%% @doc Same as kput(Domain, Class, Key, Data, #{})
-spec kput(nkbase:domain(), nkbase:class(), nkbase:key(), nkbase:obj()) ->
	{ok, eseq()} | {error, term()}.

kput(Domain, Class, Key, Obj) ->
	kput(Domain, Class, Key, Obj, #{}).


%% @doc Puts a new value in the database
-spec kput(nkbase:domain(), nkbase:class(), nkbase:key(), nkbase:obj(), kput_meta()) ->
	{ok, eseq()} | {error, term()}.

kput(Domain, Class, Key, Obj, Meta) ->
	try
		case Meta of
			#{backend:=leveldb} -> ok;
			#{backend:=_} -> throw(backend_not_supported);
			#{} -> ok
		end,
		if 
			Domain >= <<255>> -> throw(invalid_domain);
			Class >= <<255>> -> throw(invalid_class);
			Key >= <<255>> -> throw(invalid_key);
			true -> ok
		end,
		ExtKey = {Domain, Class, Key},
		{ExtObj, Meta1} = nkbase_cmds:make_ext_obj(ExtKey, Obj, Meta),
		{ExtKey2, ExtObj2, Meta2} = case maps:get(pre_write_hook, Meta1, undefined) of
			undefined -> {ExtKey, ExtObj, Meta1};
			PreFun when is_function(PreFun, 3) -> PreFun(ExtKey, ExtObj, Meta1)
		end,
		Ensemble = get_ensemble(element(2, ExtKey2), element(3, ExtKey2), Meta2),
		% lager:warning("ENS: ~p", [Ensemble]),
		Timeout = 1000*maps:get(timeout, Meta2, ?DEFAULT_TIMEOUT),
		Reply = case maps:get(eseq, Meta, new) of
			new ->
				riak_ensemble_client:kput_once(Ensemble, ExtKey2, ExtObj2, Timeout);
			overwrite ->
				riak_ensemble_client:kover(Ensemble, ExtKey2, ExtObj2, Timeout);
			ESeq when is_integer(ESeq) ->
				Old = #nk_ens{eseq=ESeq, key=ExtKey2},
				riak_ensemble_client:kupdate(Ensemble, ExtKey2, Old, ExtObj2, Timeout)
		end,
		case Reply of
			{ok, #nk_ens{eseq=NewESeq}} -> 
				case maps:get(post_write_hook, Meta2, undefined) of
					undefined -> 
						ok;
					PostFun when is_function(PostFun, 3) -> 
						PostFun(ExtKey2, ExtObj2, Meta2)
				end,
				{ok, NewESeq};
			{error, Error} -> 
				{error, Error}
		end
	catch
		throw:Throw -> {error, Throw}
	end.



%% @doc Same as kdel(Domain, Class, Key, Data, #{})
-spec kdel(nkbase:domain(), nkbase:class(), nkbase:key()) ->
	{ok, eseq()} | {error, term()}.

kdel(Domain, Class, Key) ->
	kdel(Domain, Class, Key, #{}).


%% @doc Puts a new value in the database
-spec kdel(nkbase:domain(), nkbase:class(), nkbase:key(), kput_meta()) ->
	{ok, eseq()} | {error, term()}.


kdel(Domain, Class, Key, Meta) ->
	Meta1 = nkbase:get_class(Domain, Class, Meta),
	Ensemble = get_ensemble(Class, Key, Meta1),
	Timeout = 1000*maps:get(timeout, Meta1, ?DEFAULT_TIMEOUT),
	ExtKey = {Domain, Class, Key},
	Reply = case maps:get(eseq, Meta, 0) of
		overwrite ->
			riak_ensemble_client:kdelete(Ensemble, ExtKey, Timeout);
		ESeq when is_integer(ESeq) ->
			Old = #nk_ens{eseq=ESeq, key=ExtKey},
			riak_ensemble_client:ksafe_delete(Ensemble, ExtKey, Old, Timeout)
	end,
	case Reply of
		{ok, #nk_ens{}} -> ok;
		{error, Error} -> {error, Error}
	end.


%% ===================================================================
%% Internal
%% ===================================================================


%% @private
-spec get_ensemble(nkbase:class(), nkbase:key(), nkbase:class_meta()) ->
	ensemble_id().

get_ensemble(Class, Key, Meta) ->
	N = maps:get(n, Meta, ?DEFAULT_N),
    CHashKey = case maps:get(hash, Meta, id) of
		id -> chash:key_of({Class, Key});
		class -> chash:key_of({Class, same_key})
	end,
	{ok, CHBin} = riak_core_ring_manager:get_chash_bin(),
	Idx = chashbin:responsible_index(CHashKey, CHBin),
	{nkv, Idx, N}.







