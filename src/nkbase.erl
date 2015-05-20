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

%% @doc Main User Module
-module(nkbase).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').

-export([register_class/3, unregister_class/2]).
-export([get_classes/0, get_classes/1, get_class/2, get_class/3]).
-export([get/3, get/4, put/4, put/5]).
-export([del/3, del/4, remove_all/1, remove_all/2, remove_all/3]).
-export([list_domains/0, list_domains/1, list_classes/1, list_classes/2]).
-export([list_keys/2, list_keys/3, iter_keys/7, iter_objs/7]).
-export([search/3, search/4]).
-export([reindex/2, reindex/3]).
-export([make_ext_obj/3]).
-export_type([backend/0, domain/0, class/0, key/0, ext_key/0]).
-export_type([ctx/0, obj/0, reply/0, obj_meta/0, get_spec/0]).
-export_type([index_name/0, index_spec/0]).
-export_type([class_meta/0, get_meta/0, put_meta/0, scan_meta/0, search_meta/0]).
-export_type([ext_obj/0, reconcile/0]).
-export_type([search_filter/0, search_spec/0]).

-include("nkbase.hrl").


%% ===================================================================
%% Types
%% ===================================================================

%% Availabel backends
-type backend() :: ets | leveldb.

%% Domain name
-type domain() :: term().

%% Class name
-type class() :: term().

%% Key specificarion
-type key() :: term().

%% Extended Key
-type ext_key() :: {domain(), class(), key()}.

%% Context
-type ctx() :: dvvset:vector().

%% Object specification
-type obj() :: term().

%% Replied object
-type reply() :: obj() | '$nkdeleted' |
	#{
		fields => #{ term() => term() },
		indices => #{ index_name() => [term()]}
	}.

%% Object metadata
-type obj_meta() :: 
	#{
		time => nkbase_util:l_timestamp(),
		idx => [{index_name(), term()}],
		exp => nkbase_util:l_timestamp(),
		eseq => nkbase_sc:eseq()
	}.

%% Index Name
-type index_name() :: term().


%% Index specification
%%
%% When objects are stored using put/4,5 you can describe (in the Meta option
%% or in the class specification) any number of indices that will be stored 
%% along with the object.
%%
%% These indices can be static, or dinamically computed based on the 
%% current object (only if it is an erlang map(), proplist() or nkbase dmap())
%%
%% For the value of each index, you can provide:
%% - The object's key (so you can use the key in seach calls, ranges, etc.)
%% - Any erlang term or list of terms
%% - If the object's data is a proplist(), map() or dmap(), a field of that object,
%%   that can be any erlang term o list of terms
%% - A function that will be called with the object and must return any
%%   erlang term or list of terms.
%% 
%% If you return a list of terms, each one will be inserted as a different 
%% index value.
%%
%% If you use the 'normalize' option, value will be normalized 
%% (see nkbase_util:normalize/1). If you use the 'words' option, the value will be
%% splitted into different values, using spaces as separators.

-type index_spec_type() :: 
	key | {field, term()|tuple()} | {func, fun((ext_key(), obj()) -> term()|[term()])} |
	term() | [term()].

-type index_spec_opts() :: 
	normalize | words.

-type index_spec() :: 
	{index_name(), index_spec_type()} | 
	{index_name(), index_spec_type(), [index_spec_opts()]}.


%% Search Spec
%% 
%% You can specify a simple search specification (with a single index)
%% or a complex one (with multiple indices). The server iterates only over the
%% first index, in ascending or descending order.
%%
%% For each index, you can specify a list of conditions. Objects fitting ANY of
%% these conditions will be returned. You can describe this conditions in two
%% different ways: as a filter or list of filters (i.e. [{eq, 5}, {ge, 10}]) or
%% with a string() or binary() that implements a special query language:
%%
%% Filter0 [| Filter1] [| Filter2] ...
%% FilterN = Value | > Value | >= Value | < Value | <= Value | <> Value | 
%% 			 * | *Value | Value* | *Value* | Value1 - Value2 | 
%% 			 re(<RegularExpression>)
%%
%% The query language can only be used if the indices are binaries. If you used
%% the option 'normalize' when creating the index, must also normalize the 
%% values before using them (calling nkbase_util:normalize/1)
%%
%% Filters using regular expressions {re, ..}, "*Value" and "re(..)" are 
%% very inefficient, since they must iterate over all objects
%%
%% If secondary indices are provided, the iterator must then read every object
%% (that fits first index) to find out if it fits with all seconday indices also. 
%% Only objects fitting ALL indices will be returned. Returned objects will be 
%% always sorted on the first index.
%%

-type search_filter() ::
	all | {eq, term()} | {ne, term()} | {lt, term()} | {le, term()} |
	{gt, term()} | {ge, term()} | {range, term(), term()} | {re, binary()}.

-type search_spec() ::
	{index_name(), search_filter() | [search_filter()] | string() | binary()}.
	

%% Pre and port write funs
-type pre_write_fun() ::
	fun((ext_key(), ext_obj(), put_meta()) -> {ext_key(), ext_obj(), put_meta()}).		

-type post_write_fun() ::
	fun((ext_key(), ext_obj(), put_meta()) -> ok).


%% Class Metadata
%%
%% vsn: 		Class will only be updated if Vsn > than stored class.
%% alias: 		Class will use the metadata from another class (or other fields will
%%				will ignored).
%% backend: 	Backend to use (default: leveldb).
%% sc:			If strong consistency is used (default: false)
%%				If true, get, put and del functions cannot be used, the versions
%%				in nkbase_sc module should be used instead.
%%				Implies backend=>leveldb.
%% indices: 	Secondary indices to add to the object, so that it will be 
%% 				possible to search over them later on.
%% n: 			Number of copies of the object to store (default: 3).
%% r: 			Number of nodes that must reply to send a read response (default: 3).
%% w: 			Number of nodes to agree to return a valid put response (default: 3).
%% hash: 		If 'id', objects will be spread among all nodes. If 'class', all 
%%       		the objects with the same class will go to the same N nodes.
%%       		(default: id)
%% reconcile: 	See description in get/4 and put/5
%% ttl: 		Time to live (seconds, see put/5)
%% timeout: 	Time before timeout (secs, default: 30).
%% pre_write_hook:  Function to be called before writing any object. Can update the
%%					object, or abort the put operation.
%% post_write_hook: Function to be called after writing the object.
%%.
-type class_meta() :: 
	#{
		vsn => integer(),
		alias => {domain(), class()},
		backend => backend(),
		sc => boolean(),
		indices => [index_spec()],
		n => pos_integer(),
		r => pos_integer(),
		w => pos_integer(),
		hash => id | class,
		reconcile => reconcile(),
		ttl => pos_integer(),
		timeout => pos_integer(),
		pre_write_hook => pre_write_fun(),
		post_write_hook => post_write_fun()
	}.
	

%% Reconcile funtion
%%
%% If you provide a reconcile function, it will be called in case of
%% conflict with the list of conflicting "extended objects",
%% and you must select one of them, looking at the second element of
%% each tuple, that is the current object.
%%
%% You could also generate a new object from the conflicting ones, but,
%% in that case, you must generate a new "extended object", calling
%% make_ext_obj/3, and supply the necessary metadata to generate indices, etc.,
%% in case it is not defined in the object's class
%%
-type reconcile() :: undefined | lww | fun((ext_key(), [ext_obj()]) -> ext_obj()).


%% Get Metadata. See get/3,4 for details
-type get_meta() ::	
	#{
		backend => backend(),
		n => pos_integer(),
		r => pos_integer(),
		hash => id | class,
		reconcile => reconcile(),
		timeout => pos_integer(),
		get_fields => [term() | tuple()],
		get_indices => [index_name()]
	}.


%% Put Metadata. See put/4,5 for details
-type put_meta() ::
	#{
		backend => backend(),
		indices => [index_spec()],
		n => pos_integer(),
		w => pos_integer(),
		hash => id | class,
		reconcile => reconcile(),
		ttl => pos_integer(),
		timeout => pos_integer(),
		pre_write_hook => pre_write_fun(),
		post_write_hook => post_write_fun(),
		context => ctx()
	}.


%% Scan Metadata:
-type scan_meta() :: 
	#{
		backend => backend(),
		n => pos_integer(),
		timeout => pos_integer(),
		start => key(),					% Key to start (default: first key)
		stop =>  key(),					% Key to stop (default: last key)
		page_size => pos_integer(),		% 
		filter_deleted => boolean()		% Filter deleted objects
	}.


%% Search Metadata
-type search_meta() ::
	#{
		backend => backend(),
		n => pos_integer(),
		timeout => pos_integer(),
		order => asc | desc,
		page_size => pos_integer(),
		next => {term(), key()},
		get_values => boolean(),
		get_fields => [term() | tuple()],
		get_indices => [index_name()]
	}.

%% Get fields specification
-type get_spec() ::
	#{
		get_fields => [term()],
		get_indices => [term()]
	}.


%% Extended object
-type ext_obj() :: 
	{obj_meta(), obj() | '$nkdeleted'}.



%% ===================================================================
%% API
%% ===================================================================


%%% @doc Gets all registered classes at the server
-spec get_classes() ->
	[{domain(), class()}].

get_classes() ->
	[{Domain, Name} || {{Domain, Name}, Meta} 
		<- riak_core_metadata:to_list(?CLASS_PREFIX),
		   not lists:member('$deleted', Meta)].


%%% @doc Gets all registered classes at the server for a domain
-spec get_classes(domain()) ->
	[class()].

get_classes(Domain) ->
	[Name || {Domain1, Name} <- get_classes(), Domain1==Domain].


%% @doc Gets the indicated class's definition
%% If the class is an alias for other class, the final specification will be returned
-spec get_class(domain(), class()) ->
	class_meta() | #{}.

get_class(Domain, Name) ->
	case riak_core_metadata:get(?CLASS_PREFIX, {Domain, Name}) of
		#{alias:=Alias}=Class ->
			Alias1 = case Alias of
				{_, _} -> Alias;
				_ -> {Domain, Alias}
			end,
			Vsn = maps:get(vsn, Class, -1),
			case riak_core_metadata:get(?CLASS_PREFIX, Alias1) of
				#{} = Aliased -> Aliased#{vsn=>Vsn, alias=>Alias};
				undefined -> #{vsn=>Vsn, alias=>Alias}
			end;
		#{} = Value ->
			Value;
		undefined ->
			#{}
	end.


%% @doc Gets the final version of a class using the optionally stored values
%% and updating them with the new suplied values
-spec get_class(domain(), class(), class_meta()) ->
	class_meta().

get_class(Domain, Name, Update) when is_map(Update) ->
	case get_class(Domain, Name) of
		Map when map_size(Map)==0 -> 
			Update;
		Base -> 
			case map_size(Update)==0 of
				true -> Base;
				false -> maps:merge(Base, Update)
			end
	end.


%% @doc Register a new class in the ring
%%
%% Register a data class with the server. See the valid parameters above.
%% The class will be stored in the riak_core ring, 
%% so it will be copied to all nodes using the "gossip" protocol, 
%% and save to disk in each node's ring file
%%
%% If a 'vsn' key is used, the class will be updated only if the version is > than
%% the current stored version. If an  alias key is used, this class will be only 
%% an alias for an existing class and the rest of keys will be ignored
%%
-spec register_class(domain(), class(), map()) ->
	ok | not_updated.

register_class(Domain, Name, Data) when is_map(Data) ->
	case riak_core_metadata:get(?CLASS_PREFIX, {Domain, Name}) of
		#{vsn:=OldVsn} -> ok;
		_ -> OldVsn = -2
	end,
	case Data of
		#{vsn:=NewVsn} -> ok;
		_ -> NewVsn = -1
	end,
	case NewVsn > OldVsn of
		true ->
			ok = riak_core_metadata:put(?CLASS_PREFIX, {Domain, Name}, Data, []);
		false ->
			not_updated
	end.


%% @doc Unregister a new class in the ring
-spec unregister_class(domain(), class()) ->
	ok.

unregister_class(Domain, Name) ->
	ok = riak_core_metadata:delete(?CLASS_PREFIX, {Domain, Name}).


%% @doc Same as get(Domain, Class, Key).
-spec get(domain(), class(), key()) ->
	{ok, ctx(), reply()} | {deleted, ctx()} | {multi, ctx(), [reply()]} |
	{error, term()}.

get(Domain, Class, Key) ->
	get(Domain, Class, Key, #{}).


%% @doc Gets a key from the database, returning also the object's context,
%% that must be used to update or delete the object.
%%
%% Options in Meta override options in the stored class, if present.
%%
%% If the objects is deleted (but not yet removed) the context will also be returned.
%%
%% The server will send the read request to 'n' nodes, and will then wait for 'r' nodes
%% to respond. If any vnode sends an error, the full operation is aborted. 
%% If any one sends "not found" it is ignored. If a non-conflicting value emerges,
%% it is returned, along with its context. 
%%
%% If any conflict is detected (because the stored object in any of the vnodes 
%% had several values, or because some vnodes sent different, conflicting
%% values), the result will depend on the 'reconcile' option:
%%
%% - undefined: The server will return {multi, [Objs]}, with all stored objects.
%%  (default)   You can resolve the conflict and use the supplied context to update 
%%				the object. If there are deleted objects, they will be represented as
%				'$nkdeleted'
%% - lww:       The server will return only the most recent obj, and will also
%%			    overwrite that value in all vnodes to resolve the conflict.
%% - fun():     If you supply a function, it will be called with all conflicting 
%%				objects, and you must return the right object or a new one. This
%%				will be the returned value, and the server will also overwrite this 
%%				value in all vnodes to resolve the conflict
%%
%% The server then waits for the remaining (up to 'n') nodes to respond, 
%% reconciles again, and updates the 'winning' value in all vnodes back (read repair).
%%
%% This function will normally return the full object. If you use the fields or/and 
%% indices metadata keys, it will return instead a map with the indicated fields 
%% and indices instead (see reply() type). The object must be an erlang map(), 
%% a proplist() or a dmap() for these to work. You can access 'nested' objects 
%% using the form '{field1, field_1.1, ...}'. If the field is not found, 
%% '$undefined' will be returned as value. If the object has been deleted, 
%% '$nkdeleted' will be returned instead of the map.

-spec get(domain(), class(), key(), get_meta()) ->
	{ok, ctx(), reply()} | {deleted, ctx()} | {multi, ctx(), [reply()]} |
	{error, term()}.

get(Domain, Class, Key, Meta) ->
	case get_class(Domain, Class, Meta) of
		#{strong_consistency:=true} ->
			{error, strong_consistency};
		Meta1 ->
			case nkbase_cmds:get({Domain, Class, Key}, Meta1) of
				{maps, {Ctx, ['$nkdeleted']}} -> {deleted, Ctx};
				{maps, {Ctx, [Map]}} -> {ok, Ctx, Map};
				{maps, {Ctx, Maps}} -> {multi, Ctx, Maps};
				{values, {Ctx, ['$nkdeleted']}} -> {deleted, Ctx};
				{values, {Ctx, [Obj]}} -> {ok, Ctx, Obj};
				{values, {Ctx, Objs}} -> {multi, Ctx, Objs};
				{error, Error} -> {error, Error}
		 	end
	end.


%% @doc Same as put(Domain, Class, Key, Data, #{})
-spec put(domain(), class(), key(), obj()) ->
	ok | {error, term()}.

put(Domain, Class, Key, Object) ->
	put(Domain, Class, Key, Object, #{}).


%% @doc Puts a new value in the database
%%
%% Options in Meta override options in the stored class, if present.
%%
%% If you are updating a possibly existing object, its context must be included,
%% otherwise a conflict will occur.
%%
%% The object will be sent to 'n' nodes, and the server will wait for 'w' to answer.
%%
%% If the new object conflicts with any stored one, at any of the vnodes, 
%% the result will depend on the 'reconcile' parameter:
%%
%% - undefined: the server will store both objects. Later on, when you read it,
%%   (default)  both values will be returned, along with the righ context to 
%%				resolve the conflict and save a new value.
%% - lww: 		the last stored object will win. Please notice that the "looser" 
%%				client will receive an 'ok', so it will not know that its value 
%%				has been lost.
%% - fun():     if you supply a function, it will be called with all the conflicting
%%			    objects, and you must select one of them or create a new one.
%% 
%% Please notice that an 'ok' response means that all the requested vnodes 
%% ('w' parameter) have received and stored the object. It does not mean that
%% there is no conflict. An error response means that some of the vnodes have 
%% failed to store the request.
%%
%% If you supply a 'ttl' value (in seconds) the object will be fully removed
%% (not only deleted) automatically after this time (in seconds) at all vnodes.
%% In case of conflicting values with different ttls, only the last timed
%% ttl value applies. This also means that if any of the conflicting values
%% has no ttl, no automatic remove is performed.
%%
%% You can also supply any number of secondary indices, and they will be 
%% stored along with the object, so you can search on them later on
%% (see index_spec() type).
%%
-spec put(domain(), class(), key(), obj(), put_meta()) ->
	ok | {error, term()}.


put(_, _, _, _, #{indices:=Indices, reconcile:=Fun}) 
		when Indices/=[], is_function(Fun) ->
	{error, indices_not_allowed_with_reconcile};

put(Domain, Class, Key, Obj, Meta) ->
	ExtKey = {Domain, Class, Key},
	{ExtObj, Meta1} = nkbase_cmds:make_ext_obj(ExtKey, Obj, Meta),
	case Meta1 of
		#{strong_consistency:=true} ->
			{error, strong_consistency};
		Meta1 ->
			nkbase_cmds:put(ExtKey, ExtObj, Meta1)
	end.


%% @doc Same as del(Domain, Class, Key, #{})
-spec del(domain(), class(), key()) ->
	ok | {error, term()}.

del(Domain, Class, Key) ->
	del(Domain, Class, Key, #{}).


%% @doc Marks and object as 'deleted' from the database
%%
%% Options in Meta override options in the stored class, if present.
%%
%% The server does not perform any real remove after calling this function.
%% When an object is fully removed, its context is also lost, and it can be very
%% difficult to deal with some cases, like deleted objects 'resurrecting' 
%% after a network partition.
%%
%% When you request to delete and object using this function, the server 
%% performs a standard 'put', but marking the object as 'deleted', and 
%% removing all indices. The server also includes a default ttl (3600 secs)
%% if you don't supply any. After that time, the object will be fully removed.
%% Use ttl=>0 to deactivate this behaviour.
%%
%% You MUST either include a valid context or use a reconcile method in order
%% to avoid conflicts. Since a 'delete' is only a 'put' without body or indices,
%% the same conflict explanation for put/5 applies here.
%%
%% As with put, the longest ttl applies. If any of the conflicting objects has
%% no ttl, no auto removal will occur until the conflict is resolved.
%%
%% Please notice that an 'ok' response means that all the requested vnodes 
%% ('w' parameter) have received and stored the 'deleted' version object. 
%% It does not mean that there is no conflict. An error response means that 
%% some of the vnodes have  failed to store the request.
%%
-spec del(domain(), class(), key(), put_meta()) ->
	ok | {error, term()}.

del(Domain, Class, Key, Meta)->
	%% do_make_ext_obj/3 will deleted indices and add a default ttl
	put(Domain, Class, Key, '$nkdeleted', Meta).



%% @doc Similar to list_domains(#{})
-spec list_domains() ->
	{ok, [domain()]} | {error, term()}.

list_domains() ->
	list_domains(#{}).


%% @doc Performs a scan to find all domains used in objects in a backend.
%%
%% The recognized options are only 'backend' and 'timeout'.
%%
-spec list_domains(#{backend=>backend(), timeout=>pos_integer()}) ->
	{ok, [domain()]} | {error, term()}.

list_domains(Meta) ->
	Meta1 = Meta#{n=>1, page_size=>1000000},
	FunA = fun({Domain, _, _}, [], Acc) -> [Domain|Acc] end,
	FunB = fun(List, Acc) -> [List|Acc] end,
	case nkbase_cmds:fold(none, none, Meta1, domains, FunA, [], FunB, []) of
		{ok, DeepList} -> {ok, lists:usort(lists:flatten(DeepList))};
		{error, Error} -> {error, Error}
	end.

	
%% @doc Similar to list_classes(Domain, #{})
-spec list_classes(domain()) ->
	{ok, [class()]} | {error, term()}.

list_classes(Domain) ->
	list_classes(Domain, #{}).


%% @doc Performs a scan to find all classes belonging to a domain used 
%% in objects in a backend.
%%
%% The recognized options are only 'backend' and 'timeout'.
%%
-spec list_classes(domain(), #{backend=>backend(), timeout=>pos_integer()}) ->
	{ok, [class()]}.

list_classes(Domain, Meta) ->
	Meta1 = Meta#{n=>1, page_size=>1000000},
	FunA = fun({_D, Class, _}, [], Acc) -> [Class|Acc] end,
	FunB = fun(List, Acc) -> [List|Acc] end,
	case nkbase_cmds:fold(Domain, none, Meta1, classes, FunA, [], FunB, []) of
		{ok, DeepList} -> {ok, lists:usort(lists:flatten(DeepList))};
		{error, Error} -> {error, Error}
	end.


%% @doc Similar to list_keys(Domain, Class, #{})
-spec list_keys(domain(), class()) ->
	{ok, [key()]} | {error, term()}.

list_keys(Domain, Class) ->
	list_keys(Domain, Class, #{}).


%% @doc Scan keys in database belonging to a domain and class.
%%
%% Options in Meta override options in the stored class, if present.
%% See type scan_meta() type for options.
%%
%% You can specify a page_size, and start a new list keys operation from the
%% last retrieved key
%%
%% By default, deleted but not yet removed objects will be found. You cas use
%% the option 'filter_deleted', and they will be filtered, but it will be 
%% slower since each object has to be read from the backend
%%
%% The key list will be sorted.
%%
-spec list_keys(domain(), class(), scan_meta()) ->
	{ok, [key()]} | {error, term()}.

list_keys(Domain, Class, Meta) ->
	FunA = fun(Key, Acc) -> [Key|Acc] end,
	AccA = [],
	FunB = fun(List, Acc) -> ordsets:union(Acc, lists:reverse(List)) end, 
	AccB = ordsets:new(),
	Meta1 = get_class(Domain, Class, Meta),
	PageSize = maps:get(page_size, Meta1, ?DEFAULT_PAGE_SIZE),
	case iter_keys(Domain, Class, Meta, FunA, AccA, FunB, AccB) of
		{ok, List} when length(List) > PageSize ->
			{ok, lists:sublist(List, PageSize)};
		{ok, List} ->
			{ok, List};
		{error, Error} ->
			{error, Error}
	end.
	

%% @doc Similar to search(Domain, Class, SearchSpec, #{})
-spec search(domain(), class(), search_spec()|[search_spec()]) ->
	{ok, [reply()]} | {error, term()}.

search(Domain, Class, SearchSpec) ->
	search(Domain, Class, SearchSpec, #{}).
	

%% @doc Search over the indices in the database
%% 
%% Options in Meta override options in the stored class, if present.
%% See search_meta() and search_spec() for additional options.
%%
%% You can specify the iteration order, and a maximum page_size.
%% You can then use the 'next' parameter to continue the search on this
%% index value and key
%%
-spec search(domain(), class(), search_spec()|[search_spec()], search_meta()) -> 
	{ok, [reply()]} | {error, term()}.

search(Domain, Class, Indices, Meta) ->
	Indices1 = nkbase_search:expand_search_spec(Indices),
	nkbase_cmds:search(Domain, Class, Meta, Indices1).



%% @doc Deletes all objects for the specified backend.
%% Return ok only after all vnodes have deleted their objects.
-spec remove_all(#{backend=>backend(), timeout=>pos_integer()}) ->
	ok | {error, term()}.

remove_all(Meta) ->
	Backend = maps:get(backend, Meta, ?DEFAULT_BACKEND),
	nkbase_cmds:cmd_all({remove_all, Backend}, Meta).


%% @doc Deletes all objects for the specified domain and backend.
%% Return ok only after all vnodes have deleted their objects.
-spec remove_all(domain(), #{backend=>backend(), timeout=>pos_integer()}) ->
	ok | {error, term()}.

remove_all(Domain, Meta) ->
	Backend = maps:get(backend, Meta, ?DEFAULT_BACKEND),
	nkbase_cmds:cmd_all({remove_all, Backend, Domain, '$nk_all'}, Meta).


%% @doc Deletes all objects for the specified domain, class and backend.
%% Options in Meta override options in the stored class, if present.
%% Return ok only after all vnodes have deleted their objects.
-spec remove_all(domain(), class(), #{backend=>backend(), timeout=>pos_integer()}) ->
	ok | {error, term()}.

remove_all(Domain, Class, Meta) ->
	Meta1 = get_class(Domain, Class, Meta),
	Backend = maps:get(backend, Meta1, ?DEFAULT_BACKEND),
	nkbase_cmds:cmd_all({remove_all, Backend, Domain, Class}, Meta1).


%% @doc Similar to reindex(Domain, Class, #{})
-spec reindex(domain(), class()) ->
	ok | {error, term()}.

reindex(Domain, Class) ->
	reindex(Domain, Class, #{}).


%% @doc Forces reindexation of a objects belonging to a domain and class.
%% Return ok only after all vnodes have reindexed their objects.
-spec reindex(domain(), class(), class_meta()) ->
	ok | {error, term()}.

reindex(Domain, Class, Meta) ->
	Meta1 = get_class(Domain, Class, Meta),
	Backend = maps:get(backend, Meta1, ?DEFAULT_BACKEND),
	Spec = maps:get(indices, Meta1, []),
	nkbase_cmds:cmd_all({reindex, Backend, Domain, Class, Spec}, Meta1).


%% @doc Iterates over the keys in database.
%%
%% Options in Meta override options in the stored class, if present.
%% See type scan_meta() type for options.
%%
%% FunA will be called in each vnode for each key, using AccA0 as the initial
%% accumulator.
%% FunB will be called in the coordinating node for each vnode result,
%% receiving the last return value of FunA, and using AccB0 as initial accumulator.
%% The last return from FunB will be the final result.
%%
-spec iter_keys(domain(), class(), scan_meta(), 
			    fun((key(), AccA::term()) -> AccA::term()), AccA0::term(),
		   		fun((AccA::term(), AccB::term()) -> AccB::term()), AccB0::term()) ->
	{ok, AccB::term()} | {error, term()}.

iter_keys(Domain, Class, #{filter_deleted:=true}=Meta, FunA, AccA0, FunB, AccB0) ->
	FunA1 = fun(Key, Values, Acc) -> 
		case [Value || Value <- Values, Value/='$nkdeleted'] of
			[] -> Acc;
			_ -> FunA(Key, Acc)
		end
	end,
	iter_objs(Domain, Class, Meta, FunA1, AccA0, FunB, AccB0);

iter_keys(Domain, Class, Meta, FunA, AccA0, FunB, AccB0) ->
	Meta1 = get_class(Domain, Class, Meta),
	FunA1 = fun({_Domain, _Class, Key}, [], Acc) -> FunA(Key, Acc) end,
	nkbase_cmds:fold(Domain, Class, Meta1, keys, FunA1, AccA0, FunB, AccB0).


%% @doc Iterates over the objects in database.
%%
%% Options in Meta override options in the stored class, if present.
%% See type scan_meta() type for options.
%%
%% FunA will be called in each vnode for each list of object stored for a key, 
%% using AccA0 as the initial accumulator.
%% FunB will be called in the coordinating node for each vnode result,
%% receiving the last return value of FunA and using AccB0 as initial accumulator.
%% The last return from FunB will be the final result.
%%
-spec iter_objs(domain(), class(), scan_meta(),
			    fun((key(), [obj()], AccA::term()) -> AccA::term()), AccA0::term(),
		   		fun((AccA::term(), AccB::term()) -> AccB::term()), AccB0::term()) ->
	{ok, AccB::term()} | {error, term()}.

iter_objs(Domain, Class, Meta, FunA, AccA0, FunB, AccB0) ->
	Meta1 = get_class(Domain, Class, Meta),
	FunAB = fun({_Domain, _Class, Key}, Values, Acc) -> 
		Objs = [Obj || {_Meta, Obj} <- Values],
		FunA(Key, Objs, Acc) 
	end,
	nkbase_cmds:fold(Domain, Class, Meta1, values, FunAB, AccA0, FunB, AccB0).


%% @doc Generates an extended-object, to be used in reconcile functions.
-spec make_ext_obj(ext_key(), obj(), put_meta()) ->
	ext_obj().

make_ext_obj(ExtKey, Obj, Meta) ->
	{ExtObj, _PutMeta} = nkbase_cmds:make_ext_obj(ExtKey, Obj, Meta),
	ExtObj.




