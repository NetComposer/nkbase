# Eventually Consistent Mode

See [Introduction and Concepts](concepts.md) for and introduction to NkBASE, the eventual consistent system and the definition of classes.

* [Write Operation](#write-operation)
* [Read Operation](#write-operation)
* [Get Specification](#get-specification)
* [Del Operation](#write-operation)
* [Listing Domains, Classes and Keys](#listing-domains-classes-and-keys)


## Write operation

```erlang
-spec put(nkbase:domain(), nkbase:class(), nkbase:key(), nkbase:obj()) ->
	ok | {error, term()}.

-spec put(nkbase:domain(), nkbase:class(), nkbase:key(), nkbase:obj(), nkbase:put_meta()) ->
	ok | {error, term()}.
```

You can store new objects in NkBASE using the `nkbase:put/4,5` function calls. You must supply a _Domain_, a _Class_, a _Key_ and a _value_. All of the can be any Erlang term. When using the latter version, you can supply additional metadata, that can modify the metadata already stored for this Domain and Class, if defined:

Parameter|Type|Default|Description
---|---|---
backend|`ets`&#124`leveldb`|`leveldb`|Backend to use
indices|`[nkbase:index_spec()]`|`[]`|See [search](search.md)
n|`1..5`|3|Number of copies to store
w|`1..5`|3|Number of nodes to wait in write operations
reconcile|`nkbase:reconcile()`|`undefined`|See bellow
ttl|`integer()` &#124 `float`|`undefined`|Expiration time (in seconds)
timeout|`integer()`|`30`|Time to wait for the write operation
pre_write_hook|`nkbase:pre_write_fun()`|`undefined`|See bellow
post_write_hook|`nkbase:post_write_fun()`|`undefined`|See bellow
ctx|`nkbase:ctx()`|`undefined`|Updated object's context

NkBASE select `n` nodes to store the object, and will send the write operation to them. It will then wait for `w` nodes to acknowledge the write to the indicated _backend_ and return the result. An 'ok' response means that all the requested vnodes have received and stored the object. It **does not mean that there is no conflict**. An error response means that some of the vnodes have failed to store the request (because of a disk error or similar).

If you are updating an existing object, you must supply its _context_ (you can get when calling `nkbase:get/3,4`) or a conflict will occur. If the new object conflicts with any stored one, at any of the vnodes, 
the result will depend on the `reconcile` parameter:

* undefined: the server will not resolve the conflict, storing both objects. Later on, when you read it, both values will be returned, along with the righ context to resolve the conflict and save a new reconciled value.
* lww: the last stored object will win. Please notice that all clients will receive an `ok`, so the _looser_ will not know that its value has been lost.
* `nkbase:reconcile()`: if you supply a function, it will be called with all the conflicting objects, and you can select one of them or create a new one.

If you supply a 'ttl' value (in seconds) the object will be fully removed (not only deleted) automatically after this time (in seconds) at all vnodes. In case of conflicting values with different ttls, only the last timed ttl value applies. This also means that if any of the conflicting values has no ttl, no automatic remove is performed.

You can also supply any number of secondary indices, and they will be stored along with the object, so you can search on them later on (see [search](search.md)).

If one or several of servers that host one or several of the selected vnodes are down, the request will be sent to alternative vnodes at running servers. When the failing servers are up again, the objects will be moved back to the primary vnodes.


## Read operation

```erlang
-type reply() :: obj() | '$nkdeleted' |
	#{
		fields => #{ term() => term() },
		indices => #{ nkbase:index_name() => [term()]}
	}.

-spec get(nkbase:domain(), nkbase:class(), nkbase:key()) ->
	{ok, nkbase:ctx(), reply()} | {deleted, nkbase:ctx()} | 
	{multi, nkbase:ctx(), [reply()]} |
	{error, term()}.

-spec get(nkbase:domain(), nkbase:class(), nkbase:key(), nkbase:get_meta()) ->
	{ok, nkbase:ctx(), reply()} | {deleted, nkbase:ctx()} | 
	{multi, nkbase:ctx(), [reply()]} |
	{error, term()}.
```

These functions perform a read operation in the cluster. You must supply the _Domain_, a _Class_, and _Key_ of the object you want to retrieve. When using the latter version, you can supply additional metadata, that can modify the metadata already stored for this Domain and Class, if defined:

Parameter|Type|Default|Description
---|---|---
backend|`ets`&#124`leveldb`|`leveldb`|Backend to use
n|`1..5`|3|Number of copies of the stored object
r|`1..5`|3|Number of nodes to take into account for the read operations
reconcile|`nkbase:reconcile()`|`undefined`|See bellow
timeout|`integer()`|`30`|Time to wait for the write operation
get_fields|`[term()`&#124`tuple()]`|`undefined`|Order to receive these fields only.
get_indices|`[nkbase:index_name()]`|`undefined`|Order to receive these indices only.

NkBASE will send the read request to the same `n` vnodes used to store these object, and will wait for `r` to send an answer. If any vnode sends an error, the full operation is aborted. If any one sends 'not found' it is ignored. If a non-conflicting value emerges, it is returned, along with its context. 

If any conflict is detected (because the stored object in any of the vnodes had several values, or because some vnodes sent different, conflicting values), the result will depend on the `reconcile` option:

* `undefined`: The server will return `{multi, Ctx, [Objs]}`, with all the stored objects. You can resolve the conflict and use the supplied context to update the object. If there are deleted objects, they will be represented as `'$nkdeleted'`.
* `lww`: The server will return only the most recent obj, and will also overwrite that value in all vnodes to resolve the conflict.
* `nkbase:reconcile()`: If you supply a function, it will be called with all conflicting objects, and you must return the right object or a new one. This will be the returned value, and the server will also overwrite this value in all vnodes to resolve the conflict.

The server then waits for the remaining (up to `n`) nodes to respond, reconciles again, and updates the _winning_ value in all vnodes back (this process is usually called _read repair_).


### Get specification

The functions `nkbase:get/3,4` will normally return the full object or objects stored under a _Domain_, _Class_ and _Key_. You can instruct NkBASE to return a subset from the object, instead of the full object. 

If you use the option `get_fields`, only for Erlang objects of type `map()` or `proplist()`, it will return a map with a field `fields` and the values of the fields you requested. For example:

```erlang
> nkbase:put(domain, class, key1, #{field1=>value1, field2=>value2}).
ok

> nkbase:get(domain, class, key1)..
{ok, ..., #{field1=>value1, field2=>value2}}

> nkbase:get(domain, class, key1, #{get_fields=>[field1]}).
{ok, ..., #{fields=>#{field1=>value1}}}
```

and for proplists:

```erlang
> nkbase:put(domain, class, key2, [{field1, value1}, {field2, value2}]).
ok

> nkbase:get(domain, class, key2).
{ok, ..., [{field1, value1}, {field2, value2}]}

> nkbase:get(domain, class, key1, #{get_fields=>[field2]}).
{ok, ..., #{fields => #{field2 => value2}}}
```

you can also access nested elements, even mixing `map()` and `proplist()` objects:

```erlang
> nkbase:put(domain, class, key3, #{field1 => [{field11, #{field111=>v111}}]}).
ok

> nkbase:get(domain, class, key3, #{get_fields=>[field1]}).
{ok, ..., #{fields => #{field1 => [{field11, #{field111 => v111}}]}}}

> nkbase:get(domain, class, key3, #{get_fields=>[{field1, field11}]}).
{ok, ..., #{fields => #{{field1,field11} => #{field111 => v111}}}}

> nkbase:get(domain, class, key3, #{get_fields=>[{field1, field11, field111}]}).
{ok, ..., #{fields => #{{field1,field11,field111} => v111}}}
```

Also, you can ask NkBASE to return the value of some indices:

```erlang
> nkbase:put(domain, class, key4, #{field1=>value1, field2=>value2}, #{indices=>[{i1, v1}]}).
ok

> nkbase:search(domain, class, [{i1, {eq, v1}}]).
{ok, [{v1, key4, []}]}

> nkbase:get(domain, class, key4, #{get_fields=>[field1], get_indices=>[i1]}).
{ok, ..., #{fields => #{field1 => value1},indices => #{i1 => [v1]}}}
```


## Delete operation

```erlang
-spec del(nkbase:domain(), nkbase:class(), nkbase:key()) ->
	ok | {error, term()}.

-spec del(nkbase:domain(), nkbase:class(), nkbase:key(), nkbase:put_meta()) ->
	ok | {error, term()}.
```

NkBASE does not remove the object after calling this functions. Instead, it marks the object as _deleted_, what means removing all indices and replacing its value for `'$nkdeleted'`. Also, a `ttl` is automatically added to the object, and, after that time, the object is actually permanently removed from the system.

Deletes are usually a dangerous operation on distributed databases. Since the context of the object is lost, it is not easy to recover from some circumstances. For example, if a node is down when a remove is performed, and it goes up intermediately, the object can _resurrect_ magically.

You can change the default expiration time (1 hour) to any value, even to 0.

Otherwise, deletes are normal puts, so every description for `nkbase:put/4,5` applies here (see [Write operation](#write-operation)).

```erlang
-spec remove_all(nkbase:domain(), #{backend=>backend(), timeout=>pos_integer()}) ->
	ok | {error, term()}.

-spec remove_all(nkbase:domain(), nkbase:class(), #{backend=>backend(), timeout=>pos_integer()}) ->
	ok | {error, term()}.
```

You can use the function `nkbase:remove_all/2` to remove all objects belonging to a domain for a specific backdend, and `nkbase:remove_all/3` to remove all objects belonging to a domain and class. These functions should not be used in production under load.


## Listing Domains, Classes and Keys


```erlang
-spec list_domains() ->
	{ok, [nkbase:domain()]} | {error, term()}.

-spec list_domains(#{backend=>backend(), timeout=>pos_integer()}) ->
	{ok, [nkbase:domain()]} | {error, term()}.
```

Function `nkbase:list_domains/0,1` can be used to list the domains currently having any object belonging to them for a specific backend. Please notice that _deleted_ objects, not yet removed, will count.


```erlang
-spec list_classes(nkbase:domain()) ->
	{ok, [nkbase:class()]} | {error, term()}.

-spec list_classes(nkbase:domain(), #{backend=>backend(), timeout=>pos_integer()}) ->
	{ok, [nkbase:class()]}.


Use function `nkbase:list_classes/1,2` to find the classes, for a specific domain, having any object belonging to them, for a specific backend. Please notice that _deleted_ objects, not yet removed, will count.


```erlang
-spec list_keys(nkbase:domain(), nkbase:class()) ->
	{ok, [nkbase:key()]} | {error, term()}.

-spec list_keys(nkbase:domain(), nkbase:class(), nkbase:scan_meta()) ->
	{ok, [nkbase:key()]} | {error, term()}.
```

You can use the function `nkbase:list_keys/2,3` for find keys used for a specific domain, class and backend. When using the latter version, you can supply additional metadata, that can modify the metadata already stored for this Domain and Class, if defined:

Parameter|Type|Default|Description
---|---|---
backend|`ets`&#124`leveldb`|`leveldb`|Backend to use
n|`1..5`|3|Number of copies to store
timeout|`integer()`|`30`|Time to wait for scan operation
start|`nkbase:key()`|`undefined`|First key to iterate
stop|`nkbase:key()`|`undefined`|Last key to iterate
page_size|integer()|`1000`|Page size
filter_deleted|`boolean()`|`false`|Filter deleted objects

You must indicate (here or at the stored class specification) the backend and number of copies for this class (`n`). By default the list will start at the first key, stop at the last and return up to `1000` keys. You can change the number of keys returned (`page_size`) and where to start and stop the iteration.

By default, deleted objects will appear on the list. You cas use the option `filter_deleted`, and they will be filtered, but it will be slower since each object has to be read from the backend.


### Iterating objects

NkBASE offers functions to iterate over all keys or values in the database. See functions `nkbase:iter_keys/7` and `nkbase:iter_objs/7`.











