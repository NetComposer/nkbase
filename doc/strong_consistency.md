# Strong Consistency Mode

See [Introduction and Concepts](concepts.md) for and introduction to NkBASE, the strong consistency system and the definition of classes. 

* [Introduction](#introduction)
* [Write Operation](#write-operation)
* [Read Operation](#read-operation)
* [Delete Operation](#delete-operation)
* [Listing Domains, Classes and Keys](#listing-domains-class-and-keys)
* [Examples](#examples)


## Introduction

When using the strong consistency mode, NkBASE guarantees that, if a write operation succeed, that exact value has been stored at all vnodes, whatever are the current cluster state. However, under some circumstances the write operation can fail. In eventually consistent mode, all write operation are always accepted if a single node is still up in the cluster.

Each write operation generates an unique _sequence number_. No client can update an existing object without knowing its current _sequence number_, that can be obtained after any write or calling `nkbase:kget/3,4`.

The consensus subsystem is not started automatically. There are different methods to start it:

1. The recommended way is setting the riak core [configuration parameter](configuration.md) `enable consensus` to `true`, and once the third node is added to the cluster, the consensus subsystem is automatically started. Riak core waits for the third node to be sure that it is started exactly once in the cluster.
1. With any number of nodes in the cluster, you can call `nkbase_ensemble:enable()` to start the subsystem. However, you must be absolutely sure that you enable it only once in the cluster. 
1. You can start the first node of the cluster using the Erlang virtual machine option `-force_master`. However, you must be sure again that no other node uses this option.

Once started, NkBASE will start as may _ensembles_ (_paxos_ consensus groups) as vnodes. Each ensemble will have `n` members, the vnode with the same name and the following n-1 vnodes. A leader will be elected for each ensemble.

With this scheme, all stored objects happen to select the same vnodes that would have been selected when using eventual consistency. This is the reason because the functions for searching and listing domains, classes and keys work also for strong consistency objects.

When the node that has the master for an ensemble dies, a new leader must be elected. Meanwhile, all operations directed to that specific ensemble will fail. When new nodes are added or removed to the system, some vnodes are relocated, a the ensemble members are moved along with them.

As the key namespace for eventual and strong consistency operations is the same, you must be sure about not mixing both types of operations over the same specific object. It is recommended to define a class for every strong consistency operation, using the opcion `sc => true`. This way, NkBASE will not allow eventual consistency operations using this class.

Strong consistency only support the _leveldb` backend currently.


## Write Operation

```erlang
-spec nkbase_sc:kput(nkbase:domain(), nkbase:class(), nkbase:key(), nkbase:obj()) ->
	{ok, nkbase_sc:eseq()} | {error, term()}.

-spec kput(nkbase:domain(), nkbase:class(), nkbase:key(), nkbase:obj(), kput_meta()) ->
	{ok, nkbase_sc:eseq()} | {error, term()}.

```

You can store new objects using the `nkbase_sc:kput/4,5` function calls. You must supply a _Domain_, a _Class_, a _Key_ and a _value_. All of the can be any Erlang term. When using the long version, you can supply additional metadata, that can modify the metadata already stored for this Domain and Class, if defined:

Parameter|Type|Default|Description
---|---|---|---
indices|`[nkbase:index_spec()]`|`[]`|See [search](search.md)
n|`1..5`|3|Number of copies to store
ttl|`integer()|float`|`undefined`|Expiration time (in seconds)
timeout|`integer()`|`30`|Time to wait for the write operation
pre_write_hook|`nkbase:pre_write_fun()`|`undefined`|See bellow
post_write_hook|`nkbase:post_write_fun()`|`undefined`|See bellow
eseq|`new | overwrite | nkbase_sc:eseq()`|`new`|Sequence number to use to update an object

NkBASE will find the corresponding ensemble for this class and key, find its master and send the write operation to it. The master will save the object and will send it two the rest of followers, that will also store the object.

If the master is not available, the operation will fail.

If you are writing a new object, you shouldn't include an _eseq_, (or use `eseq => new`, it is equivalent). The operation will work only if there is no previous object, otherwise it will fail.

If you are updating an existing object, you must supply the exact _sequence number_ of the object. If other client has modified the object since you got the sequence number, the operation will fail, and you should get the new sequence calling `nkbase:kget/3,4` before attempting to store it again. You can also use the option `eseq => overwrite` to overwrite any existing value without looking at the sequence number.

See [nkbase:kput/4,5](eventually_consistent.md#write-operation) for a description of the `ttl`, `timeout`, `pre_write_hook` and `post_write_hook` options. Strong consistency operations also support adding indices to the operation and searching on them (see [search](search.md)).


## Read operation

```erlang
-spec kget(nkbase:domain(), nkbase:class(), nkbase:key()) ->
	{ok, nkbase_sc:eseq(), nkbase:obj()} | {error, term()}.

-spec kget(nkbase:domain(), nkbase:class(), nkbase:key(), kget_meta()) ->
	{ok, nkbase_sc:eseq(), nkbase:obj()} | {error, term()}.
```

These functions perform a read operation in the cluster. You must supply the _Domain_, a _Class_, and _Key_ of the object you want to retrieve. When using the long version, you can supply additional metadata, that can modify the metadata already stored for this Domain and Class, if defined:

Parameter|Type|Default|Description
---|---|---|---
n|`1..5`|3|Number of copies of the stored object
timeout|`integer()`|`30`|Time to wait for the write operation
get_fields|`[term()|tuple()]`|`undefined`|Receive these fields instead of the full object
get_indices|`[nkbase:index_name()]`|`undefined`|Receive these indices instead of the full objecy
read_repair|`boolean()`|false|Use read repair (see bellow)

NkBASE will find the corresponding ensemble for this class and key, find its master and send the get operation to it. Under some circumstances, and if the `read_repair` option is not set, the master will reply without consulting its followers. Otherwise, all followers reply, and, if some of the has an old version of the data, it is corrected by the master.


## Delete operation

```erlang
-spec kdel(nkbase:domain(), nkbase:class(), nkbase:key()) ->
	{ok, nkbase_sc:eseq()} | {error, term()}.

-spec kdel(nkbase:domain(), nkbase:class(), nkbase:key(), kput_meta()) ->
	{ok, nkbase_sc:eseq()} | {error, term()}.
```

The strong consistency system does not currently fully removes objects at any time. When a delete operation success, the object is actually marked as deleted but not fully removed.

As in the case of write operations, you must supply the valid sequence number or select to overwrite the old value.


## Listing Domains, Classes and Keys

You can use the same functions for [Eventually Consistent Mode](eventually_consistent.md#listing-domains-classes-and-keys).


## Examples

Let's start by registering a class and storing a new object:

```erlang
> nkbase:register_class(domain, sc1, #{n=> 3, sc => true}).
ok

> nkbase_sc:kget(domain, sc1, key1).
{error, not_found}

> {ok, Seq1} = nkbase_sc:kput(domain, sc1, key1, value1).
{ok, ...}

> {ok, Seq1, value1} = nkbase_sc:kget(domain, sc1, key1).
{ok, ..., value1}
```

If we use the wrong sequence number, the operation will fail:
```erlang

> nkbase_sc:kput(domain, sc1, key1, value2).
{error, failed}

> {ok, Seq2} = nkbase_sc:kput(domain, sc1, key1, value2, #{eseq=>Seq1}).
{ok, ...}

> {ok, Seq2, value2} = nkbase_sc:kget(domain, sc1, key1).
{ok, ..., value2}

> {ok, Seq3} = nkbase_sc:kput(domain, sc1, key1, value3, #{eseq=>overwrite}).
{ok, ...}

> {ok, Seq3, value3} = nkbase_sc:kget(domain, sc1, key1).
{ok, ..., value3}
```

We finally delete the object.

```erlang
> nkbase_sc:kdel(domain, sc1, key1).
{error, failed}

> nkbase_sc:kdel(domain, sc1, key1, #{eseq=>Seq3}).
ok

> nkbase_sc:kget(domain, sc1, key1).
{error, not_found}
```





