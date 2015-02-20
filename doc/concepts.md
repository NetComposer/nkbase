# NkBASE Introduction

NkBASE is designed to be a powerful but simple to use distributed database for riak_core based Erlang applications. Since it is itself based on riak_core, it has many similarities with Riak, but there are also important differences. NkBASE is not designed as a stand-alone application. You must add it as a dependency of your distributed application. In fact, the main reason to develop NkBASE was to be integrated into the upcoming NetComposer system. 

NkBASE has some [configuration](configuration.md) options that must be included as Erlang application configuration options. Many other options belong to the riak_core or riak_ensemble application options.

* [Riak Core Concepts](#riak-core-concepts)
* [Backends, Keys and Values](#backends-keys-and-values)
* [Opration Modes](#operation-modes)
* [Class Metadata](#class-metadata)
* [Indices and Searching](#indices-and-searching)
* [Automatic Expiration](#automatic-expiration)



## Riak Core Concepts

Many important aspects of NkBASE are directly imported from riak_core. Most information on Riak's documentation is also relevant for NkBASE like [the ring and gossip protocol](http://docs.basho.com/riak/latest/theory/concepts/Clusters), [dotted version vectors](http://docs.basho.com/riak/latest/theory/concepts/context/#Dotted-Version-Vectors), [CRDTs](http://docs.basho.com/riak/latest/theory/concepts/crdts/) (although they are used differently in NkBASE) [eventual consistency](http://docs.basho.com/riak/latest/theory/concepts/Eventual-Consistency/), [strong consistency](http://docs.basho.com/riak/latest/theory/concepts/strong-consistency/), [vnodes](http://docs.basho.com/riak/latest/theory/concepts/vnodes/) and [replication and read repair](http://docs.basho.com/riak/latest/theory/concepts/Replication/) (NkBASE does not support active anti-entropy yet). 

When NkBASE application starts, it first try to start riak_core in case it is not already started. The [riak_core config](configuration.md) variable `platform_data_dir` selects the directory to store all data. Riak core will put there the directories _ring_ and _cluster_meta_.

Riak core will start a number of _vnodes_, depending on the value of the `ring_creation_size` config option. A new memory backend and leveldb backend will be started for each vnode, under the `nkbase_store` directory. When new nodes are added to the cluster, the vnodes will be spread among the cluster, along with their stored data See [management](management.md) to see how to add and remove nodes to the system. You can call `nkbase_admin:print_info/0` to see the full state of the system.

## Backends, Keys and Values

For each _backend_, you can store any Erlang object uniquely identified by a _Domain_, a _Class_ and a _Key_. All of them can be any Erlang term (except for binaries starting with `<<255>>`). You can use the three concepts at will, but usually a Domain should represent a big partition on the database (like a client or project) and in the future it will be associated with the security mechanism, and the Class should be used to represent the type of the objects you are storing.

To retrieve any object, you must know its Domain, Class and Key. NkBASE allows to perform several operations on all of the objects having a specific Domain and Class, like listing keys, searching or reindexing.

NkBASE supports currently two different backends:
* **LevelDB**: persistent store based on Basho version of Google's LevelDB. This is the default backend.
* **Ets**: non-persistent memory based store, based on Erlang ETS tables

Most operations can be performed on any backend. However, the strong consistency subsystems can only use leveldb, since it needs a persistent backend.

### Operation modes

NkBASE supports three different operation modes:
* **Eventualy consistent mode**. In this mode, NkBASE will always accept your writes, and it will try to save the object to the number of nodes indicated by the `n` parameter. You must supply the _context_ of the object you are updating. If the object has changed since you got the context, or several writes happen at the same time, a conflict will occur. Depending on the value of the `reconcile` parameter, NkBASE will store both objects or will resolve the conflict by itself automatically. When you read any object, if not all `n` vnodes offer the same object, a _read repair_ operation will happen. See [Eventually Consistent Mode](eventually_consistent.md) for more information.
* **Self-convergent mode**. This mode is very similar to the eventually consistent mode, but NkBASE uses a special object called a _dmap_, that happens to know how to resolve conflicts automatically. You are not allowed to write full objects, but to send _updates_ that NkBASE will apply to the real object. If several users send conflicting modifications at the same time, a well-known policy is used to apply them. See [Self-convergent Mode](self_convergent.md) for more information.
* **Strong consistency mode**. Using this mode, it is guaranteed by NkBASE that all writes and reads are consistent, and no conflict can occur. You must use the _object sequence_ for any update. If the object has been modified since you got the sequence, a failure will be returned and the object will not be modified. The trade-off is that, in case of node failures, write and get operations can fail during a small period of time. See [Strong Consistency Mode](strong_consistency.md) for more information.

The namespace for the three modes are currently the same, so you shoudn't mix different methods for the same object. You can safely use different methods if the Domain, Class or Key are different, but, to avoid errors, it is not recommended using different methods for objects belonging to the same class.

## Class Metadata

Instead of having to supply the metadata for each get or put operation, NkBASE allows you to define any metadata associated to a specific _Domain_ and _Class_, and it will be replicated automatically to all of the nodes of the cluster, so you won't need to include it anymore in any API call.

To define a class, you must use the `nkbase:register_class/3` function, for example:

```erlang
nkbase:register_class("my_domain", "my_class", 
   #{
      backend => ets,
      n => 3,
      indices => 
         [
            {index_1, field_1},
            {index_2, field_2}
         ]
   }
)
```
      
The class definition will overwrite any previous definition. To avoid overwriting previous class definitions, you can add a version:

```erlang
nkbase:register_class("my_domain", "my_class", #{n=>3, vsn => 17})
```

this way the new definition will not overwrite any old version. You can also define a class to be an _alias_ for another class, so you don't have to define it twice.
      
Use ```nkbase:get_classes/0```, ```nkbase:get_classes/1``` and ```nkbase:get_classes/2``` to find currently defined classes, and ```nkbase:get_class/2``` to find the current definition for a defined class.
      
      
## Indices and searching

You can associate with any stored object any number of pairs {index, value}, or tell NkBASE to generate indices automatically from the object, only if it is an Erlang standard ```map()``` or ```proplist()```. Later on, you can perform simple or complex queries over these indices.

The search system is supported by all operation modes. See [search](search.md) for additional information.


## Automatic Expiration

All operation modes support auto-expiration of keys. When you store an object, you can tell NkBASE to program an automatic removal of the object after a configurable amount of time:

```erlang
nkbase:put(domain, my_class, key1, value1, #{ttl=>5}
```

NkBASE will program Erlang timers for near-to-fire removals (see `expire_check` option), with a configurable resolution  (default 1sec, minimum 1 msec, see `expire_resolution` config option). Better resolution provides more accuracy, but requires more cpu time and resources. Far ahead timers are not scheduled to save resources. A periodical process schedules them when they are near to fire. 

NkBASE can support millions of objects with automatic expiration, for very short or long expiration times.






