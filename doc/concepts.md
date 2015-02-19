# NkBASE Introduction

NkBASE is desgined to be a poweful but simple to use distributed database for riak_core-based Erlang applications. Since it is itself also based on riak_core, it has many similarities with Riak, but there are also important differences. NkBASE is not designed as a stand-alone application. You must add it as a dependency of your distributed application. In fact, the main reason to develop NkBASE was to be integrated into the upcoming NetComposer system. 

NkBASE has some configuration options that must be included as Erlang application configuration options. Many other options belong to the riak_core application options or riak_ensemble application options.


* [Riak Core Concepts](#riak-core-concepts)
* [Backends, Keys and Values](#backend-keys-values)
* [Class Metadata](#class-metadata)
* [Indices and Searching](#indiced-and-searching)
* [Automatic Expiration](#automatic-expiration)



## Riak Core Concepts

Many important aspects of NkBASE are directly imported from riak_core. When NkBASE application starts, it first try to start riak_core in case it is not already started. The riak_core config variable _platform_data_dir_ describes the directory to store all data. Riak core will put there the directories 'ring' and 'cluster_meta'.

Riak core will start a number of 'vnodes', depending on the value of the _ring_creation_size_ config option. A new memory backend will be started for each vnode, and, depending on the value of the _backends_ NkBASE config option, also a leveldb backend, under the 'nkbase_store' directory. When new nodes are added to the cluster, the vnodes will be spread among the cluster, along with their stored data See [management](management.md) to see how to add and remove nodes to the system. You can call `nkbase_admin:print_info/0' to see the full state of the system.

Yon can learn more about how this process works [here](http://docs.basho.com/riak/latest/theory/concepts/Clusters/).


## Backends, Keys and Values

For each _backend_, you can store any Erlang object uniquely identified by a _Domain_, a _Class_ and a _Key_. All of them can be any Erlang term (except for binaries starting with <<255>>). You can use the three concepts at will, but usually a Domain should represent a big partition on the database (like a client or project), and the Class should be used to represent the type of the objects you are storing.

To retrieve the object, you must know the Domain, Class and Key. 
NkBASE allows to perform several operations on all of the objects having a specific Domain and Class, like listing keys, searching or reindexing.

NkBASE supports two backend currently:
* LevelDB: persistent store based on Basho version of Google's LevelDB. This is the default backend.
* Ets: non-persistent memory based store, based on Erlang ETS tables

Most operations can be performed on any backend. However, the strong consistency subsistem can only use the leveldb backend.


## Class Metadata

Instead of having to supply the metadata for each operaiton in the get or put call, NkBASE allows you to define metadata associated to a specific Domain and Class, and it will be stored in all of the nodes of the cluster automatically, so you don't need to include anymore in put or get calls.


## Indices and searching

You can associate with any object any number of indices with their values, or tell NkBASE to generate them automatically from the object, if it is an Erlang standard map() or proplist().

Later on, you can perform queries on each of these indices, or several at the same time (see [search](search.md)). 

The search system is supported by all operation modes.


## Automatic Expiration

All operation modes support auto-expiration of keys. When you store an object, you can tell NkBASE to program an automatic removal of the object after a configurable amount of time.

NkBASE program timers for near-to-fire removal, with a configurable resolution (default 1 sec, minimum 1 msec). Far ahead timers are not scheduled to save resources, and a periodical process schedules them when they are new to happen.

This way, NkBASE can support millions of objects with automatic expiration, for short or long expiration times.






