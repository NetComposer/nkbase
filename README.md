## Introduction

NkBASE is a distributed, highly available key-value database designed to be integrated into Erlang applications based on [_riak_core_](https://github.com/basho/riak_core). It is one of the core pieces of the upcoming Nekso's Software Defined Data Center Platform, **NetComposer**.

NkBASE uses a no-master, share-nothing architecture, where no node has any special role. It is able to store multiple copies of each object to achive high availabity and to distribute the load evenly among the cluster. Nodes can be added and removed on the fly. It shows low latency, and it is very easy to use. 

NkBASE has some special features, like been able to work simultaneously as a [**eventually consistent**](http://www.allthingsdistributed.com/2008/12/eventually_consistent.html) database using _Dotted Version Vectors_, a [**strong consistent**](http://docs.basho.com/riak/latest/theory/concepts/strong-consistency/) database and a [**eventually consistent, self-convergent**](http://research.microsoft.com/apps/video/default.aspx?id=153540&r=1) database using CRDTs called _dmaps_. It has also a flexible and easy to use [**query language**](doc/search.md) that (under some circunstances) can be very efficient, and has powerful support for [**auto-expiration**](doc/concepts.md#automatic-expiration) of objects.

The minimum recommended cluster size for NkBASE is three nodes, but it can work from a single node to hundreds of them. However, NkBASE is not designed for very high load or huge data (you really should use the excellent [**Riak**](http://basho.com/riak/) and [**Riak Enterprise**](http://basho.com/riak-enterprise/) for that), but as an in-system, flexible and easy to use database, useful in multiple scenarios like configuration, sessions, cluster coordination, catalogue search, temporary data, cache, field completions, etc. In the future, NetComposer will be able to start and manage multiple kinds of services, including databases like a full-blown Riak.

NkBASE has a clean code base, and can be used as a starting point to learn how to build a distributed Erlang system on top of _riak_core_, and to test new backends or replication mechanisms. NkBASE would have been impossible without the incredible work from [**Basho**](http://basho.com), the makers of Riak: [riak_core](https://github.com/basho/riak_core), [riak_dt](https://github.com/basho/riak_dt) and [riak_ensemble](https://github.com/basho/riak_ensemble).  


## Features
* Highly available, allowing read and write operation even in case of node failures.
* Operation friendly. Nodes can be added and removed on the fly.
* Scalable from three to hundreds of nodes.
* Disk (leveldb) and memory (ets) backends.
* Three simultaneous operation modes:
	* Eventually consistent mode using Dotted Version Vectors.
	* Strong consistent mode based on using riak_ensemble's multi-paxos.
	* Easy to use, self-convergent _dmaps_.
* Multiple, auto-generated secondary indices, usable in the three modes.
* Simple, easy to use, _utf8_ and _latin-1_ aware query language for secondary indices.
* Full support for auto-expiration of objects, with configurable resolution.



# Documentation

* [Introduction and Concepts](doc/concepts.md)<br/>
* [Eventually consistent mode](doc/eventually_consistent.md)<br/>
* [Self-convergent mode using DMaps](doc/self_convergent.md)<br/>
* [Strong consistency mode](doc/strong_consistency.md)<br/>
* [Searching](doc/search.md)<br/>
* [Configuration](doc/configuration.md)<br/>
* [Management](doc/management.md)<br/>
* [Roadmap](doc/roadmap.md)<br/>
* [Changelog](doc/changelog.md)<br/>


# Quick Start

Follow this steps to start a 5-node development cluster in a single machine:

```
git clone https://github.com/Nekso/nkbase.git
cd nkbase
make
make dev1
```

then, at another terminal console:
```
cd nkbase
make dev2

> nkbase_admin:join("dev1@127.0.0.1").
Success: staged join request for 'dev2@127.0.0.1' to "dev1@127.0.0.1"
ok
```

and repeat for nodes 3 to 5.
Then go to any of the nodes and run:

```
> nkbase_admin:cluster_plan().
=============================== Staged Changes ================================
Action         Details(s)
-------------------------------------------------------------------------------
join           'dev2@127.0.0.1'
join           'dev3@127.0.0.1'
join           'dev4@127.0.0.1'
join           'dev5@127.0.0.1'
-------------------------------------------------------------------------------


NOTE: Applying these changes will result in 1 cluster transition

###############################################################################
                         After cluster transition 1/1
###############################################################################

================================= Membership ==================================
Status     Ring    Pending    Node
-------------------------------------------------------------------------------
valid     100.0%     25.0%    'dev1@127.0.0.1'
valid       0.0%     25.0%    'dev2@127.0.0.1'
valid       0.0%     25.0%    'dev3@127.0.0.1'
valid       0.0%     12.5%    'dev4@127.0.0.1'
valid       0.0%     12.5%    'dev5@127.0.0.1'
-------------------------------------------------------------------------------
Valid:5 / Leaving:0 / Exiting:0 / Joining:0 / Down:0

Transfers resulting from cluster changes: 6
  1 transfers from 'dev1@127.0.0.1' to 'dev5@127.0.0.1'
  1 transfers from 'dev1@127.0.0.1' to 'dev4@127.0.0.1'
  2 transfers from 'dev1@127.0.0.1' to 'dev3@127.0.0.1'
  2 transfers from 'dev1@127.0.0.1' to 'dev2@127.0.0.1'

ok
```
and

```
> nkbase_admin:cluster_commit().
Cluster changes committed
ok
```

Congratulations! Now you have a 5-node NkBASE cluster. At any of the nodes, type:

```
> nkbase:put(domain, class, key, "my_value")
ok
```

and, at any other node,
```
> nkbase:get(domain, class, key)
{ok, ..., "my_value"}
```

# Contributing

Please contribute with code, bug fixes, documentation fixes or any other form. Use 
GitHub Issues and Pull Requests, forking this repository.


