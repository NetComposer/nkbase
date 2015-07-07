# Configuration

NkBASE has several run-time configuration options. It uses standard Erlang application environment variables. 
Some options as specific for the nkbase application, while others belong to the riak_core or riak_ensemble applications.


* [NkBASE config options](#nkbase-config-options)
* [Lager configuration options](#lager-config-options)
* [Riak Core config options](#riak-core-config-options)
* [Riak Ensemble config options](#riak-ensemble-config-options)


## NkBASE config options

The current recognized options are:

Options|Type|Default|Description
---|---|---|---
expire_resolution|`integer()`|`1000`|Resolution for auto-expiration of timers in msecs (see [auto-expiration](concepts.md#automatic-expiration))
expire_check|`integer()`|`60`|Minimum time (in secs) to decide to delay the expiration timers
vnode_workers|`integer()`|`10`|Number of workers to start at each vnode to process async requests
leveldb|`boolean()`|`true`|Activates the LevelDB backend
leveldb_options|`proplist()`|`[{max_open_files, 100}]`|Options for the leveldb backend (see [eleveldb.erl](https://github.com/basho/eleveldb/blob/develop/src/eleveldb.erl) and [Riak configuration options](http://docs.basho.com/riak/latest/ops/advanced/configs/configuration-files/))


## Lager configuration options

Have a look at the [lager repository](https://github.com/basho/lager) documentation.



## Riak Core config options

Riak Core has many configuration options. The most important are:

Options|Type|Default|Description
---|---|---|---
cluster_name|`string()`|`"default"`|Cluster name to use
platform_data_dir|`string()`|`"data"`|Directory to place the ring and all NkBASE information and databases
ring_creation_size|`integer()`|`8`|Number of vnodes to start
target_n_val|`integer()`|`4`|Highest `n` that you generally intend to use (to be used in vnodes distribution policy)
enable_consensus|`boolean()`|`false`|Start the strong consistency system

Have a look to the [riak_core repository](https://github.com/basho/riak_core) and related [Riak configuration options](http://docs.basho.com/riak/latest/ops/advanced/configs/configuration-files/).


## Riak Ensemble config options


Riak Ensemble has many configuration options. The most important are:

Options|Type|Default|Description
---|---|---|---
ensemble_tick|`integer()`|`500`|Time to refresh leases (msecs)
lease_duration|`integer()`|`ensemble_tick * 3 div 2`|Leader lease duration
trust_lease|`boolean()`|`true`|If leader leases are trusted or not. Trusting the lease allows a leader to reply to reads without contacting remote peers as long as its lease has not yet expired
follower_timeout|`integer()`|`lease_duration * 4`|How long a follower waits to hear from the leader before abandoning it
peer_get_timeout|`integer()`|`60000`|Internal timeout used by peer worker FSMs when performing gets
peer_put_timeout|`integer()`|`60000`|Internal timeout used by peer worker FSMs when performing puts
alive_tokens|`integer()`|`2`|Number of leader ticks that can go by without hearing from the ensemble
peer_workers|`integer()`|`1`|The number of peer workers/FSM processes used by the leader
storage_delay|`integer()`|`50`|The operation delay used to coalesce multiple local operations into a single disk operation
storage_tick|`integer()`|`5000`|Periodic tick at which ensembles flushes operations to disk even if there are no explicit sync requests
tree_validation|`boolean()`|`true`|When true, synctrees are not trusted after a peer restart, requiring an exchange with a trusted majority to become trusted. This provides the strongest guarantees against byzantine faults.
synchronous_tree_updates|`boolean()`|`false`|Determines if remote synctree updates are performed synchronously. When true, tree updates are performed before replying to the user

Have a look at the [Riak configuration options](http://docs.basho.com/riak/latest/ops/advanced/configs/configuration-files/) for aditional information.
