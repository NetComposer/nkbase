# NkBASE Management

NkBASE is designed to be integrated into _riak_core_ based distributed Erlang applications. All of the riak_core related aspects, like adding and removing nodes, would be common to NkBASE and your application.

NkBASE includes however all the tools you need to manager the cluster, in case you need it or for developing purposes. You can use these tools, your own, or direct riak_core calls. 

* [Starting NkBASE](#starting-nkbase)
* [Add and Remove Nodes](#add-and-remove-nodes)
* [View cluster information](#view-cluster-information)

## Starting NkBASE

NkBASE can be started as a standard OTP aplication, or calling `nkbase_app:start/0` directly. Only in the second case, NkBASE will create the data directory and start all required dependencies, specially riak_core itself.

NkBASE will use the `platform_data_dir` [config](configuration.md) parameter of _riak_core_, and will add the directory _nkbase_data_ there in case you start the persistent (leveldb) backend.

The strong consistency system will not be started automatically. See [Strong Consistency Mode](strong_consistency.md) for more information.

The default log level will be read from the `lager` application config options. You can change the _console_ level calling `nkbase_util:loglevel/0`.


## Add and Remove Nodes

Once the first node has started, you can add more nodes to the cluster (for a quick, developer multiple node start, see [Quick Start](../README.md#quick-start)).

In the second node, start NkBASE (be careful not to start the _consensus system_) and call `nkbase_admin:join/1`, for example:
```erlang
> nkbase_admin:join("dev1@127.0.0.1").
Success: staged join request for 'dev2@127.0.0.1' to "dev1@127.0.0.1"
ok
```

You can repeat this operation for any number of nodes.

Then, you must get the current cluster plan calling `nkbase_admin:cluster_plan/0`. You will see all actions that the cluster must perform to add these nodes. If everything looks ok, call `nkbase_admin:cluster_commit/0` to perform the modifications.

You can use `nkbase_admin:leave/0,1` to instruct the cluster to abandon a current node, and `nkbase_admin:force_remove/1` in case of crashed or not available nodes. You can also replace hosts calling `nkbase_admin:replace/2` and `nkbase_admin:force_replace/2`.

## View cluster information

At any moment, you can call `nkbase_admin:print_info/0` to show in screen a summary of all the cluster related information:
```
> nkbase_admin:print_info().
==================================================================
UP services: [nkbase]
All members: ['dev1@127.0.0.1','dev2@127.0.0.1','dev3@127.0.0.1',
              'dev4@127.0.0.1','dev5@127.0.0.1']
Active members: ['dev1@127.0.0.1','dev2@127.0.0.1','dev3@127.0.0.1',
                 'dev4@127.0.0.1','dev5@127.0.0.1']
Ready members: ['dev1@127.0.0.1','dev2@127.0.0.1','dev3@127.0.0.1',
                'dev4@127.0.0.1','dev5@127.0.0.1']
Partitions owned by 'dev1@127.0.0.1':
+---------+-------------------------------------------------+--+
|  type   |                      index                      |id|
+---------+-------------------------------------------------+--+
| primary |                        0                        |0 |
| primary |913438523331814323877303020447676887284957839360 |5 |
|secondary|182687704666362864775460604089535377456991567872 |1 |
|secondary|365375409332725729550921208179070754913983135744 |2 |
|secondary|548063113999088594326381812268606132370974703616 |3 |
|secondary|730750818665451459101842416358141509827966271488 |4 |
|secondary|1096126227998177188652763624537212264741949407232|6 |
|secondary|1278813932664540053428224228626747642198940975104|7 |
| stopped |                       --                        |--|
+---------+-------------------------------------------------+--+

--------------------------- Owners -------------------------------
[{0,'dev1@127.0.0.1'},
 {1,'dev2@127.0.0.1'},
 {2,'dev3@127.0.0.1'},
 {3,'dev4@127.0.0.1'},
 {4,'dev5@127.0.0.1'},
 {5,'dev1@127.0.0.1'},
 {6,'dev2@127.0.0.1'},
 {7,'dev3@127.0.0.1'}]
----------------------------- All VNodes -------------------------
[{nkbase_vnode,0,<0.211.0>},
 {nkbase_vnode,1,<0.724.0>},
 {nkbase_vnode,2,<0.742.0>},
 {nkbase_vnode,3,<0.759.0>},
 {nkbase_vnode,4,<0.779.0>},
 {nkbase_vnode,5,<0.297.0>},
 {nkbase_vnode,6,<0.796.0>},
 {nkbase_vnode,7,<0.814.0>}]
----------------------------- Transfers -------------------------
'dev5@127.0.0.1' waiting to handoff 7 partitions
'dev4@127.0.0.1' waiting to handoff 7 partitions
'dev1@127.0.0.1' waiting to handoff 6 partitions

Active Transfers:


================================= Membership ==================================
Status     Ring    Pending    Node
-------------------------------------------------------------------------------
valid      25.0%      --      'dev1@127.0.0.1'
valid      25.0%      --      'dev2@127.0.0.1'
valid      25.0%      --      'dev3@127.0.0.1'
valid      12.5%      --      'dev4@127.0.0.1'
valid      12.5%      --      'dev5@127.0.0.1'
-------------------------------------------------------------------------------
Valid:5 / Leaving:0 / Exiting:0 / Joining:0 / Down:0
================================== Claimant ===================================
Claimant:  'dev1@127.0.0.1'
Status:     up
Ring Ready: true

============================== Ownership Handoff ==============================
No pending changes.

============================== Unreachable Nodes ==============================
All nodes are up and reachable

----------------------------- Handoff -------------------------
Each cell indicates active transfers and, in parenthesis, the number of all known transfers.
The 'Total' column is the sum of the active transfers.
+------------------+-----+---------+------+------+------+
|       Node       |Total|Ownership|Resize|Hinted|Repair|
+------------------+-----+---------+------+------+------+
|  dev1@127.0.0.1  |  0  |         |      |0 (10)|      |
|  dev2@127.0.0.1  |  0  |         |      |0 (6) |      |
|  dev3@127.0.0.1  |  0  |         |      |0 (6) |      |
|  dev4@127.0.0.1  |  0  |         |      |0 (9) |      |
|  dev5@127.0.0.1  |  0  |         |      |0 (9) |      |
+------------------+-----+---------+------+------+------+

No ongoing transfers.
============================== Consensus System ===============================
Enabled:     true
Active:      true
Ring Ready:  true
Validation:  strong (trusted majority required)
Metadata:    best-effort replication (asynchronous)

================================== Ensembles ==================================
 Ensemble     Quorum        Nodes      Leader
-------------------------------------------------------------------------------
   root       0 / 1         1 / 1      --
ok
```

Have a look at `nkbase_admin.erl` for more detailed information functions.
