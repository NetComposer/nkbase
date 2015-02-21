# NkBASE Management

NkBASE is designed to be integrated into _riak_core_ based distributed Erlang applications. All of the riak_core related aspects, like adding and removing nodes, would be common to NkBASE and your application.

NkBASE includes however all the tools you need to manager the cluster, in case you need it or for developing purposes. You can use these tools, your own, or direct riak_core calls. 

* [Starting NkBASE](#starting-nkbase)
* [Add and Remove Nodes](#add-and-remove-nodes)
* [View cluster information](#view-cluster-information)

## Starting NkBASE

NkBASE can be started as a standard OTP aplication, or calling `nkbase_app:start/0` directly. Only in the second case, NkBASE will create the data directory and start all required dependencies, specially riak_core itself.

NkBASE will use the `platform_data_dir` [config](config.md) parameter of _riak_core_, and will add the directory _nkbase_data_ there in case you start the persistent (leveldb) backend.

The strong consistency system will not be started automatically. See [Strong Consistency Mode](strong_consistency.md) for more information.

The default log level will be read from the `lager` application config options. You can change the _console_ level calling `nkbase_util:loglevel/0`.


## Add and Remove Nodes

Once the first node has started, you can add more nodes to the cluster (for a quick, developer multiple node start, see [quick start](../README.md#quick-start)).

In the second node, start NkBASE (be careful not to start the _consensus system_) and call `nkbase_admin:join/1` (for example: `nkbase_admin:join("dev1@127.0.0.1")). You can repeat this operation for any number of nodes.

Then, you must get the current cluster plan calling `nkbase_admin:cluster_plan/0`. You will see all actions that the cluster must perform to add these nodes. If everything looks ok, call `nkbase_admin:cluster_commit/0` to perform the modifications.

You can use `nkbase_admin:leave/0,1` to instruct the cluster to abandon a current node, and `nkbase_admin:force_remove/1` in case of crashed or not available nodes. You can also replace hosts calling `nkbase_admin:replace/2` and `nkbase_admin:force_replace/2`.

## View cluster information

At any moment, you can call `nkbase_admin:print_info/0` to show in screen a summary of all the cluster related information.

Have a look at `nkbase_admin.erl` for more detailed information functions.
