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
%% Partially based on riak_kv_console and riak-admin
%%
%% -------------------------------------------------------------------

%% @doc Admin Module
-module(nkbase_admin).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').

-export([get_info/0, print_info/0]).
-export([join/1, leave/0, leave/1, force_remove/1, replace/2, force_replace/2]).
-export([cluster_plan/0, cluster_commit/0, cluster_clear/0, cluster_status/0]).
-export([partitions/0, partitions/1, partition_count/0, partition_count/1]).
-export([partition_id/1, partition_index/1]).
-export([ringready/0, down/1, transfers/0, transfer_limit/0]).
-export([member_status/0, ring_status/0, cluster_info/1, stats/0, services/0]).
-export([ticktime/1, reload_code/0]).
-export([handoff_summary/0, handoff_enable/2, handoff_disable/2]).
-export([handoff_details/0, handoff_details/1, handoff_config/0, handoff_config/1]).
-export([stat_show/1, stat_info/1, stat_enable/1, stat_disable/1, stat_reset/1]).
-export([ensemble_overview/0, ensemble_detail/1]).

% -include("../deps/riak_ensemble/include/riak_ensemble_types.hrl").


%% ===================================================================
%% Summary
%% ===================================================================

get_info() ->
    [
        {stats, riak_core_stat:get_stats()},
        {ring_ready, riak_core_status:ringready()},
        {all_active_transfers, riak_core_status:all_active_transfers()},
        {transfers, riak_core_status:transfers()},
        {vnodes, riak_core_vnode_manager:all_index_pid(nkbase_vnode)}
    ].


print_info() ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    io:format("==================================================================\n", []),
    io:format("UP services: ~p\n", [riak_core_node_watcher:services()]),
    io:format("All members: ~p\n", [riak_core_ring:all_members(Ring)]),
    io:format("Active members: ~p\n", [riak_core_ring:active_members(Ring)]),
    io:format("Ready members: ~p\n", [riak_core_ring:ready_members(Ring)]),
    partitions(),
    % io:format("-------------------------- Idx2Num ------------------------------\n", []),
    % io:format("~p\n", [riak_core_mochiglobal:get(nkbase_idx2pos)]),
    OwnersData = riak_core_ring:all_owners(Ring),
    Owners = [{nkdist_util:idx2pos(Idx), Node} || {Idx, Node} <- OwnersData],
    io:format("--------------------------- Owners -------------------------------\n", []),
    io:format("~p\n", [Owners]),
    AllVNodes = 
        [{Srv, nkdist_util:idx2pos(Idx), Pid} || 
        {Srv, Idx, Pid} <- riak_core_vnode_manager:all_vnodes()],
    io:format("----------------------------- All VNodes -------------------------\n", []),
    io:format("~p\n", [lists:sort(AllVNodes)]),

    io:format("----------------------------- Transfers -------------------------\n", []),
    transfers(),

    riak_core_console:member_status([]),
    riak_core_console:ring_status([]),

    io:format("----------------------------- Handoff -------------------------\n", []),
    handoff_summary(),
    handoff_details(),
    nkbase_ensemble_admin:ensemble_overview(),
    ok.


%% ===================================================================
%% Cluster commands
%%
%% The following commands stage changes to cluster membership. These commands
%% do not take effect immediately. After staging a set of changes, the staged
%% plan must be committed to take effect:
%%
%%    join <node>                     Join node to the cluster containing <node>
%%    leave                           Have this node leave the cluster and shutdown
%%    leave <node>                    Have <node> leave the cluster and shutdown
%%
%%    force-remove <node>             Remove <node> from the cluster without
%%                                    first handing off data. Designed for
%%                                    crashed, unrecoverable nodes
%%
%%    replace <node1> <node2>         Have <node1> transfer all data to <node2>,
%%                                    and then leave the cluster and shutdown
%%
%%    force-replace <node1> <node2>   Reassign all partitions owned by <node1> to
%%                                    <node2> without first handing off data, and
%%                                    remove <node1> from the cluster.
%%
%% Staging commands:
%%    plan                            Display the staged changes to the cluster
%%    commit                          Commit the staged changes
%%    clear                           Clear the staged changes
%%
%% Status and information commands:
%%    status                          Display a concise summary of node membership
%%                                    availability and ring ownership.
%%
%%    partitions [--node=<node>]      Print primary, secondary and stopped
%%                                    partition indices and ids for the current
%%                                    node, or for the specified node.
%%
%%    partition-count [--node=<node>] Print the cluster-wide number of
%%                                    partitions or the number of partitions
%%                                    on the specified node.
%%
%%    partition id=<id>               Convert the given partition id to the
%%                                    matching index.
%%
%%    partition index=<index>         Convert the given partition index to
%%                                    the matching id.
%% ===================================================================

%% @doc Prepares a new node for join
-spec join(string()) ->	ok | error.
join(Node) when is_list(Node) ->
    try
        case riak_core:staged_join(Node) of
            ok ->
                io:format("Success: staged join request for ~p to ~p~n", 
                		  [node(), Node]),
                ok;
            {error, not_reachable} ->
                io:format("Node ~s is not reachable!~n", [Node]),
                error;
            {error, different_ring_sizes} ->
                io:format("Failed: ~s has a different ring_creation_size~n",
                          [Node]),
                error;
            {error, unable_to_get_join_ring} ->
                io:format("Failed: Unable to get ring from ~s~n", [Node]),
                error;
            {error, not_single_node} ->
                io:format("Failed: This node is already a member of a "
                          "cluster~n"),
                error;
            {error, self_join} ->
                io:format("Failed: This node cannot join itself in a "
                          "cluster~n"),
                error
            % {error, _} ->
            %     io:format("Join failed. Try again in a few moments.~n", []),
            %     error
        end
    catch
        Exception:Reason ->
            lager:error("Join failed ~p:~p", [Exception, Reason]),
            io:format("Join failed, see log for details~n"),
            error
    end.


%% @doc Leaves the cluster (data partitions are handed off)
-spec leave() -> ok | error.
leave() ->
	riak_core_console:stage_leave([]).


%% @doc Instructs a node to leave the cluster (data partitions are handed off)
-spec leave(string()) -> ok | error.
leave(Node) when is_list(Node) ->
	riak_core_console:stage_leave([Node]).


%% @doc Removes another node from the cluster without first handing off 
%% its data partitions (for use wirh crashed or unrecoverable nodes).
-spec force_remove(string()) -> ok | error.
force_remove(Node) when is_list(Node) ->
	riak_core_console:stage_remove([Node]).


%% @doc Transfer all node partitions of Node1 to Node2
-spec replace(string(), string()) -> ok | error.
replace(Node1, Node2) when is_list(Node1), is_list(Node2) ->
	riak_core_console:stage_replace([Node1, Node2]).


%% @doc Transfer all node partitions of Node1 to Node2, without handing of data
-spec force_replace(string(), string()) -> ok | error.
force_replace(Node1, Node2) when is_list(Node1), is_list(Node2) ->
	riak_core_console:stage_force_replace([Node1, Node2]).


% %% @doc Resize ring (NOT YET SUPPORTED)
% -spec resize_ring(string()) ->ok | error.
% resize_ring(Size) when is_list(Size) ->
% 	riak_core_console:stage_resize_ring([Size]).


%% @doc Prints the current cluster plan
-spec cluster_plan() -> ok.
cluster_plan() ->
	riak_core_console:print_staged([]).


%% @doc Commits the current cluster plan
-spec cluster_commit() -> ok | error.
cluster_commit() ->
	riak_core_console:commit_staged([]).


%% @doc Clears the current cluster plan
-spec cluster_clear() -> ok.
cluster_clear() ->
	riak_core_console:clear_staged([]).


%% @doc Prints cluster status
-spec cluster_status() -> ok.
cluster_status() ->
    riak_core_console:command(["riak-admin", "cluster",  "status"]).


%% @doc Prints partition table
-spec partitions() -> ok.
partitions() ->
    riak_core_console:command(["riak-admin", "cluster",  "partitions"]).


%% @doc Prints partition table for a node
-spec partitions(string()) -> ok.
partitions(Node) when is_list(Node) ->
    riak_core_console:command(["riak-admin", "cluster",  "partitions", "-n", Node]).


%% @doc Prints partition count
-spec partition_count() -> ok.
partition_count() ->
    riak_core_console:command(["riak-admin", "cluster",  "partition-count"]).


%% @doc Prints partition count for a node
-spec partition_count(string()) -> ok.
partition_count(Node) when is_list(Node) ->
    riak_core_console:command(["riak-admin", "cluster",  "partition-count", "-n", Node]).


%% @doc Prints partition table for a node
-spec partition_id(string()) -> ok.
partition_id(Id) when is_list(Id) ->
    riak_core_console:command(["riak-admin", "cluster",  "partition", "id="++Id]).


%% @doc Prints partition table for a node
-spec partition_index(string()) -> ok.
partition_index(Id) when is_list(Id) ->
    riak_core_console:command(["riak-admin", "cluster",  "partition", "index="++Id]).



%% ===================================================================
%% Other Cluster
%% ===================================================================

%% @doc Check if all nodes in the cluster agree on the partition assignment
-spec ringready() -> ok | error.
ringready() ->
    try
        case riak_core_status:ringready() of
            {ok, Nodes} ->
                io:format("TRUE All nodes agree on the ring ~p\n", [Nodes]);
            {error, {different_owners, N1, N2}} ->
                io:format("FALSE Node ~p and ~p list different partition owners\n", [N1, N2]),
                error;
            {error, {nodes_down, Down}} ->
                io:format("FALSE ~p down.  All nodes need to be up to check.\n", [Down]),
                error
        end
    catch
        Exception:Reason ->
            lager:error("Ringready failed ~p:~p", [Exception,
                    Reason]),
            io:format("Ringready failed, see log for details~n"),
            error
    end.


%% @doc Marks a node as down so that ring transitions can be performed 
%% before the node is brought back online.
-spec down(string()) -> ok | error.
down(Node) ->
    try
        case riak_core:down(list_to_atom(Node)) of
            ok ->
                io:format("Success: ~p marked as down~n", [Node]),
                ok;
            {error, legacy_mode} ->
                io:format("Cluster is currently in legacy mode~n"),
                ok;
            {error, is_up} ->
                io:format("Failed: ~s is up~n", [Node]),
                error;
            {error, not_member} ->
                io:format("Failed: ~p is not a member of the cluster.~n",
                          [Node]),
                error;
            {error, only_member} ->
                io:format("Failed: ~p is the only member.~n", [Node]),
                error
        end
    catch
        Exception:Reason ->
            lager:error("Down failed ~p:~p", [Exception, Reason]),
            io:format("Down failed, see log for details~n"),
            error
    end.


%% @doc Provide a list of nodes with pending partition transfers
%% (i.e. any secondary vnodes) and list any owned vnodes that are *not* running
-spec transfers() -> ok.
transfers() ->
    riak_core_console:transfers([]).


%% @doc Print handoff transfer limit
-spec transfer_limit() -> ok.
transfer_limit() ->
    riak_core_console:transfer_limit([]).

%% @doc Prints member status
-spec member_status() -> ok.
member_status() ->
    riak_core_console:member_status([]).


%% @doc Prints ring status
-spec ring_status() -> ok.
ring_status() ->
    riak_core_console:ring_status([]).


%% @doc Dumps cluster info to file(s)
%% Format: ``<output_file> ['local' | <node> ['local' | <node>] [...]]''
-spec cluster_info([string()]) -> ok | error.
cluster_info([OutFile|Rest]) ->
    try
        case lists:reverse(atomify_nodestrs(Rest)) of
            [] ->
                cluster_info:dump_all_connected(OutFile);
            Nodes ->
                cluster_info:dump_nodes(Nodes, OutFile)
        end
    catch
        error:{badmatch, {error, eacces}} ->
            io:format("Cluster_info failed, permission denied writing to ~p~n", [OutFile]);
        error:{badmatch, {error, enoent}} ->
            io:format("Cluster_info failed, no such directory ~p~n", [filename:dirname(OutFile)]);
        error:{badmatch, {error, enotdir}} ->
            io:format("Cluster_info failed, not a directory ~p~n", [filename:dirname(OutFile)]);
        Exception:Reason ->
            lager:error("Cluster_info failed ~p:~p",
                [Exception, Reason]),
            io:format("Cluster_info failed, see log for details~n"),
            error
    end.


%% @doc Lists core stats
-spec stats() -> [{atom(), term()}].
stats() ->
    riak_core_stat:get_stats().


%% @doc Lists available services
-spec services() -> [atom()].
services() ->
    riak_core_node_watcher:services().


%% @doc Updates system ticktime (default is 60)
-spec ticktime(integer()) -> ok.
ticktime(Time) when is_integer(Time), Time > 1 ->
    riak_core_net_ticktime:start_set_net_ticktime_daemon(node(), Time).


%% @doc Get new paths and reloads all code
-spec reload_code() -> ok.
reload_code() ->
    case app_helper:get_env(nkbase, add_paths) of
        List when is_list(List) ->
            _ = [ reload_path(filename:absname(Path)) || Path <- List ],
            ok;
        _ -> 
            ok
    end.


%% ===================================================================
%% VNode Status
%% ===================================================================

% %% @doc Get Vnode Status
% -spec vnode_status() -> term().
% vnode_status() ->
%     nkbase_cmds:send_all(status, #{}).



%% ===================================================================
%% Handoff
%% ===================================================================

%% @doc Get handoff summary
-spec handoff_summary() -> ok.
handoff_summary() ->
    riak_core_console:command(["riak-admin", "handoff", "summary"]). 


%% @doc Enables handoff
-spec handoff_enable(inbound|outbound|both, string()) -> ok.
handoff_enable(Type, Node)
        when (Type==inbound orelse Type==outbound orelse Type==both)
             andalso (Node==all orelse is_list(Node)) ->
    NodeStr = case Node of
        all -> ["-a"];
        _ -> ["-n", Node]
    end,
    riak_core_console:command(["riak-admin", "handoff", "enable", 
                                atom_to_list(Type) | NodeStr]). 


%% @doc Disables handoff
-spec handoff_disable(inbound|outbound|both, string()) -> ok.
handoff_disable(Type, Node)
        when Type==inbound; Type==outbound; Type==both ->
    NodeStr = case Node of
        all -> ["-a"];
        _ -> ["-n", Node]
    end,
    riak_core_console:command(["riak-admin", "handoff", "disable", 
                                atom_to_list(Type) | NodeStr]). 


%% @doc Print handoff details
-spec handoff_details() -> ok.
handoff_details() ->
    riak_core_console:command(["riak-admin", "handoff", "details", "-a"]). 


%% @doc Print handoff details for an node
-spec handoff_details(string()) -> ok.
handoff_details(Node) when is_list(Node) ->
    riak_core_console:command(["riak-admin", "handoff", "details", "-n", Node]). 


%% @doc Print handoff config
-spec handoff_config() -> ok.
handoff_config() ->
    riak_core_console:command(["riak-admin", "handoff", "config", "-a"]). 


%% @doc Print handoff config for a node
-spec handoff_config(string()) -> ok.
handoff_config(Node) when is_list(Node) ->
    riak_core_console:command(["riak-admin", "handoff", "config", "-n", Node]). 



%% ===================================================================
%% Stats commands
%%
%% (copied info from riak-admin)
%% The following commands display, enable/disable and reset statistics.
%% A statistics entry is given either as a 'dotted' exometer name -
%% Identifiers separated by periods, '.', e.g. riak.riak_kv.node.gets,
%% or as a 'legacy' name (same as in riak-admin status) - e.g. node_gets.
%% When a legacy name is listed, the corresponding exometer name is shown as well.
%%
%% Two kinds of wildcard are suppored:
%% *  - matches anything up to the next separator ('.' or '_') or end of name;
%% ** - matches anything including separators.
%% Quoting is permitted.
%% ===================================================================


%% @doc Show matching stats entries together with corresponding values
%%
%% <code>
%% The format of <entry> can be one of:
%% - 'Dotted exometer name': In Exometer, entries are represented as [A,B,...].
%% These names can be emulated on the command-line as A.B.... Wildcards are
%% supported: '*' will match anything between deliminators (dots), whereas
%% '**' will match anything including deliminators. Thus \`stat show \"*.**\"\`
%% will match all stats entries. All Riak stat entry names start with 'riak',
%% so \`stat show riak.**\` will match all riak stat entries.
%%
%% Example:
%% \$ bin/riak-admin stat show riak.riak_kv.node.gets
%% [riak,riak_kv,node,gets]: [{count,0},{one,0}]
%%
%% - 'Legacy name': The stat names used e.g. in \`$SCRIPT status\` can be used
%% here, but also with wildcard support. The corresponding Exometer name and
%% datapoint will be shown as well.
%%
%% Example:
%% \$ bin/riak-admin stat show node_gets
%% == node_gets (Legacy pattern): ==
%% node_gets: 0 ([riak,riak_kv,node,gets]/one)
%%
%% (Note: A single '*' is treated as a legacy name and would match all such
%% names that contain no underscores; to match all exometer names, a '.' must
%% be present, so '*.**' would work as a catch-all expression.)
%%
%% Each Exometer entry has a type and a set of datapoints. A filter can be
%% given on the command line, selecting only a subset of datapoints:
%%
%% \$ bin/riak-admin stat show riak.riak_kv.node.gets/one
%% [riak,riak_kv,node,gets]: [{one,0}]
%%
%% The type can also be restricted:
%% \$ bin/riak-admin stat show *.**/type=duration/mean,max
%% [riak,riak_core,converge_delay]: [{mean,0},{max,0}]
%% [riak,riak_core,rebalance_delay]: [{mean,0},{max,0}]
%%
%% Note how multiple datapoints are separated by comma (no space).
%%
%% Showing disabled entries:
%% \$ bin/riak-admin stat show riak.riak_kv.node.gets
%% No matching stats
%% \$ bin/riak-admin stat show riak.riak_kv.node.gets/status=*
%% [riak,riak_kv,node,gets]: disabled
%% \$ bin/riak-admin stat show riak.riak_kv.node.gets/status=disabled
%% [riak,riak_kv,node,gets]: disabled
%% </code>

-spec stat_show(string()) -> ok.
stat_show(String) ->
    riak_core_console:stat_show([String]).

 
%% @doc Display Exometer meta-data for matching entries. 
%% Type of data can be controlled
%% with options:
%%
%%    info [ -name | -type
%%         | -module
%%         | -value | -cache
%%         | -status | -timestamp
%%         | -options | -ref
%%         | -datapoints ] <entry>
%%
%% The same entry formats can be used as for all other stat subcommands.
%%
%% Example:
%% \$ bin/riak-admin stat info riak.riak_kv.node.gets
%% [riak,riak_kv,node,gets]: name = [riak,riak_kv,node,gets]
%%                           type = spiral
%%                           module = exometer_spiral
%%                           value = disabled
%%                           cache = 0
%%                           status = disabled
%%                           timestamp = undefined
%%                           options = [{status,disabled}]
%%
%% \$ bin/riak-admin stat info -type -status riak.riak_kv.node.gets
%% [riak,riak_kv,node,gets]: type = spiral
%%                           status = disabled

-spec stat_info(string()) -> any().
stat_info(String) ->
    riak_core_console:stat_info([String]).


%% @doc Enable statistics
%% Exometer stats can be disabled and enabled. Disabled entries are not actively
%% updated, and have no value.
%%
%% The same syntax can be used as in \`stat show\`. The requested action will be
%% performed on the matching entries.
%%
%% \$ bin/riak-admin stat disable node_gets
%% == node_gets (Legacy pattern): ==
%% [riak,riak_kv,node,gets]: disabled
%% \$ bin/riak-admin stat enable node_gets
%% == node_gets (Legacy pattern): ==
%% [riak,riak_kv,node,gets]: enabled
%%
%% Wildcards can be used:
%%
%% \$ bin/riak-admin stat disable riak.riak_kv.node.*
%% [riak,riak_kv,node,gets]: disabled
%% [riak,riak_kv,node,puts]: disabled

-spec stat_enable(string()) -> ok.
stat_enable(String) ->
    riak_core_console:stat_enable([String]).


%% @doc Disable statistics
-spec stat_disable(string()) -> ok.
stat_disable(String) ->
    riak_core_console:stat_disable([String]).


%% @doc Rest statistics. "**" for all.
%% Reset matching stat entries. Only enabled entries can be reset.

-spec stat_reset(string()) -> ok.
stat_reset(String) ->
    riak_core_console:stat_reset([String]).


%% ===================================================================
%% Ensemble
%% ===================================================================


ensemble_overview() ->
    nkbase_ensemble_admin:ensemble_overview().


ensemble_detail(N) ->
    nkbase_ensemble_admin:ensemble_detail(N).





%% ===================================================================
%% Internal
%% ===================================================================

atomify_nodestrs(Strs) ->
    lists:foldl(fun("local", Acc) -> [node()|Acc];
                   (NodeStr, Acc) -> try
                                         [list_to_existing_atom(NodeStr)|Acc]
                                     catch error:badarg ->
                                         io:format("Bad node: ~s\n", [NodeStr]),
                                         Acc
                                     end
                end, [], Strs).

reload_path(Path) ->
    {ok, Beams} = file:list_dir(Path),
    [ reload_file(filename:absname(Beam, Path)) || Beam <- Beams, ".beam" == filename:extension(Beam) ].

reload_file(Filename) ->
    Mod = list_to_atom(filename:basename(Filename, ".beam")),
    case code:is_loaded(Mod) of
        {file, Filename} ->
            code:soft_purge(Mod),
            {module, Mod} = code:load_file(Mod),
            io:format("Reloaded module ~w from ~s.~n", [Mod, Filename]);
        {file, Other} ->
            io:format("CONFLICT: Module ~w originally loaded from ~s, won't reload from ~s.~n", [Mod, Other, Filename]);
        _ ->
            io:format("Module ~w not yet loaded, skipped.~n", [Mod])
    end.

