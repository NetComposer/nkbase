Eventual Consistency Tutorial
=============================

This a simple tutorial covering the basic aspects of NkBASE.


Firt of all install last stable version of NkBASE:
```
> git clone https://github.com/kalta/nkbase
> cd nkbase
> make
> make dev1
```


```make dev1``` starts a single node testing environment for NkBASE.

Let's start by inserting some simple data. We will use the domain ```tutorial``` and class ```test1```

```erlang
1> nkbase:put(tutorial, test1, key1, value1, #{n=>3, backend=>ets}).
ok
```

We selected to have three copies of the object (it does not make a lot of sense for a single node, but later on we will add more nodes) and use the in-memory backend.

Now we can read the object:
```erlang
2> {ok, Ctx1, value1} = nkbase:get(tutorial, test1, key1, #{n=>3, backend=>ets}).
{ok, ..., value1}
```

Of course NkBASE returns the stored object, along with the current _causal context_ for this object. We will use it later on.

We can avoid having to include the necessary metadada defining the class at ther server:
```erlang
3> nkbase:register_class(tutorial, test1, #{n=>3, backend=>ets}).
ok
4> nkbase:get_class(tutorial, test1).
#{backend => ets,n => 3}
```

Now we can use the short version of _get_ and _put_:
```erlang
5> {ok, Ctx1, value1} = nkbase:get(tutorial, test1, key1).
{ok, ..., value1}
```

Let's update the object, using the context we know:
```erlang
6> nkbase:put(tutorial, test1, key1, value2, #{ctx=>Ctx1}).
ok
7> {ok, Ctx2, value2} = nkbase:get(tutorial, test1, key1).
{ok, ..., value2}
```

If we don't include the context, or include an old one, the server will detect a conflict and store _both_ objects:
```erlang
8> nkbase:put(tutorial, test1, key1, value3, #{ctx=>Ctx1}).
ok
9> {multi, Ctx3, [value3, value2]} = nkbase:get(tutorial, test1, key1).
{ok, ..., value2}
```

We have two options here: resolve the conflict and save again with the new context (```Ctx3```) or disable the version checking using the special option 'reconcile=lww' (last write wins). You could also use your own
conflict-resolution functions, or an automatic convergent type (see [DMaps](dmap.md)). We will also set an automatic removal in 5 secs
```erlang
10> nkbase:put(tutorial, test1, key1, value2_3, #{reconcile=>lww, ttl=>5}).
ok
11> {ok, Ctx4, value2_3} = nkbase:get(tutorial, test1, key1).
{ok, ..., value2_3}
```

After 5 seconds, the object no longer exists:
```erlang
12> nkbase:get(tutorial, test1, key1).
{error, not_found}
```

We can see the current state of the cluster:
```erlang
12> nkbase_admin:print_info()
...
```

We see that we have a single node, and all vnodes are started at this node.

Let's add more nodes, for testing purposes we can start all of them in the same machine. In different terminal windows, type:

```
> make dev2
> make dev3
> make dev4
> make dev5
```

Now we have started five different nodes. Lets go to nodes 2 to 5, and program the joining to node1:
```
node2> nkbase_admin:join('dev1@127.0.0.1').
node3> nkbase_admin:join('dev1@127.0.0.1').
node4> nkbase_admin:join('dev1@127.0.0.1').
node5> nkbase_admin:join('dev1@127.0.0.1').
```

Now all five nodes are ready to join. In any of them preview the cluster plan and launch it:
```
> nkbase_admin:plan().
...
> nkbase_admin:commit().
....
```

Now, after some seconds, we have a five-node cluster!. Try storing values with, for example n=3, and check that, afte shutting down up to two nodes, the data is still there. 

Now you can move on to the [search tutorial](tutorial_search.md).











