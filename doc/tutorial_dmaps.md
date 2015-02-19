DMAPs Tutorial
==============

This a simple tutorial covering the self-convergent mode of NkBASE. 

Firt of all install last stable version of NkBASE:

```
> git clone https://github.com/kalta/nkbase
> cd nkbase
> make
> make dev1
```

You could also add more nodes to the cluster as described in the [basic tutorial](tutorial_basic.md).


Let's start by generating a new dmap with some data:

```erlang
1> nkbase_dmap:get(tutorial, test3, obj1).      
{error, not_found}

2> nkbase_dmap:update(tutorial, test3, obj1, [
	{field_1, {assign, "value1"}},
	{field_2, {increment, 10}},
	{field_3, {add_all, [1, b, "c"]}}
   ]).
ok

3> nkbase_dmap:get(tutorial, test3, obj1).
{ok, #{
	_context => ...,
	field_1 => {register,"value1"},
    field_2 => {counter,10},
    field_3 => {set,[1,b,"c"]}}}
}
```

We have generated a dmap with a register, a counter and a set. We can now send some updates:

```erlang
4> nkbase_dmap:update(tutorial, test3, obj1, [
	{field_2, {increment, -5}},
	{field_3, {add, f}},
	{field_3, {remove, 1}}
   ]).
ok

3> nkbase_dmap:get(tutorial, test3, obj1).
{ok, #{
	_context => ...,
	field_1 => {register,"value1"},
    field_2 => {counter,5},
    field_3 => {set,[b,f,"c"]]}}}
}
```
