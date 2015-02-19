Search Tutorial
===============


Firt of all install last stable version of NkBASE:

```
> git clone https://github.com/kalta/nkbase
> cd nkbase
> make
> make build_tests
> make dev1
```

You could also add more nodes to the cluster as described in the [basic tutorial](tutorial_basic.md)

First, let's create a class for our objects (you can copy and paste to the Erlang console):

```erlang
> nkbase:register_class(tutorial, test2, #{
		backend => ets,
		indices => [
			{i_key, key},
			{i_name, {field, name}},
			{i_surname, {field, surname}},
			{i_names, {field, fullname}, [normalize, words]}
		],
		reconcile => lww
  }).
ok
```

We use the reconcile option to use the 'last-write-wins' strategy and avoid siblings.

Our objects will have four indices:
* Index 'i_key', baded on the object's key
* Index 'i_name', based on the field 'name'
* Index 'i_surname', based on the field 'surname'
* Index 'i_names', based on the field 'fullname' but normalizing it and splitting words.

Now lets insert some random objects, getting random names. The function ```test_util:get_name()``` will return a random three-word spanish name, including non-ASCII chars like accents.

```erlang
> lists:foreach(
	fun(Pos) ->
		{Name1, Name2, Name3} = test_util:get_name(),
		Obj = #{
			name => Name1,
			surname => list_to_binary([Name2, " ", Name3]),
			fullname => list_to_binary([Name1, " ", Name2, " ", Name3])
		},
		ok = nkbase:put(tutorial, test2, {k, Pos}, Obj)
	end,
	lists:seq(1, 1000)).
ok
```

Now we have 1000 objects in the database, and we start searching. Lets find people having the expression 'go*' in any part of their names:

```erlang
> nkbase:search(tutorial, test2, [{i_names, "ga*"}]).
{ok, [{<<"ga...">>, {k, ..}, []}, ...]}
```

And, among the, wich have a key <= {k, 200} or > {k, 500}:
```erlang
> nkbase:search(tutorial, test2, [{i_names, "ga*"}, {i_key, [{le, {k,200}}, {gt, {k, 500}}]}]).
{ok, [{<<"ga...">>, {k, ..}, []}, ...]}
```

Let's perform the same query, but returning not only the key but also the 'name' field
```erlang
> nkbase:search(tutorial, test2, [{i_names, "ga*"}, {i_key, [{le, {k,200}}, {gt, {k, 500}}]}], 
				#{get_fields=>[name]}).
{ok, [{<<"ga...">>, {k, ..}, [#{fields=>#{name=>...}}}, ...]}
```








