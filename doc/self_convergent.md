# Self Convergent Mode

See [Introduction and Concepts](concepts.md) for and introduction to NkBASE, the eventual consistent system and the definition of classes. Since this mode is a special case of the eventually consistent mode, see also [Eventually Consistent Mode](eventually_consistent.md).

* [Introduction](#introduction)
* [Write Operation](#write-operation)
* [Read Operation](#read-operation)
* [Delete Operation](#delete-operation)
* [Listing Domains, Classes and Keys](#listing-domains-class-and-keys)
* [Examples](#examples)


## Introduction

Using this mode, you no longer send full objects to be stored at the database, but a set of _modifications_ to be performed over the current object, if it exists. If not, an _empty_ object is used as a base to perform modifications.

This special object is called a _dmap_. A _dmap_ is an automatically convergent data structure, that can have any number of _fields_. Every field has a _type_ and a _value_. Currently supported types and possible values are:

Type|Values
---|---
flag|`enabled` or `disabled`
register|`any()`
counter|`integer()`
set|`list()` (with unique elements)
map|nested _dmap_

You can perform a number of operations on a dmap to modify the value of any of its fields, add new fields or remove them. Depending on the type of field, you can describe a set of operations to be performed on it. If you perform an operation that is not allowed for the current type of the field, it will fail.


### Flag

You can perform the following operations on fields of type _flag_:

* `enable`: the field will switch to _enabled_.
* `disable`: the field will switch to _disabled_. If you use a _dcontext_ (see bellow), the flag will not switch to _disabled_ until all _enables_ have been _disabled_.
* `remove_flag`: remove the field (same note about using the _dcontext_).

If a `enable` and `disable` is simultaneously ordered, the field will remain _enabled_.

### Register

You can perform the following operations on fields of type _register_:

* `{assign, term()}`: _assigns_ a new value to the field. Last (using wallet time) assigned value wins.
* `{assign, term(), integer()}`: assigns a new value to the field, but using your own concept of _last_, indicating the time to use.
* `remove_register`: removes the field.

### Counter

You can perform the following operations on fields of type _counter_:

* `increment`: increments the counter by one.
* `{increment, integer()}`: increments the counter.
* `decrement`: decrements the counter by one.
* `{decrement, integer()}`: decrements the counter.
* `remove_counter`: removes the field.

If increments and decremenrs are simultaneously ordered, the resulting value is stored.

### Set

You can perform the following operations on fields of type _set_:

* `{add, term()}`: Adds this element to the set, if it is not yet present.
* `{remove, term()}`: Removes this element from the set, if present.
* `{add_all, list()}`: Adds all these elements to the set (atomic operation).
* `{remove_all, list()}`: Removes all these elements from the set (atomic operation).
* `remove_set`:	Removes the full set.

If an element is added and removed simultaneously, it remains in the set. 
If you try to remove and element that does not exist, you get an error, unless you use a _dcontext_.

### Map

You can perform the following operations on fields of type _map_:

* `list()`: a new, nested map is created, and you can describe the operations to apply to it, using the previous types and behaviours. If the map already exists, the operations are applied to its fields.
* `remove_map`: remove the map and all of its nested fields.

If an element in the map is updated and the map is removed at the same time, the map continues but only with the updated elements. If you remove and element that does not exist, you get an error, unless you use
a _dcontext_.

### Using dcontexts

Dcontexts are only used for _disables_ and _removals_. You can obtain the current dcontext of a dmap calling `nkbase_dmap:get/3,4`, and the the current _dcontext_ will be present in the special `_dcontext` field.

Normally, if you try to remove an element in a set or map that does no exist, you will get an error (it is supposed that other client must have removed it). You can indicate the _dcontext_ in the update list, adding `{'_dcontext', DContext}`. This way, you are saying that you indeed know the object you are modifying, and that it is safe to remove the field.


## Write operation

```erlang
-spec nkbase_dmap:update(nkbase:domain(), nkbase:class(), nkbase:key(), update_spec()) ->
	ok | {error, term()}.

-spec nkbase_dmap:update(nkbase:domain(), nkbase:class(), nkbase:key(), 
			 nkbase_dmap:update_spec(), nkbase:put_meta()) ->
	ok | {error, term()}.
```

Use these functions to send a group of modifications to a _dmap_ object, existing or not. The object is retrieved at the first vnode, and after appling the modifications, a new object is generated, that is indexed,
stored and sent to the rest of vnodes. 

Any conflict is automatically resolved with the rules described above. You can't specify the object's context (it is read from the base object), but you can include the dmap's _dcontext_ (see above).

Indices can be added to the object (see [search](search.md))


## Read operation

```erlang
-type type() :: flag | register | counter | set | map.

-type reply() :: 
	#{
		term() => {type(), term()}	
	}
	|
	#{
		fields => #{ term() => {type(), term()}},
		indices => #{ nkbase:index_name() => [term()]}
	}.

-spec get(nkbase:domain(), nkbase:class(), nkbase:key()) ->
	{ok, reply()} | {error, term()}.

-spec get(nkbase:domain(), nkbase:class(), nkbase:key(), nkbase:get_meta()) ->
	{ok, nkbase:reply()} | {error, term()}.
```

These functions are very similar to `nkbase:get/3,4` (see Operation at [Eventually Consistent Mode](eventually_consistent.md#read-operation), but assumes requested object is a _dmap_, resolving conflicts on read automatically. 

By default it returns the full description of the dmap, along with the _dcontext_. You can however indicate specific fields or indices to be returned (see [get specification](eventually_consistent.md#get-specification)).


## Delete operation

```erlang
-spec del(nkbase:domain(), nkbase:class(), nkbase:key()) ->
	ok | {error, term()}.

-spec del(nkbase:domain(), nkbase:class(), nkbase:key(), nkbase:put_meta()) ->
	ok | {error, term()}.
```

Use these functions to _delete_ a dmap. It will find an existing dmap, remove all of its fields, and add a `ttl` to schedule its removal (see Delete Operation at [Eventually Consistent Mode](eventually_consistent.md#delete-operation)).


## Listing Domains, Classes and Keys

You can use the same functions for [Eventually Consistent Mode](eventually_consistent.md#listing-domains-classes-and-keys).


## Examples

We start updating a _new_ object:

```erlang
> nkbase_dmap:get(domain, class, dkey).
{error, not_found}

> nkbase_dmap:update(domain, class, dkey, 
	[
		{field1, enable},						% it is a flag
		{field2, increment},					% it is a counter
		{field3, {assign, "hi"}},				% it is a register
		{field4, {add_all, [1, a, <<"b">>]}},	% it is a set
		{field5, 								% it is a nested map
			[
				{field5a, {decrement, 5}}
			]
		}			
	]).
ok

> {ok, Dmap1} = nkbase_dmap:get(domain, class, dkey).
{ok, 
	#{
		'_dcontext' => ...,
  		field1 => {flag, enable},
      	field2 => {counter, 1},
      	field3 => {register, "hi"},
      	field4 => {set, [1, a, <<"b">>]},
      	field5 => {map, #{field4a => {counter, -5}}}}}
    }
}
```

Now we can send more updates, and get specific fields:

```erlang
> nkbase_dmap:update(domain, class, dkey, 
	[
		{field2, increment},
		{field2, increment},
		{field3, remove_register},
		{field4, {remove, a}},
		{field5, [{field5a, decrement}]
		}
	]).
ok

> {ok, Dmap2} = nkbase_dmap:get(domain, class, dkey).
{ok,
	#{
		'_dcontext' => ...,
      	field1 => {flag, enabled},
      	field2 => {counter, 3},
        field4 => {set, [1, <<"b">>]},
        field5 => {map, #{field5a => {counter, -6}}}}}
    }
}

> nkbase_dmap:get(domain, class, key, #{get_fields=>[{field5, field5a}]}).
{ok, #{fields => #{{field5,field5a} => -6}}}
```

If we try to remove a non-existing element it will fail unless we use the _dcontext_:

```erlang
> nkbase_dmap:update(domain, class, dkey, 
	[
		{field4, {remove, 2}}
	]).
{error, {field_not_present,2}}

> nkbase_dmap:update(domain, class, dkey, 
	[
		{field4, {remove, 2}},
		{'_dcontext', maps:get('_dcontext', Dmap2)}
	]).
ok
```

Finally, we can delete the object, lowering the default ttl to 5 seconds:

```erlang
> nkbase_dmap:del(domain, class, dkey, #{ttl=>5}).
ok

> nkbase_dmap:get(domain, class, dkey).
{ok, #{'_dcontext' => ...}} 

%% After 5 seconds:
> nkbase_dmap:get(domain, class, dkey).
{error, not_found}
```
