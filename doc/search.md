# Search System

NkBASE includes a simple but flexible search engine that can be used in the three supported modes (eventual consistency, self-convergent and strong consistency).

When storing an object, you can supply any number of indices, each of them with a single o multiple values. This indices are stored along the object at each vnode. If the object is updated with different indices, the old ones are removed. If the object is deleted, indices are also deleted.

Later on, you can perform simple or quite complex query operations over these indices.

* [Adding Indices](#adding-indices)
* [Simple queries](#simple-queries)
* [Text queries](#simple-text-queries)
* [Get Specification](#get-specification)
* [Complex queries](#complex-queries)


## Adding indices

You can add any number of indices to an object using the `indices` option when calling [`nkbase:put/4,5`](eventually_consistent.md#write-operation), [`nkase_dmap:update/4,5`](self_convergent.md#write-operation) or [`nkbase_sc:kput/4,5`](strong_consistency.md#write-operation).

The specification for it is `#{indices => [nkbase:index_spec()]}`:

```erlang
-type index_spec() :: 
	{index_name(), index_spec_type()} | 
	{index_name(), index_spec_type(), [index_spec_opts()]}.

-type index_name() :: 
	term().

-type index_spec_type() :: 
	key | {field, term()|tuple()} | {func, fun((ext_key(), obj()) -> term()|[term()])} |
	term() | [term()].

-type index_spec_opts() :: 
	normalize | words.
```

You can associate with any _index_name_ a single erlang value, or a list of values. In the second case, searching over any of the will find the object. You can also use the `{func, fun/2}` option, and NkBASE will call this function before saving the object, that must return the index value or values.

You can use the `key` option to use the key of the object as the index value.

If the stored object is an Erlang `map()` or `proplist()`, you can use the `{field, Field::term()|tuple()}` form, where _Field_ must a field of the map or list of tuples. If Field is a tuple, it is used to find nested fields in maps or list of tuples.

If you use the `normalize` option, NkBASE will normalize the index value, converting it to lowercase and replacing non-standard _utf8_ and _latin1_ characters to their ASCII simple equivalents. If you use the `words` option, NkBASE will split the index value on spaces, generating a new index value for each word.

### Examples

Consider the following index specification:

```erlang
> erlang:register_class(domain, class, 
	#{
		indices => 
			[
				{i_1, key},
				{i_2, <<"my index">>},
				{i_3, {field, field_1}, [normalize, words]},
				{i_4, {field, {field_2, field_22}}}
			]
	}).
```

Then, if you store the following object:
```erlang
> nkbase:put(domain, class, my_key, 
	#{
		field1 => <<"Word1 Wórd2",
		field2 => #{field22 => <<"Word3 Word4">>}
	}).
```

It would associate the following indices with the object:
```erlang
[
	{i_1, [my_key]},
	{i_2, [<<"my index">>],
	{i_3, [<<"word1">>, <<"word2">>]},
	{i_4, [<<"Word3 Word4">>]}
]
```

It would have been the same if using a `proplist()` object instead of a `map()` object:

```erlang
> nkbase:put(domain, class, my_key, 
	[
		{field1, <<"Word1 Wórd2">>},
		{field2, [{field22, <<"Word3 Word4">>}]}
	])
```

## Simple queries

```erlang
-type search_spec() ::
	{index_name(), search_filter() | [search_filter()] | string() | binary()}.

-type search_filter() ::
	all | {eq, term()} | {ne, term()} | {lt, term()} | {le, term()} |
	{gt, term()} | {ge, term()} | {range, term(), term()} | {re, binary()}.

-spec nkbase:search(nkbase:domain(), nkbase:class(), 
					nkbase:search_spec()|[nkbase:search_spec()]) ->
	{ok, [{term(), nkbase:key(), [nkbase:reply()]} | {error, term()}.

-spec nkbase:search(nkbase:domain(), nkbase:class(), 
					nkbase:search_spec()|[nkbase:search_spec()],
					search_meta()) ->
	{ok, [{term(), nkbase:key(), [nkbase:reply()]} | {error, term()}.
```

You must use the `nkbase:search/3,4` functions to launch a query. In the longer version, you can add some metadata:

Parameter|Type|Default|Description
---|---|---|---
backend|`ets|leveldb`|`leveldb`|Backend to use
n|`1..5`|3|Number of stored copies of the objects
timeout|`integer()`|`30`|Time to wait for the write operation
order|`asc|desc`|`asc`|Ascending or descending
page_size|`integer()`|`1000`|Number of objects to return
next|`{term(), nkbase:key()}`|`undefined`|Continue the search at this point
get_values|`boolean()`|`false`|Receive the full object
get_fields|`[term()|tuple()]`|`undefined`|Receive these fields instead of the full object
get_indices|`[nkbase:index_name()]`|`undefined`|Receive these indices instead of the full object

For a single query, you must select a single index and a filter, for example:

```erlang
nkbase:search(domain, class, {index1, {ge, 123}}).
{ok, [...]}
```

will find all objects with an _index1_ index with value greater or equal than 123. Since you can use any erlang term for index values, normal Erlang sort order is applied here. For each found object, a tuple of three elements is returned: the found value, the object's key and the returned info about the object (by default `[]`). The results are sorted over the index value.

The available filters are:

Filter|Description
---|---
all|Matches any object (having this index)
{eq, Value}|Matches only this specific value as index value
{ne, Value}|Matches objects not having this value as index value
{lt, Value}|Matches if it has a lower value
{le, Value}|Matches if it has a lower or equal value
{gt, Value}|Matches if it has a higher value
{ge, Value}|Matches if it has a higher or equal value
{range, Value1, Value2}|Matches with index value >= Value1 and =< Value2
{re, Re}|Applies a regular expression to the object value (must be a `binary()` or `string())

Except for the regular expression filter, all of the rest are very fast, since indices are stored already sorted on all backends and NkBASE can go directly to the first value matching the filter. 

You can use the `page_size` option to limit the number of results. If you want more results, you can launch a new query using the `next` option and selecting the the first value and key you want to find. You can also find in reverse order using the `order` option.


## Get specification

By default, `nkbase:search/3,4` will only return the found index value, the key and an empty list. You can use the same [get specification](eventually_consistent.md#get-specification) defined for _gets_ in order to get specific fields or index values from the found object.

When using this option, NkBASE will always return a list of maps, because there could be several different objects stored under this key (in case of conflict). NkBASE will only return a map for each objects that match the query.

Keep in mind thatm if you order to reply specific fields or index values, NkBASE must read the full object for each matching key, so it will be slower.


## Simple text queries

If the index values to find happen to be binaries, you can use a different query language, for example:

```erlang
nkbase:search(domain, class, {index1, "header*"})
{ok, [...]}
```

will find all objects with an _index1_ value that is a binary starting with `<<"header">>`. The available options and their equivalents are described here:

Text mode|Filter equivalent
---|---
"*"|all
"header *"|{range, <<"header">>, <<"header", 255>>}
"* tail"|{re, <<"tail$">>}
"* any *"|{re, <<"any">>}
" > text"|{gt, <<"text"}
" >= text"|{ge, <<"text"}
" < text"|{lt, <<"text"}
" <= text"|{le, <<"text"}
" <> text"|{ne, <<"text"}
"value1-value"|{range, <<"value1">>, <<"value2", 255>>}
"re(my_re)"|{re, <<"my_re">>}

You can use any whitespace. "header*" and " header  *  " will get the same result.


## Complex queries

You can perform a **logical OR** using a list of filters instead of a single filter, for example:

```erlang
nkbase:search(domain, class, {index1, [{eq, 1}, {eq, 10}, {range 15-20}]}).
{ok, [...]}
```

will find objects having value for _index1_ equal to 1, 10 or in the range 15-20. This kind of operations is also pretty fast, since NkBASE detects when iterating that must jump from 2 to 10 in the previous example, and from 11 to 15.

You can perform a **logical AND** using a list of indices (possible with a list of filters) instead of a single index, for example: 

```erlang
nkbase:search(domain, class, [{index1, [{eq, 1}, {eq, 10}]}, {index2, {eq, <<"good">>}}]).
{ok, [...]}
```

wil find objects having an index1 with value 1 or 10 and an index2 with value <<"good">>. The resulting list will be sorted over the first index.

With this tools, you can launch very complex query operations over multiple indices.

For this king of queries, NkBASE iterates over the first index, and, for the objects mathing the first index, must read the full object from disk and check if the rest of indices also match, so it can be quite slow.

The worst case would be that the first index matches many objects, but any of the next indices only matches objects that happen to be very far according to the first index order.





