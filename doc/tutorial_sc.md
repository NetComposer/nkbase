Strong Consistency Tutorial
===========================

This a simple tutorial covering the strong consistency mode of NkBASE. 

Firt of all install last stable version of NkBASE:

```
> git clone https://github.com/kalta/nkbase
> cd nkbase
> make
> make dev1
```

You could also add more nodes to the cluster as described in the [basic tutorial](tutorial_basic.md).

Let's start the strong consistency subsystem:
```erlang
1> nkbase_ensemble:enable().
ok (after a while)
```

Let's start by inserting some data:

```erlang
1> nkbase_sc:kget(tutorial, test4, obj1).      
{error, not_found}

2> {ok, Seq1} = nkbase_sc:kput(tutorial, test4, obj1, value1).
{ok, ...}

3> {ok, Seq1, value1} = nkbase_sc:kget(tutorial, test4, obj1).
{ok, ..., value1}
```

Now, if we try to write to the same object without the right sequence, it will fail:

```erlang
4> nkbase_sc:kput(tutorial, test4, obj1, value2).
{error, failed}

5> {ok, Seq2} = nkbase_sc:kput(tutorial, test4, obj1, value2, #{eseq=>Seq1}).
{ok, ...}

6> nkbase_sc:kput(tutorial, test4, obj1, value3, #{eseq=>Seq1}).
{error, failed}
```

We could also overwrite it:

```erlang
7> {ok, Seq3} = nkbase_sc:kput(tutorial, test4, obj1, value3, #{eseq=>overwrite}),
{ok, ...}

8> {ok, Seq3, value3} = nkbase_sc:kget(tutorial, test4, obj1).
{ok, ..., value3}

9> nkbase_sc:kdel(tutorial, test4, obj1, #{eseq=>Seq3}).
ok

10> nkbase_sc:kget(tutorial, test4, obj1).      
{error, not_found}
```