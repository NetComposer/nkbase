-ifndef(NKBASE_HRL_).
-define(NKBASE_HRL_, 1).

-define(DEFAULT_N, 3).
-define(DEFAULT_BACKEND, leveldb).
-define(DEFAULT_PAGE_SIZE, 1000).
-define(DEFAULT_TIMEOUT, 30).
-define(DEFAULT_DEL_TTL, 3600).

-define(CLASS_PREFIX, {nkbase, class}).

-define(EXP_IDX, {'$g', exp}).
-define(ERL_LOW, -1.0e99).
-define(ERL_HIGH, <<255>>).

-record(fold_spec, {
	backend  :: nkbase:backend(),
	domain = '$nk_all' :: '$nk_all' | nkbase:domain(),
	class = '$nk_all' :: '$nk_all' | nkbase:class(),
	start = '$nk_first' :: '$nk_first' | nkbase:key(),
	stop = '$nk_last':: '$nk_last' | nkbase:key(),
	fold_type = keys :: keys | values,
	fold_fun :: fun((nkbase:ext_key(), [nkbase:ext_obj()], term()) -> term()),
	fold_acc :: term(),
	max_items = 10000 :: undefined | pos_integer(),
	timeout = ?DEFAULT_TIMEOUT :: pos_integer(),		% secs
	n = ?DEFAULT_N :: pos_integer(),
	db_ref :: nkbase_backend:db_ref(),
	keyfilter :: undefined | fun((nkbase:ext_key()) -> boolean())
}).

-record(search_spec, {
	backend :: nkbase:backend(),
	domain :: nkbase:domain(),
	class :: nkbase:class(),
	next :: {IndexValue::term(), Key::nkbase:key()},
	order :: asc | desc,
	indices :: nkbase_search:ext_search_spec(),
	fold_type = keys :: keys | values,
	% get_values = false :: boolean() | map(),
	fold_fun :: 
		fun((nkbase:ext_key(), Value::term(), [nkbase:ext_obj()], term()) -> term()),
	fold_acc :: term(),
	max_items = 10000 :: pos_integer(),
	timeout = ?DEFAULT_TIMEOUT :: pos_integer(),
	n :: pos_integer(),
	db_ref :: nkbase_backend:db_ref(),
	keyfilter :: undefined | fun((nkbase:ext_key()) -> boolean())
}).

-record(nk_ens, {
	eseq :: nkbase_sc:eseq(),
	key :: nkbase:ext_key(),
	val :: nkbase:ext_obj() | notfound
}).


-endif.

