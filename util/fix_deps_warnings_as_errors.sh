#!/usr/bin/env sh

sed 's/, warnings_as_errors//' deps/poolboy/rebar.config > deps/poolboy/rebar.config.1
mv deps/poolboy/rebar.config.1 deps/poolboy/rebar.config

sed 's/warnings_as_errors, //' deps/meck/rebar.config > deps/meck/rebar.config.1
mv deps/meck/rebar.config.1 deps/meck/rebar.config

sed 's/warnings_as_errors, //' deps/riak_core/rebar.config > deps/riak_core/rebar.config.1
mv deps/riak_core/rebar.config.1 deps/riak_core/rebar.config

sed 's/{require_min_otp_vsn, "R14B03"}.//' deps/pbkdf2/rebar.config > deps/pbkdf2/rebar.config.1
mv deps/pbkdf2/rebar.config.1 deps/pbkdf2/rebar.config

sed 's/, warnings_as_errors//' deps/riak_dt/rebar.config > deps/riak_dt/rebar.config.1
mv deps/riak_dt/rebar.config.1 deps/riak_dt/rebar.config
