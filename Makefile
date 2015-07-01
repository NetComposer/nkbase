REPO ?= nkbase

.PHONY: rel stagedevrel deps release

all: deps compile

compile:
	./rebar compile

compile-no-deps:
	./rebar compile skip_deps=true

deps:
	./rebar get-deps
	# util/fix_deps_warnings_as_errors.sh

clean: 
	./rebar clean

distclean: clean
	./rebar delete-deps

tests: compile eunit

eunit:
	export ERL_FLAGS="-config test/app.config -args_file test/vm.args"; \
	./rebar eunit skip_deps=true

shell:
	erl -config util/shell_app.config -args_file util/shell_vm.args -s nkbase_app

dev_clean:
	rm -rf dev/1/data
	rm -rf dev/2/data
	rm -rf dev/3/data
	rm -rf dev/4/data
	rm -rf dev/5/data

dev1:
	erl -config util/dev1.config -args_file util/dev_vm.args \
		-name dev1@127.0.0.1 -s nkbase_app

dev1-master:
	erl -config util/dev1.config -args_file util/dev_vm.args \
		-name dev1@127.0.0.1 -s nkbase_app -force_master

dev2:
	erl -config util/dev2.config -args_file util/dev_vm.args \
	    -name dev2@127.0.0.1 -s nkbase_app

dev3:
	erl -config util/dev3.config -args_file util/dev_vm.args \
	    -name dev3@127.0.0.1 -s nkbase_app

dev4:
	erl -config util/dev4.config -args_file util/dev_vm.args \
	    -name dev4@127.0.0.1 -s nkbase_app

dev5:
	erl -config util/dev5.config -args_file util/dev_vm.args \
	    -name dev5@127.0.0.1 -s nkbase_app


# Still failling
# docs:
# 	./rebar skip_deps=true doc
# 	@cp -R apps/riak_core/doc doc/riak_core


APPS = kernel stdlib sasl erts ssl tools os_mon runtime_tools crypto inets \
	xmerl webtool snmp public_key mnesia eunit syntax_tools compiler
COMBO_PLT = $(HOME)/.$(REPO)_combo_dialyzer_plt

check_plt: 
	dialyzer --check_plt --plt $(COMBO_PLT) --apps $(APPS) deps/*/ebin

build_plt: 
	dialyzer --build_plt --output_plt $(COMBO_PLT) --apps $(APPS) deps/*/ebin

dialyzer:
	dialyzer -Wno_return --plt $(COMBO_PLT) ebin/nkbase*.beam | \
	    fgrep -v -f ./dialyzer.ignore-warnings

cleanplt:
	@echo 
	@echo "Are you sure?  It takes about 1/2 hour to re-build."
	@echo Deleting $(COMBO_PLT) in 5 seconds.
	@echo 
	sleep 5
	rm $(COMBO_PLT)


build_tests:
	erlc -pa ebin -pa deps/lager/ebin -o ebin -I include \
	+export_all +debug_info +"{parse_transform, lager_transform}" \
	test/*.erl
