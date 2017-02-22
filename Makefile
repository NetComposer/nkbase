APP = nkbase
REBAR = rebar3

.PHONY: rel stagedevrel package version all tree shell

all: compile


version:
	@echo "$(shell git symbolic-ref HEAD 2> /dev/null | cut -b 12-)-$(shell git log --pretty=format:'%h, %ad' -1)" > $(APP).version


version_header: version
	@echo "-define(VERSION, <<\"$(shell cat $(APP).version)\">>)." > include/$(APP)_version.hrl

clean:
	$(REBAR) clean


rel:
	$(REBAR) release


compile:
	$(REBAR) compile


dialyzer:
	$(REBAR) dialyzer


xref:
	$(REBAR) xref


upgrade:
	$(REBAR) upgrade
	make tree


update:
	$(REBAR) update


tree:
	$(REBAR) tree | grep -v '=' | sed 's/ (.*//' > tree


tree-diff: tree
	git diff test -- tree


docs:
	$(REBAR) edoc


shell:
	mkdir -p data/ring
	$(REBAR) shell --config config/shell.config --name $(APP)@127.0.0.1 --setcookie nk --apps $(APP)


dev1:
	mkdir -p data/1/ring
	$(REBAR) shell --config config/dev1.config --name dev1@127.0.0.1 --setcookie nk


dev2:
	mkdir -p data/2/ring
	$(REBAR) shell --config config/dev2.config --name dev2@127.0.0.1 --setcookie nk


dev3:
	mkdir -p data/3/ring
	$(REBAR) shell --config config/dev3.config --name dev3@127.0.0.1 --setcookie nk


dev4:
	mkdir -p data/4/ring
	$(REBAR) shell --config config/dev4.config --name dev4@127.0.0.1 --setcookie nk


dev5:
	mkdir -p data/5/ring
	$(REBAR) shell --config config/dev5.config --name dev5@127.0.0.1 --setcookie nk
