REBAR = rebar3
NKDIST = _build/default/lib/nkdist

.PHONY: rel stagedevrel package version all tree shell

all: compile


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
	$(REBAR) shell --config $(NKDIST)/config/shell.config --name nkbase@127.0.0.1 --setcookie nk


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
