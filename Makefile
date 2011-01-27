REBAR:=rebar

.PHONY: all erl test clean doc pub sub

all: erl

erl:
	$(REBAR) get-deps compile

pub:
	ERL_LIBS=deps erl -sname publisher -pa ebin -run work_queue_p

sub1:
	ERL_LIBS=deps erl -sname consumer_1 -pa ebin -run work_queue_c

sub2:
	ERL_LIBS=deps erl -sname consumer_2 -pa ebin -run work_queue_c

test: all
	@mkdir -p .eunit
	$(REBAR) skip_deps=true eunit

clean:
	$(REBAR) clean
	-rm -rvf deps ebin doc .eunit

doc:
	$(REBAR) doc

