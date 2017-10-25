confpath := ./example/snconfig
confpath := $(shell pwd)/$(confpath)
skynetdir := ./skynet

all: $(skynetdir)/skynet
	cd 3rd && make

$(skynetdir)/skynet: 
	cd $(skynetdir) && make linux

start: 
	@cd $(skynetdir) && ./skynet $(confpath)

.PHONY: start
