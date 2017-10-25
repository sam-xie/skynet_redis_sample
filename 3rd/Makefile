libdir := ./lib
$(shell mkdir -p $(libdir))

LIBS := lfs cjson

all: $(foreach v, $(LIBS), $(libdir)/$(v).so)

clean: 
	$(foreach v, $(LIBS), rm -rf $(libdir)/$(v).so)
.PHONY: clean

$(libdir)/lfs.so:
	cd luafilesystem && make clean && make
	cp luafilesystem/src/lfs.so $@

$(libdir)/cjson.so:
	cd lua-cjson && make clean && make
	cp lua-cjson/cjson.so $@
