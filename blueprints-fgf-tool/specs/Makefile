#
# Makefile to manage the graph generation
#

TARGETS := $(subst .xml,.fgf,$(shell ls -1 *.xml | grep -v common))
FGFTOOL := ../fgftool.sh

all: $(TARGETS)

clean:
	rm -rf *.fgf *.fgf-dex-csvs

.PHONY: all clean

%.fgf: %.xml common*.xml names.txt
	$(FGFTOOL) generate -v -o $@ $<
	$(FGFTOOL) fgf2dexcsv -v $<

