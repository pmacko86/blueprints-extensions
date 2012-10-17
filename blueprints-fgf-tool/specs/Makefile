#
# Makefile to manage the graph generation
#

TARGETS := $(subst .xml,.fgf-dex-csvs,$(shell ls -1 *.xml | grep -v common))
FGFTOOL := ../fgftool.sh

.PHONY: all clean

all: $(TARGETS)

clean:
	rm -rf *.fgf *.fgf-dex-csvs

%.fgf: %.xml common*.xml names.txt
	$(FGFTOOL) generate -v -o $@ $<

%.fgf-dex-csvs: %.fgf
	$(FGFTOOL) fgf2dexcsv -v $< $@

