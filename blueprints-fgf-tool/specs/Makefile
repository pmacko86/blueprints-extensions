#
# Makefile to manage the graph generation
#


#
# Tools
#

FGFTOOL := ../fgftool.sh

GENERATE_ARGS := -v
FGF2DEXCSV_ARGS := -v
SPLIT_ARGS := -v -w 90


#
# Inputs and Outputs
#

SOURCES := $(shell ls -1 *.xml | grep -v common) 

TARGETS_FGF    := $(subst .xml,.fgf,$(SOURCES))
TARGETS_SPLIT  := $(subst .xml,-a.fgf,$(SOURCES)) $(subst .xml,-b.fgf,$(SOURCES))
TARGETS_DEXCSV := $(subst .fgf,.fgf-dex-csvs,$(TARGETS_FGF) $(TARGETS_SPLIT))

TARGETS    := $(TARGETS_FGF) $(TARGETS_SPLIT) $(TARGETS_DEXCSV)
TARGETS_1K := $(filter barabasi_1k_%,$(TARGETS))


#
# Phony targets
#

all: $(TARGETS)

1k: $(TARGETS_1K)

clean:
	rm -rf *.fgf *.fgf-dex-csvs

.PHONY: all 1k clean


#
# The actual targets
#

%.fgf: %.xml common*.xml names.txt
	$(FGFTOOL) generate $(GENERATE_ARGS) -o $@ $<

%-a.fgf %-b.fgf: %.fgf common*.xml names.txt
	$(FGFTOOL) split $(SPLIT_ARGS) -o $*-a.fgf -o $*-b.fgf $<

%.fgf-dex-csvs: %.fgf
	$(FGFTOOL) fgf2dexcsv $(FGF2DEXCSV_ARGS) $< $@

