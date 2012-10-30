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
TARGETS        := $(TARGETS_FGF) $(TARGETS_SPLIT) $(TARGETS_DEXCSV)


# Specialized / filtered targets

TARGETS_1K   := $(filter barabasi_1k_%,$(TARGETS))
TARGETS_10K  := $(filter barabasi_10k_%,$(TARGETS))
TARGETS_100K := $(filter barabasi_100k_%,$(TARGETS))
TARGETS_1M   := $(filter barabasi_1m_%,$(TARGETS))
TARGETS_2M   := $(filter barabasi_2m_%,$(TARGETS))
TARGETS_10M  := $(filter barabasi_10m_%,$(TARGETS))


#
# Phony targets
#

all: $(TARGETS)

clean:
	rm -rf *.fgf *.fgf-dex-csvs

1k  : $(TARGETS_1K)
10k : $(TARGETS_10K)
100k: $(TARGETS_100K)
1m  : $(TARGETS_1M)
2m  : $(TARGETS_2M)
10m : $(TARGETS_10M)

.PHONY: all clean 1k 10k 100k 1m 2m 10m


#
# The actual targets
#

%.fgf: %.xml common*.xml names.txt
	$(FGFTOOL) generate $(GENERATE_ARGS) -o $@ $<

%-a.fgf %-b.fgf: %.fgf common*.xml names.txt
	$(FGFTOOL) split $(SPLIT_ARGS) -o $*-a.fgf -o $*-b.fgf $<

%.fgf-dex-csvs: %.fgf
	$(FGFTOOL) fgf2dexcsv $(FGF2DEXCSV_ARGS) $< $@

