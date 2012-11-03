#
# Makefile to manage the graph generation
#


#
# The FGF tool
#

FGFTOOL := ../fgftool.sh

GENERATE_ARGS := -v
FGF2DEXCSV_ARGS := -v
SPLIT_ARGS := -v -w 90


#
# Krongen from http://snap.stanford.edu/snap/
#

KRONGEN := krongen
KRONGEN_ARGS := '-m:0.3 0.9; 0.75 0.25'

ifeq (x$(shell which $(KRONGEN)),x)
	HAVE_KRONGEN :=
else
	HAVE_KRONGEN := yes
endif


#
# Inputs and Outputs -- continued
#

SOURCES := $(shell ls -1 *.xml | grep -v common | grep -v kron_)
TARGETS :=
KRONGEN_TARGETS :=

ifeq (x$(HAVE_KRONGEN),xyes)
	SOURCES := $(SOURCES) \
		$(shell ls -1 *.xml | grep -v common | grep kron_)

	KRONGEN_TARGETS := kron_1k.txt kron_8k.txt kron_1m.txt kron_2m.txt
	TARGETS := $(TARGETS) $(KRONGEN_TARGETS)
endif

TARGETS_FGF    := $(subst .xml,.fgf,$(SOURCES))
TARGETS_SPLIT  := $(subst .xml,-a.fgf,$(SOURCES)) $(subst .xml,-b.fgf,$(SOURCES))
TARGETS_DEXCSV := $(subst .fgf,.fgf-dex-csvs,$(TARGETS_FGF) $(TARGETS_SPLIT))
TARGETS        := $(TARGETS) $(TARGETS_FGF) $(TARGETS_SPLIT) $(TARGETS_DEXCSV)


# Specialized / filtered targets

TARGETS_B1K   := $(filter barabasi_1k_%,$(TARGETS))
TARGETS_B10K  := $(filter barabasi_10k_%,$(TARGETS))
TARGETS_B100K := $(filter barabasi_100k_%,$(TARGETS))
TARGETS_B1M   := $(filter barabasi_1m_%,$(TARGETS))
TARGETS_B2M   := $(filter barabasi_2m_%,$(TARGETS))
TARGETS_B10M  := $(filter barabasi_10m_%,$(TARGETS))

TARGETS_K1K   := $(filter kron_1k_%,$(TARGETS))
TARGETS_K8K   := $(filter kron_8k_%,$(TARGETS))
TARGETS_K1M   := $(filter kron_1m_%,$(TARGETS))
TARGETS_K2M   := $(filter kron_2m_%,$(TARGETS))


#
# Phony targets
#

all: $(TARGETS)

clean:
	rm -rf *.fgf *.fgf-dex-csvs

b1k  : $(TARGETS_B1K)
b10k : $(TARGETS_B10K)
b100k: $(TARGETS_B100K)
b1m  : $(TARGETS_B1M)
b2m  : $(TARGETS_B2M)
b10m : $(TARGETS_B10M)

k1k  : $(TARGETS_K1K)
k8k  : $(TARGETS_K8K)
k1m  : $(TARGETS_K1M)
k2m  : $(TARGETS_K2M)

.PHONY: all clean b1k b10k b100k b1m b2m b10m k1k k8k k1m k2m


#
# Krongen targets
#

kron_1k.txt:
	$(KRONGEN) $(KRONGEN_ARGS) -o:kron_1k.txt -i:10

kron_8k.txt:
	$(KRONGEN) $(KRONGEN_ARGS) -o:kron_8k.txt -i:13

kron_1m.txt:
	$(KRONGEN) $(KRONGEN_ARGS) -o:kron_1m.txt -i:20

kron_2m.txt:
	$(KRONGEN) $(KRONGEN_ARGS) -o:kron_2m.txt -i:21

krongen: $(KRONGEN_TARGETS)

.PHONY: krongen


#
# The actual targets
#

%.fgf: %.xml common*.xml names.txt
	$(FGFTOOL) generate $(GENERATE_ARGS) -o $@ $<

%-a.fgf %-b.fgf: %.fgf common*.xml names.txt
	$(FGFTOOL) split $(SPLIT_ARGS) -o $*-a.fgf -o $*-b.fgf $<

%.fgf-dex-csvs: %.fgf
	$(FGFTOOL) fgf2dexcsv $(FGF2DEXCSV_ARGS) $< $@

