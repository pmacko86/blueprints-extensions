
A collection of several graph specs files and random data sets that can be
used for graph generation.

names.txt:
	A collection of many random names, one per each line. Generated using
	../tools/combine.py using the given name and family name data sets
	from the US Census Bureau downloaded from the following webpages:

		http://www.ssa.gov/oact/babynames/limits.html
		http://names.mongabay.com/most_common_surnames1.htm
		
	The surname data were copied from the webpage as surnames.txt; the first
	names were taken from the 2010 census and renamed to firstnames.txt. The
	downloaded files were processed using the following commands:

		cat surnames.txt | cut -f 1 | tr '[A-Z]' '[a-z]' \
			| sed 's/\([a-z]\)\([a-zA-Z0-9]*\)/\u\1\2/g' > s.txt
		cat firstnames.txt | cut -d, -f 1 | tr '[A-Z]' '[a-z]' \
			| sed 's/\([a-z]\)\([a-zA-Z0-9]*\)/\u\1\2/g' > f.txt

	And then they were combined using the aforementioned script.

