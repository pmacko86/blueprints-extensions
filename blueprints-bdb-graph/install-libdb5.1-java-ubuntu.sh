#!/bin/sh
set -e

sudo apt-get install libdb5.1-java
mvn install:install-file -Dfile=/usr/share/java/db.jar \
	-DgroupId=com.sleepycat.db -DartifactId=libdb5.1-java -Dversion=5.1 \
	-Dpackaging=jar

