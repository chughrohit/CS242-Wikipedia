#!/bin/bash

#
# INSTRUCTIONS TO RUN THIS FILE
#
# Run this file with the following two positional parameters
# PARAM1: Location of the index
# PARAM2: Location of the data dump
#
javac WikiIndexer.java

java -classpath . WikiIndexer $1 $2