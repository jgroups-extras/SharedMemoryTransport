#!/bin/bash
java -classpath ./target/classes:target/dependency/* org.jgroups.tests.perf.UPerf -props shm.xml "$@";