#!/bin/bash
java -Xmx1G  -Dagrona.disable.bounds.checks=true -classpath ./target/classes:target/dependency/* \
    org.jgroups.tests.perf.ManyToOnePerf "$@";