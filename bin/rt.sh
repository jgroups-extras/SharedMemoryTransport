#!/bin/bash
java -classpath ./target/classes:target/dependency/* org.jgroups.tests.RoundTrip "$@";