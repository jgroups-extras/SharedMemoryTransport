#!/bin/bash
java -classpath ./target/classes:target/dependency/* org.jgroups.demos.Draw -props shm.xml "$@";