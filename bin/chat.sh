#!/bin/bash
java -classpath ./target/classes:target/dependency/* org.jgroups.demos.Chat -props shm.xml "$@";