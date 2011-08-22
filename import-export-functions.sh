#!/bin/bash

trim() { echo $1|sed 's/^\s\+\|\s\+$//g'; }
bacula_exec() { echo "$@" | bconsole -s -n > /dev/null; }
bacula_check_running() {
        running=$(echo "list jobs" | bconsole -s -n | grep R | awk -F\| '{print $9}' | grep R)
        if [ -n "$running" ]; then
                echo "Bacula has currently running jobs, aborting!"
                exit 1
        fi
}

echo_f() {
	if [ -n "$jobsqueue" -a -n "$jobsdone" ]; then
		echo
		echo "-- $jobsdone/$jobsqueue -- $@ --";
	else
		echo
		echo "-- $@ --" 
	fi
}

bconsole_import() {
        bconsole << END_OF_DATA
@output /tmp/import-$d.log
@time
label barcodes slots=$t drive=$d pool=${pools[$p]}
yes
wait
mount drive=$d slot=
wait
quit
END_OF_DATA
	((queue_finished++))
}
