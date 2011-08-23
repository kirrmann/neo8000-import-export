#!/bin/bash

source "import-export-functions.sh" || exit 1
source "import-export.conf" || exit 1

bacula_check_running

# TODO unload drives again?

transfers=0
pools=(Inc Full Diff Inc)

declare -a emptyslots=($($mtxchanger $storage listall | grep '^S:.*:E$' | awk -F: '{print $2}'))
declare -a mailslots=($($mtxchanger $storage listall | grep '^I:.*:F:' | awk -F: '{print $2}'))

if [ ${#mailslots[*]} -gt ${#emptyslots[*]} ]; then
	transfers=${#emptyslots[*]}
else
	transfers=${#mailslots[*]}
fi

echo_f "statistics"
jobqueue=3
jobsdone=1
echo "Empty slots found: ${#emptyslots[*]}"
echo "Mailslots used ${#mailslots[*]} from 15"

echo_f "starting transfer"
((jobsdone++))
mindex=0
for e in ${emptyslots[@]}; do
	transfer=$(($mindex+1))
	echo "($transfer/$transfers) moving tape from ${mailslots[$mindex]} to $e"
	$mtxchanger $storage transfer ${mailslots[$mindex]} $e
	((mindex++))
done

echo_f "updating bacula volume names"
((jobsdone++))
c=`chunks`
newtapes=($(echo "update slots drive=0" | bconsole -s -n | \
	grep 'not found in catalog' | \
	sed 's/.*Slot=\([0-9]\+\).*/\1/g' | \
	xargs -n $c | \
	tr ' ' ','))
d=0
p=0
queue_finished=0
echo "redirecting output to /tmp/import-{0,1,2,3}.log"
for t in ${newtapes[@]}; do
	touch /tmp/import-$d.log

	echo "* Started import to bacula pool=${pools[$p]} with drive=$d"
	bconsole_import &
	((d++))
	((p++))
done

while [ $queue_finished -lt $d ]; do
	echo "Waiting for completed label jobs (done $queue_finished from $d)"
	sleep 5
	# FIXME won't work, $queue_finished always 0 ... try pidof and save PIDs
	exit 0
done

echo_f "import finished"
