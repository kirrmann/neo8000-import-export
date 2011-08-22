#!/bin/bash

source "import-export-functions.sh" || exit 1
source "import-export.conf" || exit 1

bacula_check_running

jobqueue=3
jobsdone=1
transfers=0
full_volume_query="SELECT volumename,slot FROM media WHERE inchanger=1 AND volstatus='Full' ORDER BY lastwritten"

echo_f "unmouting drives"
((jobsdone++))
for d in ${drives[@]}; do
	echo "unmounting drive-$d"
	bacula_exec "unmount drive=$d"
done

{
	IFS=$'\n'
	declare -a fullslots=($($mtxchanger $storage listall | grep '^S:.*:F'))
	declare -a fullvolumes=($(psql -d bacula -c "$full_volume_query" --tuples-only|grep 'ST'))
	declare -a emptymailslots=($($mtxchanger $storage listall | grep '^I:.*:E$' | awk -F: '{print $2}'))
}

if [ ${#fullslots[*]} -gt ${#emptymailslots[*]} ]; then
	transfers=${#emptymailslots[*]}
else
	transfers=${#fullslots[*]}
fi

echo_f "statistics"
echo "Full slots: ${#fullslots[*]}"
echo "Full volumes: ${#fullvolumes[*]}"
echo "Empty mailslots: ${#emptymailslots[*]}"

echo_f "starting transfer"
((jobsdone++))
fvindex=0
for mslot in ${emptymailslots[@]}; do
	transfer=$(($fvindex+1))
	vol=${fullvolumes[$fvindex]}
	label=`trim "$(echo $vol|awk -F\| '{print $1}')"`
	slot=`trim "$(echo $vol|awk -F\| '{print $2}')"`
	echo "($transfer/$transfers) moving full volume $label from $slot to $mslot"
	$mtxchanger $storage transfer $slot $mslot
	[[ $fvindex -gt ${#emptymailslots[*]} ]] && break
	((fvindex++))
done

echo_f "export finished"
