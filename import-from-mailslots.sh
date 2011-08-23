#!/bin/bash

source "import-export.conf" || exit 1

transfers=0
label=0
option=""
pools=(Inc Full Diff Inc)


trim() {
    echo $1|sed 's/^\s\+\|\s\+$//g';
}

chunks() {
    echo "${#mailslots[*]} / ${#drives[*]}" | bc -l | xargs printf '%1.0f';
}

bacula_exec() {
    echo "$@" | bconsole -s -n > /dev/null;
}

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

unmount_drives() {
    echo_f "unmouting drives"
    ((jobsdone++))
    for d in ${drives[@]}; do
        echo "unmounting drive-$d"
        bacula_exec "unmount drive=$d"
    done
}

calc_transfers() {
    if [ ${#fullslots[*]} -gt ${#emptymailslots[*]} ]; then
        transfers=${#emptymailslots[*]}
    else
        transfers=${#fullslots[*]}
    fi
}

check_running_pids() {
    for p in ${pids[@]}; do
        if [ -z "`ps a | awk '{print $1}' | grep $p`" ]; then
            unset pids[$p]
        fi
    done
}

usage() {
    echo 'usage: $0 [options] (--import|--export)'
    echo
    echo '    --import    - start import to bacula from mailslots'
    echo '    --export    - start export to bacula from mailslots'
    echo '    --label     - label volumes after import'
    echo '    -s          - path to storage device'
    echo '    -m          - path to mtx-changer script'
    echo '    -h          - outputs this message'
    exit
}

# Argument parsing
[[ $1 ]] || usage
while [[ $1 ]]; do
    case "$1" in
        '-s') storage="$2" ;;
        '--import') option="import" ;;
        '--export') option="export" ;;
        '--label') label=1 ;;
        '-h'|'--help') usage ;;
        '-m') mtxchanger="$2" ;;
        -*) echo "$0: Option \`$1' is not valid." ; exit 5 ;;
        *) ;;
    esac
    shift
done

bacula_check_running

if [[ $option == 'import' ]]; then
    unmount_drives

    declare -a emptyslots=($($mtxchanger $storage listall | grep '^S:.*:E$' | awk -F: '{print $2}'))
    declare -a mailslots=($($mtxchanger $storage listall | grep '^I:.*:F:' | awk -F: '{print $2}'))

    calc_transfers

    echo_f "statistics"
    if [ $label -gt 0 ]; then
        jobqueue=3
    else
        jobqueue=2
    fi
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

    if [ $label -gt 0 ]; then
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
        pids=()
        echo "redirecting output to /tmp/import-{0,1,2,3}.log"
        for t in ${newtapes[@]}; do
            touch /tmp/import-drive-$d.log

            echo "* Started import to bacula pool=${pools[$p]} with drive=$d"
            bconsole_import &
            pids[$d] = $!
            ((d++))
            ((p++))
        done

        while [ "${#pids[*]}" -gt 0 ]; do
            check_pids
            echo "Waiting for completed label jobs (done $queue_finished from $d)"
            sleep 5
            # FIXME won't work, $queue_finished always 0 ... try pidof and save PIDs
            exit 0
        done
    fi

    echo_f "import finished"
fi

if [[ $option == 'export' ]]; then
    jobqueue=3
    jobsdone=1
    full_volume_query="SELECT volumename,slot FROM media WHERE inchanger=1 AND volstatus='Full' ORDER BY lastwritten"

    unmount_drives

    {
        IFS=$'\n'
        declare -a fullslots=($($mtxchanger $storage listall | grep '^S:.*:F'))
        declare -a fullvolumes=($(psql -d bacula -c "$full_volume_query" --tuples-only|grep 'ST'))
        declare -a emptymailslots=($($mtxchanger $storage listall | grep '^I:.*:E$' | awk -F: '{print $2}'))
    }

    calc_transfers

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
fi
