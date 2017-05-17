#!/bin/bash

# Copyright (C) 2017 Edinburgh Genomics, The University of Edinburgh
#
# Redistribution and use in source and binary forms, with or withouti
# modification, are permitted provided that the following conditions are met:
#
# 1. Redistributions of source code must retain the above copyright notice,
#    this list of conditions and the following disclaimer.
#
# 2. Redistributions in binary form must reproduce the above copyright notice,
#    this list of conditions and the following disclaimer in the documentation
#    and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

# Seems the --wait flag on sbatch is a new feature and unreliable.
# So what I want is a wrapper script which:

# 1) Runs sbatch "$@"
# 2) Captures the job ID /Submitted batch job ([0-9]+)/
# 3) Waits for sbatch to finish
# 4) Polls on 'scontrol show' until the job is really finished and returns the
#    exit status.

# This is going to be messy. Has someone not done it already??
# I can't find anything at all. Ask Tim, maybe?
# Tim's using 'sacct', which is going to be slowwwww.

# So, job states are listed here:
# https://slurm.schedmd.com/squeue.html#lbAG

# 1) Run sbatch and capture the job ID
exec 3>&1
jobid=$( sbatch "$@" | while read l ; do
    if [[ $l =~ Submitted\ batch\ job\ ([0-9]+) ]] ; then
        echo "${BASH_REMATCH[1]}"
    fi
    echo "$l" >&3
done )

# 2) I can't easily get the retcode from sbatch but I don't trust it anyway.
if [[ ! "$jobid" =~ [0-9]+ ]] ; then
    echo "sbatch did not return a valid job ID"
    exit 3
fi

# 4) Poll 'scontrol show'
while true ; do
    ji=(`scontrol show -o jobid "$jobid" 2>&1 | grep -o 'error.*\|JobState=[^s ]\+\|ExitCode=[^s ]\+' | sort -ur`)

    if [[ "${ji[*]}" =~ Invalid\ job\ id ]] ; then
        #We shouldn't see this. Go to 'sacct' for a definitive answer.
        echo "warning: scontrol says ${ji[*]}"
        di="`sacct -nP -o ExitCode -j "$jobid" | sort -u`"
        if [[ "$di" == "0:0" ]] ; then
            #OK, I guess it was all good.
            exit 0
        else
            exit 1
        fi
    fi

    #Now I expect a bunch of JOBSTATE lines followed by some EXITCODE lines.
    #If any job is CONFIGURING/COMPLETING/PENDING/RUNNING/SUSPENDED/STOPPED then keep polling.
    #Else if any job is BOOT_FAIL/CANCELLED/FAILED/NODE_FAIL/PREEMPTED/TIMEOUT then log a fail.
    jobcount=0
    jobrunning=0
    jobfailed=0
    for js in "${ji[@]}" ; do
        [[ "$js" =~ ^JobState=([A-Z]+) ]] || continue
        case "${BASH_REMATCH[1]}" in
            CONFIGURING | COMPLETING | PENDING | RUNNING | SUSPENDED | STOPPED)
                jobrunning=$(( $jobrunning + 1 )) ;;

            BOOT_FAIL | CANCELLED | FAILED | NODE_FAIL | PREEMPTED | TIMEOUT)
                jobfailed=$(( $jobfailed + 1 )) ;;
        esac
        jobcount=$(( $jobcount + 1 ))
    done

    if [[ $jobrunning != 0 ]] ; then
        #Jobs still running. Keep waiting.
        sleep 2
        continue
    fi

    if [[ $jobcount == 0 ]] ; then
        #Maybe some intermittent error?
        echo "${ji[*]}"
        sleep 5
        continue
    fi

    #So we're done. Break the loop and get the exit codez
    break
done

highest_exit_code=0
for js in "${ji[@]}" ; do
    [[ "$js" =~ ^ExitCode=([0-9]+):([0-9]+) ]] || continue

    if [[ "${BASH_REMATCH[1]}" > "$highest_exit_code" ]] ; then
        highest_exit_code="${BASH_REMATCH[1]}"
    fi
    if [[ "${BASH_REMATCH[2]}" > "$highest_exit_code" ]] ; then
        highest_exit_code="${BASH_REMATCH[2]}"
    fi
done
if [[ $jobfailed != 0 ]] && [[ "$highest_exit_code" == 0 ]] ; then
    #Should not be!
    highest_exit_code=99
fi

#Here we go.
exit "$highest_exit_code"
