#!/usr/bin/env python3

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
#
# Author - Tim Booth <tim.booth@ed.ac.uk>
# Based on earlier code and ideas by Timothee Cezard and Stephen Bridget

import sys, os, re
import logging as L
import random
import inspect
import subprocess
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter

"""msub.py is a script that helps you submit cluster jobs on SLURM.
   You need to provide a file of commands, one per line. The commands
   will be split into an array of jobs and submitted to the cluster.
"""

def main():

    args = parse_args()

    #Use logging to control output level.
    L.basicConfig(stream = sys.stderr,
                  format = '%(levelname)s: %(message)s',
                  level  = L.WARNING if args.quiet else L.INFO)

    #See where the commands are coming from.
    if args.iinput and args.input:
        L.error("Extra argument seen. Input specified as both %s and %s." % (
                    args.iinput, args.input ))
        exit(1)

    _i = args.iinput or args.input
    if (_i or '-') == '-':
        input_file = sys.stdin
        input_name = "STDIN"
        job_name = args.name or 'msub_stdin'
    else:
        input_file = open(_i)
        input_name = _i
        job_name = args.name or os.path.basename(_i)
        if job_name.endswith('.sh'):
            job_name = job_name[:-3]

    #Tidy up some corner cases on job name
    if job_name == '' or job_name.startswith('.'):
        job_name = "msub" + job_name

    if(os.isatty(input_file.fileno())):
        L.info("Type commands, one per line. Press Ctrl+D when done, or Ctrl+C to abort.")
    else:
        L.info("Reading commands from %s." % input_name)

    #Make a script and spew commands into it.
    #But we do need to count the commands before we can write the header.
    command_list = []
    try:
        for l in input_file:
            l = l.strip()
            if l.startswith('#SBATCH ') or l.startswith('#$ -'):
                L.error("Input appears to be a SLURM or SGE batch file. This is not suitable input for msub.")
                exit(1)
            if l == '.':
                break
            if l == '' or l.startswith('#'):
                continue
            command_list.append(l)
    except KeyboardInterrupt:
        exit("..Aborted")

    if not command_list:
        L.error("No commands supplied. Exiting.")
        exit(1)

    with open_sesame(job_name + '.sbatch') as ofh:
        script_file = ofh.name

        write_header(ofh, args, len(command_list))

        for i, c in enumerate(command_list):
            write_command(ofh, args, c, i)

        write_footer(ofh, args)


    L.info("%d commands written to %s" % (len(command_list), script_file))

    #Now run the script. Hopefully I can just launch it with os.execlp and I'm done.
    #None of this daemon nonsense.

    #These parameters don't make much sense being embedded in the job script.
    #Maybe -d should be in the script?
    #Maybe -p/--partition should be in here too??
    job_flags = ['-p', args.queue]    # partition (queue) to submit to
    job_flags.append('--nice=%i' % args.priority)
    if args.sync:
        job_flags.append('--wait') #for some reason, short arg -W fails

    sbatch = 'sbatch'
    if args.hard_sync:
        #Note that --sync and --hard-sync are not strictly incompatible,
        #just redundant.
        sbatch = find_sbatch_wait()

    if args.nosubmit:
        L.info( "Not running sbatch as --nosubmit was specified. Here's the command:")
        print( ' '.join(['mkdir -p', args.stdoutdir, ';', sbatch, *job_flags, script_file] ) )

    else:
        #We must have a slurm_output directory before launching the job.
        os.makedirs(args.stdoutdir, exist_ok=True)

        L.info( "Running " + ' '.join([sbatch, *job_flags, script_file]) )
        os.execlp(sbatch, sbatch, *job_flags, script_file)

def find_sbatch_wait():
    """Find the sbatch_wait.sh script.
    """
    #Is it in the $PATH?
    try:
        subprocess.check_output(["which", "sbatch_wait.sh"], stderr=subprocess.STDOUT)
        return "sbatch_wait.sh"
    except subprocess.CalledProcessError:
        pass

    #Is it in the same dir as $0?
    sw = os.path.dirname(os.path.abspath(sys.argv[0])) + "/sbatch_wait.sh"
    if os.path.isfile(sw) and os.access(sw, os.X_OK):
        return sw

    #OK, maybe $0 is a symlink and the script is in the real location of the file?
    sw = os.path.dirname(os.path.realpath(sys.argv[0])) + "/sbatch_wait.sh"
    if os.path.isfile(sw) and os.access(sw, os.X_OK):
        return sw

    #I give up.
    raise FileNotFoundError("Unable to locate an executable sbatch_wait.sh")

def write_header(ofh, args, task_count):
    """Print out the SBATCH header and optionally the command specified by
       the --begin argument.
    """

    def P(*x):
        for l in x:
            print(l.format(**inspect.stack()[1].frame.f_locals), file=ofh)

    #Interpret legacy environ spec as a request for more cores per task
    cpu_count = args.cpu
    if cpu_count == 1 and args.environ:
        cpu_count = int(args.environ.split()[-1])

    #For now, set the mem limit at 6144 * cpu unless overridden
    mem_limit = args.mem or (6144 * args.cpu)

    #Output file names should be the script file name, minus the extension
    jobname = os.path.basename(ofh.name)[:-len(".sbatch")]

    #Work out the array parameter based on task_count and args.max_running_task
    array_param = "0-%i" % (task_count - 1)
    if args.max_running_task:
        array_param += "%%%i" % args.max_running_task

    P("#!/bin/bash", '#')
    P("#SBATCH -a {array_param:<17}   # array of tasks and max number to run at once")
    P("#SBATCH -n 1                   # 1 task per node (fixed for all msub jobs)")
    P("#SBATCH -c {cpu_count:<15}     # number of cores per task")
    P("#SBATCH --mem {mem_limit:<15}  # memory pool per task (not per core)")
    P("#SBATCH -o {args.stdoutdir}/{jobname}.%A.%a.out     # STDOUT")
    P("#SBATCH -e {args.stdoutdir}/{jobname}.%A.%a.err     # STDERR")

    if args.hold:
        hold_arg = munge_hold_arg(args.hold)
        P("#SBATCH -d {hold_arg}  # hold off waiting for jobs")

    if args.noemail:
        P("#SBATCH --mail-type=NONE       # no email")
    else:
        P("#SBATCH --mail-type=END,FAIL   # notifications for job done & fail")

    #Hopefully this is automagic? Or do I have to do something silly to make it work?
    #SBATCH --mail-user=tim.booth@ed.ac.uk # send-to address

    #BASH "strict mode" - see http://redsymbol.net/articles/unofficial-bash-strict-mode/
    P("", r"set -euo pipefail", r"IFS=$'\n\t'", "")

    #User-specified pre-commands
    if args.begin: P("{args.begin}", "")

    #Make it slightly easier to manually run a task by setting $TASK
    #Set up case statement
    P("TASK=${{TASK:-unset}}")
    P("case ${{SLURM_ARRAY_TASK_ID:-$TASK}} in")

def write_footer(ofh, args):
    """Add a footer to the script file. Closes the case statement and adds
       --final if that was set.
    """
    print('*) echo "Unexpected SLURM_ARRAY_TASK_ID=${SLURM_ARRAY_TASK_ID:-$TASK}"', file=ofh)
    print('esac', file=ofh)

    print("", file=ofh)
    if args.final:
        print(args.final, file=ofh)

def write_command(ofh, args, cmd, task_number):
    """Write a single command to the file. As with the original msub, $??? is used
       to determine which command should be running within each task.
    """
    print("{}) {}\n;;".format(task_number, cmd), file=ofh)

def munge_hold_arg(hold_arg):
    """Takes the --hold parameter and translates it for SLURM
    """
    if ':' in hold_arg:
        #pass it as-is
        return hold_arg
    else:
        return 'afterok:' + ':'.join([str(int(j)) for j in hold_arg.split(',')])

class open_sesame:
    """Opens a file for writing, but if that already exists keeps adding
       integers (eg, foo.2.sh, foo.3.sh), until an unused name is found.
       Use this class in a with clause.
       Use fh.name to see what file was actually opened.
    """
    def __init__(self, filename):
        _filename = filename
        _counter = 0
        while True:
            try:
                self._fh = open(_filename, 'x')
                break
            except FileExistsError:
                _counter += 1
                if '.' in os.path.basename(filename):
                    _filename = '.'.join( filename.split('.')[:-1] +
                                          [str(_counter)] +
                                          filename.split('.')[-1:] )
                else:
                    _filename = filename + '.' + str(_counter)

        #Now we have a FH. Or we died of a permissions error.

    def __enter__(self):
        return self._fh

    def __exit__(self, *exc_info):
        self._fh.close()


def parse_args(*args):

    description = """Job submission wrapper for SLURM. Provide one task per line,
    either from a file or from STDIN. A .sbatch script will be created in the current
    directory and submitted to SLURM.
    """

    parser = ArgumentParser(description=description, formatter_class=ArgumentDefaultsHelpFormatter)

    parser.add_argument("-b", "--begin",
                        help="Commands to run before each task.")
    parser.add_argument("-c", "--cpu", type=int, default=1,
                        help="Number of CPUs to assign per task.")
    parser.add_argument("-m", "--mem", type=int,
                        help="Memory to assign per task, in MB. Defaults to 6144 (=6GB) per CPU.")
    #DRMAA argument not supported
    parser.add_argument("-e", "--environ",
                        help="Parallel environment. For compatibility with the old cluster, 'single 4' will set --cpu 4, "
                             "but you should really just use --cpu for this.")
    parser.add_argument("-f", "--final",
                        help="Commands to run after each task.")
    parser.add_argument("--hold",
                        help="Hold until the given job (or list of jobs, comma-separated) has successfully completed. "
                             "You can also supply a SLURM --dependency string and it will be passed as-is.")
    #--hold_tasks not explicitly supported, but you can use --hold aftercorr:1234 to achieve it.
    parser.add_argument("--nosubmit", action="store_true",
                        help="Don't actually submit the job, just make the script.")

    parser.add_argument("--max_running_task", type=int,
                        help="Limit the max number of tasks that can run at once.")

    #Input may be given with -i or just
    parser.add_argument("-i", "--input", dest="iinput",
                        help="Input file. Defaults to STDIN.")
    parser.add_argument("input", nargs='?',
                        help="Input file if not specified with -i")
    #-j --merge unimplemented

    parser.add_argument("-n", "--name",
                        help="The prefix name of the qsub script that will also be used as a submission name.")
    #--nodaemon (nodeamon?!) not implemented

    parser.add_argument("--noemail", action="store_true",
                        help="Don't e-mail the user when jobs complete")
    parser.add_argument("--sync", "--wait", action="store_true",
                        help="Wait for the jobs to finish before returning")
    parser.add_argument("--hard_sync", "--hard-sync", action="store_true",
                        help="Use the wrapper script for more reliable synchronization")
    parser.add_argument("-p", "--priority", "--nice", type=int, default=50,
                        help="Set the task priority (or rather, niceness. >50 for low priority, 0-49 for high priority.)")
    parser.add_argument("-q", "--queue", "--partition", default="global",
                        help="SLURM queue (partition) to use - eg. qc, analysis, blast")
    parser.add_argument("-s", "--stdoutdir", default="slurm_output",
                        help="Directory for stdout/stderr files.")
    #-t --tidyup not implemented
    parser.add_argument("-z", "--quiet", action="store_true",
                        help="Suppress most logging messages.")

    parser.add_argument("--version", action="version", version=str(round(random.random() * 10, 3)))

    return parser.parse_args(*args)

if __name__ == "__main__":
    main()

