# slurm_tools
Some scripts we're using with the SLURM job scheduler

At the moment, the only script in here is msub_generic.py, which is a
version of the msub script we use internally, but with a few bits
that only relate to our local system stripped out.

msub.py takes a list of commands, one per line, and runs each as
a separate element of an array job.
