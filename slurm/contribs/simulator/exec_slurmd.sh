#!/bin/bash

`LD_LIBRARY_PATH=$1/lib LD_PRELOAD=libslurm_sim.so $1/sbin/slurmd -Dc 2&> $1/tmp/slurmd.log`;
