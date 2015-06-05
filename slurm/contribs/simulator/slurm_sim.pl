#!/usr/bin/perl

my $USER=$ENV{"USER"};
my $PREFIX=$ARGV[0];
my $SRCDIR=$ARGV[1];

open SBATCH,">$SRCDIR/contribs/simulator/sim_sbatch";
print SBATCH
"#!/bin/bash
PARAMS=`echo $@`
DATE=`date +%s`
# Just for debug
echo 'LD_LIBRARY_PATH=$PREFIX/lib LD_PRELOAD=libslurm_sim.so $PREFIX/bin/sbatch' \$PARAMS > $PREFIX/tmp/sim_sbatch-\$DATE
RES=`LD_LIBRARY_PATH=$PREFIX/lib LD_PRELOAD=libslurm_sim.so $PREFIX/bin/sbatch \$PARAMS > $PREFIX/tmp/sim_sbatch.out`
";

open LOG,">$SRCDIR/contribs/simulator/slurm_sim.h";

print LOG
"
/*
########################################################################
#  THIS FILE IS AUTOMATICALLY CREATED BY slurm_sim.pl when             #
#	Slurm configure is executed with --enable-simulator	       #
#  								       #
#    IPC names are based on username compiling Slurm		       #
#    sim_mgr should be executed with same uid			       #
#    								       #
########################################################################
*/

#include <pthread.h>

#define MAX_THREADS 64

#define SLURM_SIM_SHM \"/".$USER."_slurm_sim.shm\"

#define SIM_GLOBAL_SEM \"".$USER."_slurm_simulator\"
#define SIM_LOCAL_SEM_PREFIX \"".$USER."_slurm_simulator_thread_sem_\"
#define SIM_LOCAL_SEM_BACK_PREFIX \"".$USER."_slurm_simulator_thread_sem_back_\"


/* Offsets */
#define SIM_SECONDS_OFFSET      0
#define SIM_MICROSECONDS_OFFSET 4

/* This sets a MAX_THREAD upper limit to 64 */
#define SIM_SLEEP_ARRAY_MAP_OFFSET    8
#define SIM_THREAD_EXIT_MAP_OFFSET    16
#define SIM_THREAD_NEW_MAP_OFFSET     24
#define SIM_THREADS_COUNT_OFFSET      32
#define SIM_PROTO_THREADS_OFFSET      36
#define SIM_FAST_THREADS_OFFSET       40
#define SIM_PTHREAD_CREATE_COUNTER    44
#define SIM_PTHREAD_EXIT_COUNTER      48
#define SIM_PTHREAD_SLURMCTL_PID      52
#define SIM_PTHREAD_SLURMD_PID        56
#define SIM_SYNC_ARRAY_OFFSET         60

typedef struct thread_data {
		pthread_t ptid;
		pid_t pid;
		unsigned long func_addr;
		time_t creation;
		time_t last_sleep;
		time_t last_wakeup;
		time_t deletion;
		int sleep;
		int is_new;
		int never_ending;
		int joining;
		long long wait_time;
		long int wait_count;
} thread_data_t;

/* Each thread needs a sleep count value and a sem_t variable */
#define THREAD_DATA_SIZE    (sizeof(thread_data_t))
";
