#!/usr/bin/perl

my $PREFIX=$ARGV[0];
my $TOTAL_JOBS=$ARGV[1];

my $logfile="$PREFIX/tmp/exec_sim.log";

open LOG,">$logfile";

print LOG "Launching sim_mgr...$PREFIX/sbin/sim_mgr\n";
system("cd $PREFIX");

system("SLURM_CONF=$PREFIX/etc/slurm.conf SLURM_PROGRAMS=$PREFIX/bin $PREFIX/sbin/sim_mgr 0 > $PREFIX/tmp/sim_mgr.log &");

sleep 5;
print LOG "Launching slurmctld...$PREFIX/sbin/exec_slurmctld.sh\n";
system("$PREFIX/sbin/exec_slurmctld.sh $PREFIX &");

sleep 5;
print LOG "Launching slurmd...$PREFIX/sbin/exec_slurmd.sh\n";
system("$PREFIX/sbin/exec_slurmd.sh $PREFIX &");

# Let's see which addresses have main slurm functions including plugins
sleep 5;
open PS,"ps axl | grep -v grep| grep slurmctld|";
while (<PS>){
	print LOG "$_\n\n";
	if(/(\d+)(\s+)(\d+)(\s+)(\d+)/){
		print LOG "Getting maps for process ID $5\n";
		system("more /proc/$5/maps > slurmctld.maps");
	}
}
close PS;


print LOG "Waiting...\n";
while(1){
	open SQUEUE,"$PREFIX/bin/squeue|";
	while(<SQUEUE>){
		print "$1\n";
	}
	close SQUEUE;
	$jobs_completed=$TOTAL_JOBS;
	if(($jobs_completed == $TOTAL_JOBS) || ($jobs_completed > $TOTAL_JOBS)){
		last;
	}
	print LOG "Just $jobs_completed jobs completed of $TOTAL_JOBS. Sleeping\n";
}

print LOG "Ok. We have $jobs_completed completed jobs\n";

sleep 5;
system("sync");
close PS;
print LOG "Killing simulation processes...\n\n";

# Killing sim_mgr with SIGINT signal

`killall sim_mgr -SIGINT $5`;
