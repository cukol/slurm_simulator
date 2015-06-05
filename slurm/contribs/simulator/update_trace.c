/*****************************************************************************\
 *  update_trace.c -
 *****************************************************************************
 *  Copyright (C) 2011 BSC-CNS (Barcelona Supercomputing Center)
 *
 *  This file is part of SLURM, a resource management program.
 *  For details, see <http://slurm.schedmd.com/>.
 *  Please also read the included file: DISCLAIMER.
 *
 *  SLURM is free software; you can redistribute it and/or modify it under
 *  the terms of the GNU General Public License as published by the Free
 *  Software Foundation; either version 2 of the License, or (at your option)
 *  any later version.
 *
 *  In addition, as a special exception, the copyright holders give permission
 *  to link the code of portions of this program with the OpenSSL library under
 *  certain conditions as described in each individual source file, and
 *  distribute linked combinations including the two. You must obey the GNU
 *  General Public License in all respects for all of the code used other than
 *  OpenSSL. If you modify file(s) with this exception, you may extend this
 *  exception to your version of the file(s), but you are not obligated to do
 *  so. If you do not wish to do so, delete this exception statement from your
 *  version.  If you delete this exception statement from all source files in
 *  the program, then also delete it here.
 *
 *  SLURM is distributed in the hope that it will be useful, but WITHOUT ANY
 *  WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 *  FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 *  details.
 *
 *  You should have received a copy of the GNU General Public License along
 *  with SLURM; if not, write to the Free Software Foundation, Inc.,
 *  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301  USA.
\*****************************************************************************/
#include <errno.h>
#include <getopt.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include "sim_trace.h"

/* This command modifies a job trace file depending on options given by the user:
 *
 * -R implies linking jobs to reservations
 * -D implies linking a job to other by dependency
 *
*/

static struct option long_options[] = {
	{"account",	1, 0, 'a'},
	{"dependency",	0, 0, 'D'},
	{"jobid",	1, 0, 'j'},
	{"reservation",	0, 0, 'R'},
	{"rsv_name",	1, 0, 'n'},
	{"ref_jobid",	1, 0, 'r'},
	{NULL,		0, 0, 0}
};

char *rsv_name;
int reservation_opt = 0;
int dependency_opt = 0;
int jobid;
char *ref_jobid;
char *account;

int main(int argc, char *argv[])
{
	int trace_file, new_file;
	job_trace_t job_trace;
	int option_index;
	int opt_char;


	while ((opt_char = getopt_long(argc, argv, "RDnjra",
		long_options, &option_index)) != -1) {
		switch (opt_char) {
			case (int)'R':
				printf("Reservation option\n");
				reservation_opt = 1;
				break;
			case (int)'D':
				printf("Dependency option\n");
				dependency_opt = 1;
				break;
			case (int)'n':
				rsv_name = strdup(optarg);
				printf("Parsing reservation name to %s\n",
					rsv_name);
				break;
			case (int)'j':
				jobid = atoi(optarg);
				printf("Parsing jobid to %d\n", jobid);
				break;
			case (int)'r':
				ref_jobid = strdup(optarg);
				break;
			case (int)'a':
				account = strdup(optarg);
				printf("Parsing account to %s\n", account);
				break;
			default:
				fprintf(stderr, "getopt error, returned %c\n",
					opt_char);
				exit(0);
		}
	}

	if (!reservation_opt && !dependency_opt) {
		printf("Command needs to specify reservation or dependency action\n");
		return -1;
	}

	if (reservation_opt) {
		if ((rsv_name == NULL) || (jobid == 0) || (account == NULL)) {
			printf("Reservation option needs:\n\t --rsv_name and \n\t--jobid\n\t--account\n");
			return -1;
		}
	}

	if (dependency_opt) {
		if ((ref_jobid == NULL) || (jobid == 0)) {
			printf("Dependency option needs --jobid and --ref_jobid\n");
			return -1;
		}
	}


	trace_file = open("test.trace", O_RDONLY);
	if (trace_file < 0) {
		printf("Error opening test.trace\n");
		return -1;
	}

	new_file = open(".test.trace.new", O_CREAT | O_RDWR, S_IRWXU);
	if (new_file < 0) {
		printf("Error creating temporal file at /tmp\n");
		return -1;
	}

	while (read(trace_file, &job_trace, sizeof(job_trace))) {
		if (reservation_opt) {
			if (job_trace.job_id != jobid) {
				write(new_file, &job_trace, sizeof(job_trace));
				continue;
			}
           
			sprintf(job_trace.reservation, "%s", rsv_name);
			sprintf(job_trace.account, "%s", account);
		}

		if (dependency_opt) {
			if (job_trace.job_id != jobid) {
				write(new_file, &job_trace, sizeof(job_trace));
				continue;
			}

			sprintf(job_trace.dependency, "%s", ref_jobid);
		}

		write(new_file, &job_trace, sizeof(job_trace));
	}

	close(trace_file);
	close(new_file);

	if (rename(".test.trace.new", "./test.trace") < 0) {
		printf("Error renaming file: %d\n", errno);
	}

	return 0;
}
