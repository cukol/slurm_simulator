/*****************************************************************************\
 *  rsv_trace_builder.c - resource reservation trace builder
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
#include <fcntl.h>
#include <getopt.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include "sim_trace.h"

int option_index;
static struct option long_options[] = {

	{"account",	1, 0, 'a'},
	{"duration",	1, 0, 'd'},
	{"flags",	1, 0, 'f'},
	{"name",	1, 0, 'n'},
	{"nodecnt",	1, 0, 'N'},
	{"partition",	1, 0, 'p'},
	{"starttime",	1, 0, 's'},
	{"user",	1, 0, 'u'},
	{NULL,		0, 0, 0}
};

int trace_file;
job_trace_t new_trace;

char *user;
char *account;
char *rsv_name;
char *partition;
char *starttime;
char *duration;
char *flags;
char *nodecount;

char scontrol_command[1000];

int main(int argc, char *argv[])
{
	int opt_char;
	int option_index;

	while ((opt_char = getopt_long(argc, argv, "naupsdNf",
			long_options, &option_index)) != -1) {
		switch (opt_char) {
		case (int)'n':
			rsv_name = strdup(optarg);
		    break;

		case (int)'a':
			account = strdup(optarg);
			break;

		case (int)'u':
			user = strdup(optarg);
			break;

		case (int)'p':
			partition = strdup(optarg);
			break;

		case (int)'s':
			starttime = strdup(optarg);
			break;

		case (int)'d':
			duration = strdup(optarg);
			break;

		case (int)'f':
			flags = strdup(optarg);
			break;

		case (int)'N':
			nodecount = strdup(optarg);
			break;

		default:
			fprintf(stderr, "getopt error, returned %c\n",opt_char);
			exit(0);
		}
	}

	sprintf(scontrol_command,
		"scontrol create reservation=%s users=%s accounts=%s "
		"partitionname=%s starttime=%s duration=%s flags=%s nodecnt=%s\n", 
		rsv_name, user, account,
		partition, starttime, duration, flags, nodecount);

	trace_file = open("test.trace",
			  O_CREAT | O_APPEND | O_RDWR, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
	if (trace_file < 0) {
		printf("Error opening file test.trace\n");
		return -1;
	}

	write(trace_file, scontrol_command, strlen(scontrol_command));
	close(trace_file);

	return 0;
}
