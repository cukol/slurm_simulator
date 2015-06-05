/*****************************************************************************\
 *  list_trace.c - option functions for sacct
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
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include "sim_trace.h"

int main(int argc, char *argv[])
{
	int trace_file;
	job_trace_t job_trace;

	trace_file = open("test.trace", O_RDONLY);
	if (trace_file < 0) {
		printf("Error opening test.trace\n");
		return -1;
	}

	printf("JOBID  \tUSERNAME  \tPARTITION  \tACCOUNT  \tQOS     "
		"\tSUBMIT    \tDURATION  \tWCLIMIT  \tTASKS\n");
	printf("=====  \t========  \t=========  \t=======  \t======  "
		"\t========  \t========  \t======== \t=====\n");

	while (read(trace_file, &job_trace, sizeof(job_trace))) {
		printf("%5d  \t%8s  \t%9s  \t%7s  \t%6s  \t%8ld  \t%8d  \t%7d  \t%5d(%d,%d)",
			job_trace.job_id, job_trace.username,
			job_trace.partition, job_trace.account,
			job_trace.qosname, job_trace.submit, job_trace.duration,
			job_trace.wclimit, job_trace.tasks,
			job_trace.tasks_per_node, job_trace.cpus_per_task);
		if (strlen(job_trace.reservation) > 0)
			printf(" RES=%s", job_trace.reservation);
		if (strlen(job_trace.dependency) > 0)
			printf(" DEP=%s", job_trace.dependency);

		printf("\n");

	}

	return 0;
}
