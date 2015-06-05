/*****************************************************************************\
 *  sim_trace.h - job trace header
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

#ifndef __SIM_TRACE_H__
#define __SIM_TRACE_H__

#define MAX_USERNAME_LEN 30
#define MAX_RSVNAME_LEN 30
#define MAX_QOSNAME 30
#define TIMESPEC_LEN 30
#define MAX_RSVNAME 30

typedef struct job_trace {
	int job_id;
	char username[MAX_USERNAME_LEN];
	long int submit; /* relative or absolute? */
	int duration;
	int wclimit;
	int tasks;
	char qosname[MAX_QOSNAME];
	char partition[MAX_QOSNAME];
	char account[MAX_QOSNAME];
	int cpus_per_task;
	int tasks_per_node;
	char reservation[MAX_RSVNAME];
	char dependency[MAX_RSVNAME];
	struct job_trace *next;
} job_trace_t;

typedef struct rsv_trace {
	long int creation_time;
	char *rsv_command;
	struct rsv_trace *next;
} rsv_trace_t;

#endif /*__SIM_TRACE_H__*/
