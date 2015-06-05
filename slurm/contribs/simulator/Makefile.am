#
# Makefile for Slurm workload simulator package
#
# libslurm_sim version information :
current = $(SLURM_API_CURRENT)
age     = $(SLURM_API_AGE)
rev     = $(SLURM_API_REVISION)

AUTOMAKE_OPTIONS = foreign
CLEANFILES = core.*

EXTRA_DIST = README

# SCRIPTS for bin
bin_SCRIPTS = sim_sbatch

install-binSCRIPTS: $(bin_SCRIPTS)
	@$(NORMAL_INSTALL)
	test -z "$(DESTDIR)$(bindir)" || $(mkdir_p) "$(DESTDIR)$(bindir)"
	@list='$(bin_SCRIPTS)'; for p in $$list; do \
	   cp $(top_srcdir)/contribs/simulator/$$p $(DESTDIR)$(bindir)/$$p; \
	   chmod 755 $(DESTDIR)$(bindir)/$$p;\
	done

uninstall-binSCRIPTS:
	@$(NORMAL_UNINSTALL)
	@list='$(bin_SCRIPTS)'; for p in $$list; do \
	  echo " rm -f '$(DESTDIR)$(bindir)/$$p'"; \
	  rm -f "$(DESTDIR)$(bindir)/$$p"; \
	done

# SCRIPTS for sbin
sbin_SCRIPTS =	\
	exec_sim.pl	\
	exec_slurmctld.sh \
	exec_slurmd.sh

install-sbinSCRIPTS: $(sbin_SCRIPTS)
	@$(NORMAL_INSTALL)
	test -z "$(DESTDIR)$(sbindir)" || $(mkdir_p) "$(DESTDIR)$(sbindir)"
	@list='$(sbin_SCRIPTS)'; for p in $$list; do \
	   cp $(top_srcdir)/contribs/simulator/$$p $(DESTDIR)$(sbindir)/$$p; \
	   chmod 755 $(DESTDIR)$(sbindir)/$$p;\
	done

uninstall-sbinSCRIPTS:
	@$(NORMAL_UNINSTALL)
	@list='$(bin_SCRIPTS)'; for p in $$list; do \
	  echo " rm -f '$(DESTDIR)$(sbindir)/$$p'"; \
	  rm -f "$(DESTDIR)$(sbindir)/$$p"; \
	done


AM_CPPFLAGS = -I$(top_srcdir) $(HWLOC_CPPFLAGS)

sbin_PROGRAMS = list_trace rsv_trace_builder sim_ctlr sim_mgr trace_builder update_trace

list_trace_SOURCES = list_trace.c sim_trace.h
rsv_trace_builder_SOURCES = rsv_trace_builder.c sim_trace.h
sim_ctlr_SOURCES = sim_ctlr.c
sim_mgr_SOURCES = sim_mgr.c comm_protocol.h sim_trace.h slurm_sim.h
trace_builder_SOURCES = trace_builder.c sim_trace.h
update_trace_SOURCES = update_trace.c sim_trace.h

convenience_libs = $(top_builddir)/src/api/libslurm.o 

list_trace_LDADD = $(convenience_libs)
rsv_trace_builder_LDADD = $(convenience_libs)
sim_ctlr_LDADD = $(convenience_libs)
sim_mgr_LDADD = $(convenience_libs)
trace_builder_LDADD = $(convenience_libs)
update_trace_LDADD = $(convenience_libs)

CMD_FLAGS = -export-dynamic $(CMD_LDFLAGS) $(DL_LIBS) -lrt -lpthread $(HWLOC_LDFLAGS)
list_trace_LDFLAGS = $(CMD_FLAGS)
rsv_trace_builder_LDFLAGS = $(CMD_FLAGS)
sim_ctlr_LDFLAGS = $(CMD_FLAGS)
sim_mgr_LDFLAGS = $(CMD_FLAGS)
trace_builder_LDFLAGS = $(CMD_FLAGS)
update_trace_LDFLAGS = $(CMD_FLAGS)

lib_LTLIBRARIES = libslurm_sim.la

libslurm_sim_la_SOURCES = sim_lib.c comm_protocol.h slurm_sim.h
libslurm_sim_la_LDFLAGS = $(LIB_LDFLAGS) \
	$(DL_LIBS) -lrt -lpthread $(HWLOC_LDFLAGS) \
        -version-info $(current):$(rev):$(age)

force:
$(convenience_libs) : force
	      @cd `dirname $@` && $(MAKE) `basename $@`
