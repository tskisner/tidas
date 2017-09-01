EXTRA_DIST = README.md

# Ensure that any time libtool is updated, we also generate
# a version that works with MPI

noinst_DATA = fixlibtool

fixlibtool : $(top_srcdir)/libtool
	@echo "Fixing libtool" \
    && cat $(top_srcdir)/libtool \
    | sed -e "s#CC=\"$(CC)\"#CC=\"$(MPICC)\"#g" \
    | sed -e "s#CC=\"$(CXX_ORIG)\"#CC=\"$(MPICXX_ORIG)\"#g" \
    | sed -e "s#CC=\"$(FC)\"#CC=\"$(MPIFC)\"#g" \
    > $(top_srcdir)/libtool_mpi \
    && chmod +x $(top_srcdir)/libtool_mpi \
    && touch fixlibtool

CLEANFILES = fixlibtool

