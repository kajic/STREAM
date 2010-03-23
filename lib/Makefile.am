lib_LTLIBRARIES = libdsms.la

libdsms_la_SOURCES = 

libdsms_la_LIBADD = $(top_builddir)/dsms/src/common/libcommon.la                 \
                    $(top_builddir)/dsms/src/execution/indexes/libindex.la       \
                    $(top_builddir)/dsms/src/execution/internals/libinternal.la  \
                    $(top_builddir)/dsms/src/execution/memory/libmemory.la       \
                    $(top_builddir)/dsms/src/execution/monitors/libmon.la        \
                    $(top_builddir)/dsms/src/execution/operators/libop.la        \
                    $(top_builddir)/dsms/src/execution/queues/libqueue.la        \
                    $(top_builddir)/dsms/src/execution/scheduler/libsched.la     \
                    $(top_builddir)/dsms/src/execution/stores/libstore.la        \
                    $(top_builddir)/dsms/src/execution/synopses/libsyn.la        \
                    $(top_builddir)/dsms/src/metadata/libmetadata.la             \
                    $(top_builddir)/dsms/src/parser/libparser.la                 \
                    $(top_builddir)/dsms/src/querygen/libquerygen.la             \
                    $(top_builddir)/dsms/src/server/libserver.la


interfacedir=$(pkgincludedir)/interface
interface_HEADERS=$(top_srcdir)/dsms/include/interface/error.h \
                  $(top_srcdir)/dsms/include/interface/query_output.h \
                  $(top_srcdir)/dsms/include/interface/server.h \
                  $(top_srcdir)/dsms/include/interface/table_source.h

commondir=$(pkgincludedir)/common
common_HEADERS=$(top_srcdir)/dsms/include/common/aggr.h \
               $(top_srcdir)/dsms/include/common/constants.h \
               $(top_srcdir)/dsms/include/common/debug.h \
               $(top_srcdir)/dsms/include/common/op.h \
               $(top_srcdir)/dsms/include/common/types.h \
               $(top_srcdir)/dsms/include/common/window.h
