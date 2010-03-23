
dnl
dnl Derived from the example configure.ac at: 
dnl
dnl http://www.openismus.com/documents/linux/building_libraries/building_libraries.shtml
dnl

dnl ------------------------------------------------------------
dnl Initialization macros.
dnl ------------------------------------------------------------

AC_INIT(dsms/include/interface/server.h)
AM_CONFIG_HEADER(config.h)

dnl ------------------------------------------------------------
dnl Package name and version number
dnl ------------------------------------------------------------

PROJECT=stream
VERSION=0.6.1

AM_INIT_AUTOMAKE($PROJECT, $VERSION, no-define)

dnl -----------------------------------------------
dnl Checks for programs
dnl -----------------------------------------------

AC_PROG_CC
AC_PROG_CXX
AC_PROG_LEX
AC_PROG_YACC
AM_PROG_LIBTOOL

AC_LANG_CPLUSPLUS

dnl CXXFLAGS='-g -ansi -Wall -D_MONITOR_ -D_DM_ -D_SYS_STR_'
CXXFLAGS='-ansi -D_MONITOR_ -O3 -D_SYS_STR_'
AC_SUBST(CXXFLAGS)

dnl -----------------------------------------------
dnl Generates Makefile's, configuration files and scripts
dnl -----------------------------------------------

AC_OUTPUT(Makefile \
          dsms/Makefile \
          dsms/src/Makefile \
          dsms/src/common/Makefile \
          dsms/src/execution/Makefile \
          dsms/src/execution/indexes/Makefile \
          dsms/src/execution/internals/Makefile \
          dsms/src/execution/memory/Makefile \
          dsms/src/execution/monitors/Makefile \
          dsms/src/execution/operators/Makefile \
          dsms/src/execution/queues/Makefile \
          dsms/src/execution/scheduler/Makefile \
          dsms/src/execution/stores/Makefile \
          dsms/src/execution/synopses/Makefile \
          dsms/src/interface/Makefile \
          dsms/src/metadata/Makefile \
          dsms/src/parser/Makefile \
          dsms/src/querygen/Makefile \
          dsms/src/server/Makefile \
	  dsms/include/Makefile \
	  dsms/include/common/Makefile \
	  dsms/include/execution/Makefile \
	  dsms/include/execution/queues/Makefile \
	  dsms/include/execution/indexes/Makefile \
	  dsms/include/execution/internals/Makefile \
	  dsms/include/execution/memory/Makefile \
	  dsms/include/execution/monitors/Makefile \
	  dsms/include/execution/operators/Makefile \
	  dsms/include/execution/scheduler/Makefile \
	  dsms/include/execution/stores/Makefile \
	  dsms/include/execution/synopses/Makefile \
	  dsms/include/interface/Makefile \
	  dsms/include/metadata/Makefile \
	  dsms/include/parser/Makefile \
	  dsms/include/querygen/Makefile \
	  dsms/include/server/Makefile \
          lib/Makefile \
          gen_client/Makefile \
          net_server/Makefile \
          net_server/include/Makefile \
          net_server/src/Makefile \
          test/Makefile \
          test/data/Makefile \
          test/sample-out/Makefile \
          test/scripts/Makefile \
          examples/Makefile \
          examples/scripts/Makefile \
          examples/data/Makefile \
          docs/Makefile
)
