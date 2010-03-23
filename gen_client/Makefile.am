

INCLUDES = -I$(top_srcdir)/dsms/include

bin_PROGRAMS = gen_client

gen_client_SOURCES = generic_client.cc file_source.cc gen_output.cc script_file_reader.cc file_source.h gen_output.h script_file_reader.h

gen_client_LDADD = $(top_builddir)/lib/libdsms.la  -lpthread

