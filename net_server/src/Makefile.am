
INCLUDES = -I$(top_srcdir)/dsms/include -I$(top_srcdir)/net_server/include

bin_PROGRAMS = net_server

net_server_SOURCES = command_conn.cc \
                     gen_conn.cc     \
                     in_conn.cc      \
                     net_mgr.cc      \
                     net_util.cc     \
                     out_conn.cc     \
                     queue.cc        \
                     thread.cc 

net_server_LDADD = $(top_builddir)/lib/libdsms.la -lpthread
