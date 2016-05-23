if OPS
openvswitchincludedir = $(includedir)/ovs/openvswitch
else
openvswitchincludedir = $(includedir)/openvswitch
endif
openvswitchinclude_HEADERS = \
	include/openvswitch/compiler.h \
	include/openvswitch/list.h \
	include/openvswitch/thread.h \
	include/openvswitch/token-bucket.h \
	include/openvswitch/types.h \
	include/openvswitch/util.h \
	include/openvswitch/version.h \
	include/openvswitch/vconn.h \
	include/openvswitch/vlog.h

