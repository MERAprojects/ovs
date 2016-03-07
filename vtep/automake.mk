# vtep IDL
if OPS
ovslibincludedir = $(includedir)/ovs
ovslibinclude_HEADERS += \
	vtep/vtep-idl.h
endif

OVSIDL_BUILT += \
	vtep/vtep-idl.c \
	vtep/vtep-idl.h \
	vtep/vtep-idl.ovsidl
EXTRA_DIST += vtep/vtep-idl.ann
VTEP_IDL_FILES = \
	$(srcdir)/vtep/vtep.ovsschema \
	$(srcdir)/vtep/vtep-idl.ann
    $(srcdir)/vtep/vtep-idl.ovsidl: $(VTEP_IDL_FILES)
	$(AM_V_GEN)$(OVSDB_IDLC) annotate $(VTEP_IDL_FILES) > $@.tmp && \
	mv $@.tmp $@
    CLEANFILES += vtep/vtep-idl.c vtep/vtep-idl.h

bin_PROGRAMS += \
   vtep/vtep-ctl

MAN_ROOTS += \
   vtep/vtep-ctl.8.in

DISTCLEANFILES += \
   vtep/vtep-ctl.8

man_MANS += \
   vtep/vtep-ctl.8

vtep_vtep_ctl_SOURCES = vtep/vtep-ctl.c
vtep_vtep_ctl_LDADD = lib/libovscommon.la ovsdb/libovsdb.la lib/libopenvswitch.la vtep/libvtep.la

# ovs-vtep
scripts_SCRIPTS += \
    vtep/ovs-vtep

docs += vtep/README.ovs-vtep.md
EXTRA_DIST += vtep/ovs-vtep

# VTEP schema and IDL
EXTRA_DIST += vtep/vtep.ovsschema
pkgdata_DATA += vtep/vtep.ovsschema

# VTEP E-R diagram
#
# If "python" or "dot" is not available, then we do not add graphical diagram
# to the documentation.
if HAVE_PYTHON
if HAVE_DOT
vtep/vtep.gv: ovsdb/ovsdb-dot.in vtep/vtep.ovsschema
	$(AM_V_GEN)$(OVSDB_DOT) --no-arrows $(srcdir)/vtep/vtep.ovsschema > $@
vtep/vtep.pic: vtep/vtep.gv ovsdb/dot2pic
	$(AM_V_GEN)(dot -T plain < vtep/vtep.gv | $(PERL) $(srcdir)/ovsdb/dot2pic -f 3) > $@.tmp && \
	mv $@.tmp $@
VTEP_PIC = vtep/vtep.pic
VTEP_DOT_DIAGRAM_ARG = --er-diagram=$(VTEP_PIC)
DISTCLEANFILES += vtep/vtep.gv vtep/vtep.pic
endif
endif

# VTEP schema documentation
EXTRA_DIST += vtep/vtep.xml
DISTCLEANFILES += vtep/vtep.5
man_MANS += vtep/vtep.5
vtep/vtep.5: \
	ovsdb/ovsdb-doc vtep/vtep.xml vtep/vtep.ovsschema $(VTEP_PIC)
	$(AM_V_GEN)$(OVSDB_DOC) \
		$(VTEP_DOT_DIAGRAM_ARG) \
		--version=$(VERSION) \
		$(srcdir)/vtep/vtep.ovsschema \
		$(srcdir)/vtep/vtep.xml > $@.tmp && \
	mv $@.tmp $@

# Version checking for vtep.ovsschema.
ALL_LOCAL += vtep/vtep.ovsschema.stamp
vtep/vtep.ovsschema.stamp: vtep/vtep.ovsschema
	$(srcdir)/build-aux/cksum-schema-check $? $@
CLEANFILES += vtep/vtep.ovsschema.stamp
