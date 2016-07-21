#! /bin/sh
# Copyright (C) 2016 Hewlett-Packard Development Company, L.P.
# All Rights Reserved.
#
# This script prepares the environment needed to run OVS tests
# inside an ops-build repo.
# 1. Verify the existance of the schema.
# 2. Verify the existance of the folder for the new compiling
# 3. Check if a previous compilation was done in order to clean
#    the folder.
# 4. Prepare the environment for the local target host.
# 5. Execute the tests
# 6. Delete the folder of tools modified in 4. in the base
#    folder.
# The results are displayed in screen and also can be found in
# folder ovs_test_build/tests/testsuite.log.

if ! test -f vswitchd/vswitch.ovsschema; then
    echo "============================================="
    echo "Schema files are needed to compile this "
    echo "module and run the tests."
    echo "Please build ops-openvswitch first"
    echo "============================================="
    exit 1
fi
if ! test -d ovs_test_build; then
	mkdir ovs_test_build
fi
cd ovs_test_build
if test -f Makefile; then
    echo "Cleanning previous makes"
    make clean
fi
export OVS_TEST=1
autoreconf --install --force ../configure.ac
../configure --build=x86_64-linux --host=x86_64-linux --target=x86_64-linux --prefix=/usr --exec_prefix=/usr --bindir=/usr/bin --sbindir=/usr/sbin --libexecdir=/usr/lib/ops-openvswitch --datadir=/usr/share --sysconfdir=/etc --sharedstatedir=/com --localstatedir=/var --libdir=/usr/lib --includedir=/usr/include --oldincludedir=/usr/include --infodir=/usr/share/info --mandir=/usr/share/man --disable-silent-rules --disable-dependency-tracking --with-libtool-sysroot=$(pwd)/../../build/tmp/sysroots/genericx86-64 TARGET_PYTHON=/usr/bin/python TARGET_PERL=/usr/bin/perl --disable-static --enable-shared LIBS=-ljemalloc --enable-simulator-provider
make check TESTSUITEFLAGS='-j'$(nproc)
unset OVS_TEST
cd ../
# Deleting this folder because it is created for each compilation for the target
# been compiled, no matter if you compile in $src_dir or in ovs_test_build dir and
# yocto don't update if it's found, so after running the tests, this folder targets for
# local host libraries instead of the desired yocto target.
rm -rf libltdl/
