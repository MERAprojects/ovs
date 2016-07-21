# Copyright (C) 2016 Hewlett-Packard Development Company, L.P.
# All Rights Reserved.
#

.PHONY: ops-openvswitch-ovstests

# Add option to run OVS tests inside ops-openvswitch folder
ops-openvswitch-ovstests:
	$(V)$(call BITBAKE, -f -c ovstests ops-openvswitch)
	$(V)$(call BITBAKE, -f -c configure ops-openvswitch)
