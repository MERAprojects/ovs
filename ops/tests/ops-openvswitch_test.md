Openvswitch Socket Permissions
==============================


## Contents
- [openvswitch socket permissions](#openvswitch-socket-permissions)

##  L2 interface configuration
### Objective
This test case confirms that the OVSDB socket file permissions are valid.
### Requirements
- Virtual mininet test setup
- **CT File**:  ops-openvswitch/ops/tests/test_openvswitch_ct_socket_permission.py (socket permissions)

### Setup
#### Topology diagram
```ditaa
                           +--------+
                           |        |
                           |   S1   |
                           |        |
                           +--------+
```

### Description
Validate that the socket files in /var/run/openvswitch have file permission of type ‘srwxrw’ and has ‘ovsdb-client’ as the file group owner.
