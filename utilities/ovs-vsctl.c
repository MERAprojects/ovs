/*
 * Copyright (c) 2009, 2010, 2011, 2012, 2013, 2014, 2015 Nicira, Inc.
 * Copyright (C) 2015, 2016 Hewlett-Packard Development Company, L.P.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <config.h>

#include <ctype.h>
#include <errno.h>
#include <float.h>
#include <getopt.h>
#include <inttypes.h>
#include <signal.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "db-ctl-base.h"

#include "command-line.h"
#include "compiler.h"
#include "dynamic-string.h"
#include "fatal-signal.h"
#include "hash.h"
#include "json.h"
#include "ovsdb-data.h"
#include "ovsdb-idl.h"
#include "poll-loop.h"
#include "process.h"
#include "stream.h"
#include "stream-ssl.h"
#include "smap.h"
#include "sset.h"
#include "svec.h"
#include "lib/vswitch-idl.h"
#include "table.h"
#include "timeval.h"
#include "util.h"
#include "openvswitch/vconn.h"
#include "openvswitch/vlog.h"

VLOG_DEFINE_THIS_MODULE(vsctl);

struct vsctl_context;

/* --db: The database server to contact. */
static const char *db;

/* --oneline: Write each command's output as a single line? */
static bool oneline;

/* --dry-run: Do not commit any changes. */
static bool dry_run;

/* --no-wait: Wait for ovs-vswitchd to reload its configuration? */
static bool wait_for_reload = true;

/* --timeout: Time to wait for a connection to 'db'. */
static int timeout;

/* --retry: If true, ovs-vsctl will retry connecting to the database forever.
 * If false and --db says to use an active connection method (e.g. "unix:",
 * "tcp:", "ssl:"), then ovs-vsctl will try to connect once and exit with an
 * error if the database server cannot be contacted (e.g. ovsdb-server is not
 * running).
 *
 * Regardless of this setting, --timeout always limits how long ovs-vsctl will
 * wait. */
static bool retry;

/* Format for table output. */
static struct table_style table_style = TABLE_STYLE_DEFAULT;

static void vsctl_cmd_init(void);

/* The IDL we're using and the current transaction, if any.
 * This is for use by vsctl_exit() only, to allow it to clean up.
 * Other code should use its context arguments. */
static struct ovsdb_idl *the_idl;
static struct ovsdb_idl_txn *the_idl_txn;
OVS_NO_RETURN static void vsctl_exit(int status);

OVS_NO_RETURN static void usage(void);
static void parse_options(int argc, char *argv[], struct shash *local_options);
static void run_prerequisites(struct ctl_command[], size_t n_commands,
                              struct ovsdb_idl *);
static void do_vsctl(const char *args, struct ctl_command *, size_t n,
                     struct ovsdb_idl *);

/* post_db_reload_check frame work is to allow ovs-vsctl to do additional
 * checks after OVSDB transactions are successfully recorded and reload by
 * ovs-vswitchd.
 *
 * For example, When a new interface is added to OVSDB, ovs-vswitchd will
 * either store a positive values on successful implementing the new
 * interface, or -1 on failure.
 *
 * Unless -no-wait command line option is specified,
 * post_db_reload_do_checks() is called right after any configuration
 * changes is picked up (i.e. reload) by ovs-vswitchd. Any error detected
 * post OVSDB reload is reported as ovs-vsctl errors. OVS-vswitchd logs
 * more detailed messages about those errors.
 *
 * Current implementation only check for Post OVSDB reload failures on new
 * interface additions with 'add-br' and 'add-port' commands.
 *
 * post_db_reload_expect_iface()
 *
 * keep track of interfaces to be checked post OVSDB reload. */
static void post_db_reload_check_init(void);
#ifndef OPS_TEMP
static void post_db_reload_do_checks(const struct vsctl_context *);
#endif
#ifdef OPS
static struct vsctl_iface *
find_orphan_iface(struct vsctl_context *ctx, const char *name, bool must_exist);
#endif
static void post_db_reload_expect_iface(const struct ovsrec_interface *);

static struct uuid *neoteric_ifaces;
static size_t n_neoteric_ifaces;
static size_t allocated_neoteric_ifaces;

int
main(int argc, char *argv[])
{
    extern struct vlog_module VLM_reconnect;
    struct ovsdb_idl *idl;
    struct ctl_command *commands;
    struct shash local_options;
    unsigned int seqno;
    size_t n_commands;
    char *args;

    set_program_name(argv[0]);
    fatal_ignore_sigpipe();
    vlog_set_levels(NULL, VLF_CONSOLE, VLL_WARN);
    vlog_set_levels(&VLM_reconnect, VLF_ANY_DESTINATION, VLL_WARN);
    ovsrec_init();

    vsctl_cmd_init();

    /* Log our arguments.  This is often valuable for debugging systems. */
    args = process_escape_args(argv);
    VLOG(ctl_might_write_to_db(argv) ? VLL_INFO : VLL_DBG, "Called as %s", args);

    /* Parse command line. */
    shash_init(&local_options);
    parse_options(argc, argv, &local_options);
    commands = ctl_parse_commands(argc - optind, argv + optind, &local_options,
                                  &n_commands);

    if (timeout) {
        time_alarm(timeout);
    }

    /* Initialize IDL. */
    idl = the_idl = ovsdb_idl_create(db, &ovsrec_idl_class, false, retry);
    run_prerequisites(commands, n_commands, idl);

    /* Execute the commands.
     *
     * 'seqno' is the database sequence number for which we last tried to
     * execute our transaction.  There's no point in trying to commit more than
     * once for any given sequence number, because if the transaction fails
     * it's because the database changed and we need to obtain an up-to-date
     * view of the database before we try the transaction again. */
    seqno = ovsdb_idl_get_seqno(idl);
    for (;;) {
        ovsdb_idl_run(idl);
        if (!ovsdb_idl_is_alive(idl)) {
            int retval = ovsdb_idl_get_last_error(idl);
            ctl_fatal("%s: database connection failed (%s)",
                        db, ovs_retval_to_string(retval));
        }

        if (seqno != ovsdb_idl_get_seqno(idl)) {
            seqno = ovsdb_idl_get_seqno(idl);
            do_vsctl(args, commands, n_commands, idl);
        }

        if (seqno == ovsdb_idl_get_seqno(idl)) {
            ovsdb_idl_wait(idl);
            poll_block();
        }
    }
}

static void
parse_options(int argc, char *argv[], struct shash *local_options)
{
    enum {
        OPT_DB = UCHAR_MAX + 1,
        OPT_ONELINE,
        OPT_NO_SYSLOG,
        OPT_NO_WAIT,
        OPT_DRY_RUN,
        OPT_BOOTSTRAP_CA_CERT,
        OPT_PEER_CA_CERT,
        OPT_LOCAL,
        OPT_RETRY,
        OPT_COMMANDS,
        OPT_OPTIONS,
        VLOG_OPTION_ENUMS,
        TABLE_OPTION_ENUMS
    };
    static const struct option global_long_options[] = {
        {"db", required_argument, NULL, OPT_DB},
        {"no-syslog", no_argument, NULL, OPT_NO_SYSLOG},
        {"no-wait", no_argument, NULL, OPT_NO_WAIT},
        {"dry-run", no_argument, NULL, OPT_DRY_RUN},
        {"oneline", no_argument, NULL, OPT_ONELINE},
        {"timeout", required_argument, NULL, 't'},
        {"retry", no_argument, NULL, OPT_RETRY},
        {"help", no_argument, NULL, 'h'},
        {"commands", no_argument, NULL, OPT_COMMANDS},
        {"options", no_argument, NULL, OPT_OPTIONS},
        {"version", no_argument, NULL, 'V'},
        VLOG_LONG_OPTIONS,
        TABLE_LONG_OPTIONS,
        STREAM_SSL_LONG_OPTIONS,
        {"bootstrap-ca-cert", required_argument, NULL, OPT_BOOTSTRAP_CA_CERT},
        {"peer-ca-cert", required_argument, NULL, OPT_PEER_CA_CERT},
        {NULL, 0, NULL, 0},
    };
    const int n_global_long_options = ARRAY_SIZE(global_long_options) - 1;
    char *tmp, *short_options;

    struct option *options;
    size_t allocated_options;
    size_t n_options;
    size_t i;

    tmp = ovs_cmdl_long_options_to_short_options(global_long_options);
    short_options = xasprintf("+%s", tmp);
    free(tmp);

    /* We want to parse both global and command-specific options here, but
     * getopt_long() isn't too convenient for the job.  We copy our global
     * options into a dynamic array, then append all of the command-specific
     * options. */
    options = xmemdup(global_long_options, sizeof global_long_options);
    allocated_options = ARRAY_SIZE(global_long_options);
    n_options = n_global_long_options;
    ctl_add_cmd_options(&options, &n_options, &allocated_options, OPT_LOCAL);
    table_style.format = TF_LIST;

    for (;;) {
        int idx;
        int c;

        c = getopt_long(argc, argv, short_options, options, &idx);
        if (c == -1) {
            break;
        }

        switch (c) {
        case OPT_DB:
            db = optarg;
            break;

        case OPT_ONELINE:
            oneline = true;
            break;

        case OPT_NO_SYSLOG:
            vlog_set_levels(&VLM_vsctl, VLF_SYSLOG, VLL_WARN);
            break;

        case OPT_NO_WAIT:
            wait_for_reload = false;
            break;

        case OPT_DRY_RUN:
            dry_run = true;
            break;

        case OPT_LOCAL:
            if (shash_find(local_options, options[idx].name)) {
                ctl_fatal("'%s' option specified multiple times",
                            options[idx].name);
            }
            shash_add_nocopy(local_options,
                             xasprintf("--%s", options[idx].name),
                             optarg ? xstrdup(optarg) : NULL);
            break;

        case 'h':
            usage();

        case OPT_COMMANDS:
            ctl_print_commands();

        case OPT_OPTIONS:
            ctl_print_options(global_long_options);

        case 'V':
            ovs_print_version(0, 0);
            printf("DB Schema %s\n", ovsrec_get_db_version());
            exit(EXIT_SUCCESS);

        case 't':
            timeout = strtoul(optarg, NULL, 10);
            if (timeout < 0) {
                ctl_fatal("value %s on -t or --timeout is invalid",
                            optarg);
            }
            break;

        case OPT_RETRY:
            retry = true;
            break;

        VLOG_OPTION_HANDLERS
        TABLE_OPTION_HANDLERS(&table_style)

        STREAM_SSL_OPTION_HANDLERS

        case OPT_PEER_CA_CERT:
            stream_ssl_set_peer_ca_cert_file(optarg);
            break;

        case OPT_BOOTSTRAP_CA_CERT:
            stream_ssl_set_ca_cert_file(optarg, true);
            break;

        case '?':
            exit(EXIT_FAILURE);

        default:
            abort();
        }
    }
    free(short_options);

    if (!db) {
        db = ctl_default_db();
    }

    for (i = n_global_long_options; options[i].name; i++) {
        free(CONST_CAST(char *, options[i].name));
    }
    free(options);
}

static void
usage(void)
{
    printf("\
%s: ovs-vswitchd management utility\n\
usage: %s [OPTIONS] COMMAND [ARG...]\n\
\n\
Open vSwitch commands:\n\
  init                        initialize database, if not yet initialized\n\
  show                        print overview of database contents\n\
  emer-reset                  reset configuration to clean state\n\
\n\
Bridge commands:\n\
  add-br BRIDGE               create a new bridge named BRIDGE\n\
  add-br BRIDGE PARENT VLAN   create new fake BRIDGE in PARENT on VLAN\n\
  del-br BRIDGE               delete BRIDGE and all of its ports\n\
  list-br                     print the names of all the bridges\n\
  br-exists BRIDGE            exit 2 if BRIDGE does not exist\n\
  br-to-vlan BRIDGE           print the VLAN which BRIDGE is on\n\
  br-to-parent BRIDGE         print the parent of BRIDGE\n\
  br-set-external-id BRIDGE KEY VALUE  set KEY on BRIDGE to VALUE\n\
  br-set-external-id BRIDGE KEY  unset KEY on BRIDGE\n\
  br-get-external-id BRIDGE KEY  print value of KEY on BRIDGE\n\
  br-get-external-id BRIDGE  list key-value pairs on BRIDGE\n\
\n\
Port commands (a bond is considered to be a single port):\n\
  list-ports BRIDGE           print the names of all the ports on BRIDGE\n\
  add-port BRIDGE PORT        add network device PORT to BRIDGE\n\
  add-bond BRIDGE PORT IFACE...  add bonded port PORT in BRIDGE from IFACES\n\
  del-port [BRIDGE] PORT      delete PORT (which may be bonded) from BRIDGE\n\
  port-to-br PORT             print name of bridge that contains PORT\n\
\n\
Interface commands (a bond consists of multiple interfaces):\n\
  list-ifaces BRIDGE          print the names of all interfaces on BRIDGE\n\
  iface-to-br IFACE           print name of bridge that contains IFACE\n\
\n\
Controller commands:\n\
  get-controller BRIDGE      print the controllers for BRIDGE\n\
  del-controller BRIDGE      delete the controllers for BRIDGE\n\
  set-controller BRIDGE TARGET...  set the controllers for BRIDGE\n\
  get-fail-mode BRIDGE       print the fail-mode for BRIDGE\n\
  del-fail-mode BRIDGE       delete the fail-mode for BRIDGE\n\
  set-fail-mode BRIDGE MODE  set the fail-mode for BRIDGE to MODE\n\
\n\
Manager commands:\n\
  get-manager                print the managers\n\
  del-manager                delete the managers\n\
  set-manager TARGET...      set the list of managers to TARGET...\n\
\n\
SSL commands:\n\
  get-ssl                     print the SSL configuration\n\
  del-ssl                     delete the SSL configuration\n\
  set-ssl PRIV-KEY CERT CA-CERT  set the SSL configuration\n\
\n\
Auto Attach commands:\n\
  add-aa-mapping BRIDGE I-SID VLAN   add Auto Attach mapping to BRIDGE\n\
  del-aa-mapping BRIDGE I-SID VLAN   delete Auto Attach mapping VLAN from BRIDGE\n\
  get-aa-mapping BRIDGE              get Auto Attach mappings from BRIDGE\n\
\n\
Switch commands:\n\
  emer-reset                  reset switch to known good state\n\
\n\
%s\
\n\
Options:\n\
  --db=DATABASE               connect to DATABASE\n\
                              (default: %s)\n\
  --no-wait                   do not wait for ovs-vswitchd to reconfigure\n\
  --retry                     keep trying to connect to server forever\n\
  -t, --timeout=SECS          wait at most SECS seconds for ovs-vswitchd\n\
  --dry-run                   do not commit changes to database\n\
  --oneline                   print exactly one line of output per command\n",
           program_name, program_name, ctl_get_db_cmd_usage(), ctl_default_db());
#ifdef OPS
    printf("\
\n\
VLAN commands:\n\
  list-vlans BRIDGE           print the names of all the VLANs on BRIDGE\n\
  add-vlan BRIDGE VID         add VLAN with VLAN ID (VID) to BRIDGE\n\
  del-vlan BRIDGE VID         delete VLAN with VLAN ID (VID) from BRIDGE\n\
\n\
VRF commands:\n\
  add-vrf VRF                 create a new vrf named VRF\n\
  del-vrf VRF                 delete VRF and all of its ports\n\
  list-vrf                    print the names of all the vrfs\n\
\n\
VRF Port commands:\n\
  list-vrf-ports VRF          print the names of all the ports on VRF\n\
  add-vrf-port VRF PORT       add network device PORT to VRF\n\
  del-vrf-port [VRF] PORT     delete PORT from VRF\n\
  port-to-vrf PORT            print name of vrf that contains PORT\n");
#endif
    vlog_usage();
    printf("\
  --no-syslog             equivalent to --verbose=vsctl:syslog:warn\n");
    stream_usage("database", true, true, false);
    printf("\n\
Other options:\n\
  -h, --help                  display this help message\n\
  -V, --version               display version information\n");
    exit(EXIT_SUCCESS);
}


/* ovs-vsctl specific context.  Inherits the 'struct ctl_context' as base. */
struct vsctl_context {
    struct ctl_context base;

    /* Modifiable state. */
    const struct ovsrec_open_vswitch *ovs;
    bool verified_ports;

    /* A cache of the contents of the database.
     *
     * A command that needs to use any of this information must first call
     * vsctl_context_populate_cache().  A command that changes anything that
     * could invalidate the cache must either call
     * vsctl_context_invalidate_cache() or manually update the cache to
     * maintain its correctness. */
    bool cache_valid;
    struct shash bridges;   /* Maps from bridge name to struct vsctl_bridge. */
    struct shash ports;     /* Maps from port name to struct vsctl_port. */
    struct shash ifaces;    /* Maps from port name to struct vsctl_iface. */
#ifdef OPS
    struct shash vrfs;      /* Maps from vrf name to struct vsctl_vrf. */
    struct shash orphan_ifaces; /* unused interfaces */
                            /* Maps from iface name to struct vsctl_iface. */
    bool subsystems_exist;
#endif
};

#ifdef OPS
struct vsctl_vrf {
    struct ovsrec_vrf *vrf_cfg;
    char *name;
    struct ovs_list ports;      /* Contains "struct vsctl_port"s. */
};
#endif


struct vsctl_bridge {
    struct ovsrec_bridge *br_cfg;
    char *name;
    struct ovs_list ports;      /* Contains "struct vsctl_port"s. */

    /* VLAN ("fake") bridge support.
     *
     * Use 'parent != NULL' to detect a fake bridge, because 'vlan' can be 0
     * in either case. */
    struct hmap children;        /* VLAN bridges indexed by 'vlan'. */
    struct hmap_node children_node; /* Node in parent's 'children' hmap. */
    struct vsctl_bridge *parent; /* Real bridge, or NULL. */
    int vlan;                    /* VLAN VID (0...4095), or 0. */
};

struct vsctl_port {
    struct ovs_list ports_node;  /* In struct vsctl_bridge's 'ports' list. */
    struct ovs_list ifaces;      /* Contains "struct vsctl_iface"s. */
    struct ovsrec_port *port_cfg;
    struct vsctl_bridge *bridge;
#ifdef OPS
    enum {BR_PORT, VRF_PORT} port_type;
    struct vsctl_vrf *vrf;
#endif
};

struct vsctl_iface {
    struct ovs_list ifaces_node; /* In struct vsctl_port's 'ifaces' list. */
    struct ovsrec_interface *iface_cfg;
    struct vsctl_port *port;
};

/* Casts 'base' into 'struct vsctl_context'. */
static struct vsctl_context *
vsctl_context_cast(struct ctl_context *base)
{
    return CONTAINER_OF(base, struct vsctl_context, base);
}

static struct vsctl_bridge *find_vlan_bridge(struct vsctl_bridge *parent,
                                             int vlan);

static char *
vsctl_context_to_string(const struct ctl_context *ctx)
{
    const struct shash_node *node;
    struct svec words;
    char *s;
    int i;

    svec_init(&words);
    SHASH_FOR_EACH (node, &ctx->options) {
        svec_add(&words, node->name);
    }
    for (i = 0; i < ctx->argc; i++) {
        svec_add(&words, ctx->argv[i]);
    }
    svec_terminate(&words);

    s = process_escape_args(words.names);

    svec_destroy(&words);

    return s;
}

static void
verify_ports(struct vsctl_context *vsctl_ctx)
{
    if (!vsctl_ctx->verified_ports) {
        const struct ovsrec_bridge *bridge;
        const struct ovsrec_port *port;

        ovsrec_open_vswitch_verify_bridges(vsctl_ctx->ovs);
        OVSREC_BRIDGE_FOR_EACH (bridge, vsctl_ctx->base.idl) {
            ovsrec_bridge_verify_ports(bridge);
        }
        OVSREC_PORT_FOR_EACH (port, vsctl_ctx->base.idl) {
            ovsrec_port_verify_interfaces(port);
        }

        vsctl_ctx->verified_ports = true;
    }
}

static struct vsctl_bridge *
add_bridge_to_cache(struct vsctl_context *vsctl_ctx,
                    struct ovsrec_bridge *br_cfg, const char *name,
                    struct vsctl_bridge *parent, int vlan)
{
    struct vsctl_bridge *br = xmalloc(sizeof *br);
    br->br_cfg = br_cfg;
    br->name = xstrdup(name);
    list_init(&br->ports);
    br->parent = parent;
    br->vlan = vlan;
    hmap_init(&br->children);
    if (parent) {
        struct vsctl_bridge *conflict = find_vlan_bridge(parent, vlan);
        if (conflict) {
            VLOG_WARN("%s: bridge has multiple VLAN bridges (%s and %s) "
                      "for VLAN %d, but only one is allowed",
                      parent->name, name, conflict->name, vlan);
        } else {
            hmap_insert(&parent->children, &br->children_node,
                        hash_int(vlan, 0));
        }
    }
    shash_add(&vsctl_ctx->bridges, br->name, br);
    return br;
}

static void
ovs_delete_bridge(const struct ovsrec_open_vswitch *ovs,
                  struct ovsrec_bridge *bridge)
{
    struct ovsrec_bridge **bridges;
    size_t i, n;

    bridges = xmalloc(sizeof *ovs->bridges * ovs->n_bridges);
    for (i = n = 0; i < ovs->n_bridges; i++) {
        if (ovs->bridges[i] != bridge) {
            bridges[n++] = ovs->bridges[i];
        }
    }
    ovsrec_open_vswitch_set_bridges(ovs, bridges, n);
    free(bridges);
}

static void
del_cached_bridge(struct vsctl_context *vsctl_ctx, struct vsctl_bridge *br)
{
    ovs_assert(list_is_empty(&br->ports));
    ovs_assert(hmap_is_empty(&br->children));
#ifndef OPS_TEMP
    if (br->parent) {
        hmap_remove(&br->parent->children, &br->children_node);
    }
#endif
    if (br->br_cfg) {
        ovsrec_bridge_delete(br->br_cfg);
        ovs_delete_bridge(vsctl_ctx->ovs, br->br_cfg);
    }
    shash_find_and_delete(&vsctl_ctx->bridges, br->name);
    hmap_destroy(&br->children);
    free(br->name);
    free(br);
}

#ifndef OPS_TEMP
static bool
port_is_fake_bridge(const struct ovsrec_port *port_cfg)
{
    return (port_cfg->fake_bridge
            && port_cfg->tag
            && *port_cfg->tag >= 0 && *port_cfg->tag <= 4095);
}
#endif

static struct vsctl_bridge *
find_vlan_bridge(struct vsctl_bridge *parent, int vlan)
{
    struct vsctl_bridge *child;

    HMAP_FOR_EACH_IN_BUCKET (child, children_node, hash_int(vlan, 0),
                             &parent->children) {
        if (child->vlan == vlan) {
            return child;
        }
    }

    return NULL;
}

#ifdef OPS
static struct vsctl_vrf *
add_vrf_to_cache(struct vsctl_context *ctx,
                 struct ovsrec_vrf *vrf_cfg, const char *name)
{
    struct vsctl_vrf *vrf = xmalloc(sizeof *vrf);
    vrf->vrf_cfg = vrf_cfg;
    vrf->name = xstrdup(name);
    list_init(&vrf->ports);
    shash_add(&ctx->vrfs, vrf->name, vrf);
    return vrf;
}

static struct vsctl_port *
add_vrf_port_to_cache(struct vsctl_context *ctx, struct vsctl_vrf *parent_vrf, 
                      struct ovsrec_port *port_cfg)
{
    struct vsctl_port *port;

    port = xmalloc(sizeof *port);
    list_push_back(&parent_vrf->ports, &port->ports_node);
    list_init(&port->ifaces);
    port->port_cfg = port_cfg;
    port->vrf = parent_vrf;
    port->port_type = VRF_PORT;
    shash_add(&ctx->ports, port_cfg->name, port);

    return port;
}
#endif

static struct vsctl_port *
add_port_to_cache(struct vsctl_context *vsctl_ctx, struct vsctl_bridge *parent,
                  struct ovsrec_port *port_cfg)
{
    struct vsctl_port *port;

    if (port_cfg->tag
        && *port_cfg->tag >= 0 && *port_cfg->tag <= 4095) {
        struct vsctl_bridge *vlan_bridge;

        vlan_bridge = find_vlan_bridge(parent, *port_cfg->tag);
        if (vlan_bridge) {
            parent = vlan_bridge;
        }
    }

    port = xmalloc(sizeof *port);
    list_push_back(&parent->ports, &port->ports_node);
    list_init(&port->ifaces);
    port->port_cfg = port_cfg;
    port->bridge = parent;
#ifdef OPS
    port->port_type = BR_PORT;
#endif
    shash_add(&vsctl_ctx->ports, port_cfg->name, port);

    return port;
}

static void
del_cached_port(struct vsctl_context *vsctl_ctx, struct vsctl_port *port)
{
    ovs_assert(list_is_empty(&port->ifaces));
    list_remove(&port->ports_node);
    shash_find_and_delete(&vsctl_ctx->ports, port->port_cfg->name);
    ovsrec_port_delete(port->port_cfg);
    free(port);
}

#ifdef OPS
static struct vsctl_iface *
add_orphan_iface_to_cache(struct vsctl_context *ctx,
                          const struct ovsrec_interface *iface_cfg)
{
    struct vsctl_iface *iface;

    iface = xmalloc(sizeof *iface);
    iface->iface_cfg = (struct ovsrec_interface *)iface_cfg;
    iface->port = NULL;
    shash_add(&ctx->orphan_ifaces, iface_cfg->name, iface);

    return iface;
}

static void
move_orphan_iface_to_cache(struct vsctl_context *ctx, struct vsctl_port *parent,
                   struct ovsrec_interface *iface_cfg)
{
    /* Move an interface from orphan_ifaces to ifaces (it's now in use). */
    struct vsctl_iface *iface;

    iface = find_orphan_iface(ctx, iface_cfg->name, true);
    list_push_back(&parent->ifaces, &iface->ifaces_node);
    iface->port = parent;
    shash_find_and_delete(&ctx->orphan_ifaces, iface_cfg->name);
    shash_add(&ctx->ifaces, iface_cfg->name, iface);
}
#endif

static struct vsctl_iface *
add_iface_to_cache(struct vsctl_context *vsctl_ctx, struct vsctl_port *parent,
                   struct ovsrec_interface *iface_cfg)
{
    struct vsctl_iface *iface;

    iface = xmalloc(sizeof *iface);
    list_push_back(&parent->ifaces, &iface->ifaces_node);
    iface->iface_cfg = iface_cfg;
    iface->port = parent;
    shash_add(&vsctl_ctx->ifaces, iface_cfg->name, iface);

    return iface;
}

static void
del_cached_iface(struct vsctl_context *vsctl_ctx, struct vsctl_iface *iface)
{
    list_remove(&iface->ifaces_node);
    shash_find_and_delete(&vsctl_ctx->ifaces, iface->iface_cfg->name);
    ovsrec_interface_delete(iface->iface_cfg);
    free(iface);
}

static void
vsctl_context_invalidate_cache(struct ctl_context *ctx)
{
    struct vsctl_context *vsctl_ctx = vsctl_context_cast(ctx);
    struct shash_node *node;

    if (!vsctl_ctx->cache_valid) {
        return;
    }
    vsctl_ctx->cache_valid = false;

    SHASH_FOR_EACH (node, &vsctl_ctx->bridges) {
        struct vsctl_bridge *bridge = node->data;
        hmap_destroy(&bridge->children);
        free(bridge->name);
        free(bridge);
    }
    shash_destroy(&vsctl_ctx->bridges);

#ifdef OPS
    SHASH_FOR_EACH (node, &vsctl_ctx->vrfs) {
        struct vsctl_vrf *vrf = node->data;
        free(vrf->name);
        free(vrf);
    }
    shash_destroy(&vsctl_ctx->vrfs);
#endif

    shash_destroy_free_data(&vsctl_ctx->ports);
    shash_destroy_free_data(&vsctl_ctx->ifaces);
#ifdef OPS
    shash_destroy_free_data(&vsctl_ctx->orphan_ifaces);
#endif
}

static void
pre_get_info(struct ctl_context *ctx)
{
    ovsdb_idl_add_column(ctx->idl, &ovsrec_open_vswitch_col_bridges);

    ovsdb_idl_add_column(ctx->idl, &ovsrec_bridge_col_name);
#ifndef OPS_TEMP
    ovsdb_idl_add_column(ctx->idl, &ovsrec_bridge_col_controller);
    ovsdb_idl_add_column(ctx->idl, &ovsrec_bridge_col_fail_mode);
#endif
    ovsdb_idl_add_column(ctx->idl, &ovsrec_bridge_col_ports);

    ovsdb_idl_add_column(ctx->idl, &ovsrec_port_col_name);
#ifndef OPS_TEMP
    ovsdb_idl_add_column(ctx->idl, &ovsrec_port_col_fake_bridge);
#endif
    ovsdb_idl_add_column(ctx->idl, &ovsrec_port_col_tag);
    ovsdb_idl_add_column(ctx->idl, &ovsrec_port_col_interfaces);

    ovsdb_idl_add_column(ctx->idl, &ovsrec_interface_col_name);
#ifndef OPS_TEMP
    ovsdb_idl_add_column(ctx->idl, &ovsrec_interface_col_ofport);
#endif
#ifdef OPS
    /* FIXME: We currently use presence of subsystems as an indicator
     * that this is a physical switch instead of a virtual switch.  This
     * isn't ideal, and we may want a more direct method of determining
     * this distinction.
     */
    ovsdb_idl_add_column(ctx->idl, &ovsrec_open_vswitch_col_subsystems);
#endif
}

static void
vsctl_context_populate_cache(struct ctl_context *ctx)
{
    struct vsctl_context *vsctl_ctx = vsctl_context_cast(ctx);
    const struct ovsrec_open_vswitch *ovs = vsctl_ctx->ovs;
    struct sset bridges, ports;
#ifdef OPS
    struct sset vrfs;
#endif
    size_t i;

    if (vsctl_ctx->cache_valid) {
        /* Cache is already populated. */
        return;
    }
    vsctl_ctx->cache_valid = true;
    shash_init(&vsctl_ctx->bridges);
    shash_init(&vsctl_ctx->ports);
    shash_init(&vsctl_ctx->ifaces);
#ifdef OPS
    shash_init(&vsctl_ctx->vrfs);
    shash_init(&vsctl_ctx->orphan_ifaces);
    /* OPS: Use presence of subsystems as an indicator that this
     * is a physical switch instead of a virtual switch.  This may be
     * changed to something explicit in the future.
     */
    vsctl_ctx->subsystems_exist = (ovs->n_subsystems != 0);
#endif

    sset_init(&bridges);
    sset_init(&ports);
    for (i = 0; i < ovs->n_bridges; i++) {
        struct ovsrec_bridge *br_cfg = ovs->bridges[i];
        struct vsctl_bridge *br;
        size_t j;

        if (!sset_add(&bridges, br_cfg->name)) {
            VLOG_WARN("%s: database contains duplicate bridge name",
                      br_cfg->name);
            continue;
        }
        br = add_bridge_to_cache(vsctl_ctx, br_cfg, br_cfg->name, NULL, 0);

        if (!br) {
            continue;
        }

        for (j = 0; j < br_cfg->n_ports; j++) {
            struct ovsrec_port *port_cfg = br_cfg->ports[j];

            if (!sset_add(&ports, port_cfg->name)) {
                /* Duplicate port name.  (We will warn about that later.) */
                continue;
            }

#ifndef OPS_TEMP
            if (port_is_fake_bridge(port_cfg)
                && sset_add(&bridges, port_cfg->name)) {
                add_bridge_to_cache(vsctl_ctx, NULL, port_cfg->name, br,
                                    *port_cfg->tag);
            }
#endif
        }
    }
    sset_destroy(&bridges);
    sset_destroy(&ports);

    sset_init(&bridges);
    for (i = 0; i < ovs->n_bridges; i++) {
        struct ovsrec_bridge *br_cfg = ovs->bridges[i];
        struct vsctl_bridge *br;
        size_t j;

        if (!sset_add(&bridges, br_cfg->name)) {
            continue;
        }
        br = shash_find_data(&vsctl_ctx->bridges, br_cfg->name);
        for (j = 0; j < br_cfg->n_ports; j++) {
            struct ovsrec_port *port_cfg = br_cfg->ports[j];
            struct vsctl_port *port;
            size_t k;

            port = shash_find_data(&vsctl_ctx->ports, port_cfg->name);
            if (port) {
                if (port_cfg == port->port_cfg) {
                    VLOG_WARN("%s: port is in multiple bridges (%s and %s)",
                              port_cfg->name, br->name, port->bridge->name);
                } else {
                    /* Log as an error because this violates the database's
                     * uniqueness constraints, so the database server shouldn't
                     * have allowed it. */
                    VLOG_ERR("%s: database contains duplicate port name",
                             port_cfg->name);
                }
                continue;
            }

#ifndef OPS_TEMP
            if (port_is_fake_bridge(port_cfg)
                && !sset_add(&bridges, port_cfg->name)) {
                continue;
            }
#endif
            port = add_port_to_cache(vsctl_ctx, br, port_cfg);
            for (k = 0; k < port_cfg->n_interfaces; k++) {
                struct ovsrec_interface *iface_cfg = port_cfg->interfaces[k];
                struct vsctl_iface *iface;

                iface = shash_find_data(&vsctl_ctx->ifaces, iface_cfg->name);
                if (iface) {
                    if (iface_cfg == iface->iface_cfg) {
                        VLOG_WARN("%s: interface is in multiple ports "
                                  "(%s and %s)",
                                  iface_cfg->name,
                                  iface->port->port_cfg->name,
                                  port->port_cfg->name);
                    } else {
                        /* Log as an error because this violates the database's
                         * uniqueness constraints, so the database server
                         * shouldn't have allowed it. */
                        VLOG_ERR("%s: database contains duplicate interface "
                                 "name", iface_cfg->name);
                    }
                    continue;
                }

                add_iface_to_cache(vsctl_ctx, port, iface_cfg);
            }
        }
    }

#ifdef OPS
    if (vsctl_ctx->subsystems_exist) {
        const struct ovsrec_interface *iface_cfg;
        struct vsctl_iface *iface;
        const struct ovsdb_idl *idl = ctx->idl;

        OVSREC_INTERFACE_FOR_EACH (iface_cfg, idl) {
            iface = shash_find_data(&vsctl_ctx->ifaces, iface_cfg->name);
            if (!iface) {
                add_orphan_iface_to_cache(vsctl_ctx, iface_cfg);
            }
        }
    }
#endif

    sset_destroy(&bridges);
#ifdef OPS
    sset_init(&vrfs);
    sset_init(&ports);
    for (i = 0; i < ovs->n_vrfs; i++) {
        struct ovsrec_vrf *vrf_cfg = ovs->vrfs[i];
        struct vsctl_vrf *vrf = NULL;
        size_t j = 0;

        if (!sset_add(&vrfs, vrf_cfg->name)) {
            VLOG_WARN("%s: database contains duplicate vrf name",
                      vrf_cfg->name);
            continue;
        }
        vrf = add_vrf_to_cache(vsctl_ctx, vrf_cfg, vrf_cfg->name);
        if (!vrf) {
            continue;
        }

        for (j = 0; j < vrf_cfg->n_ports; j++) {
            struct ovsrec_port *port_cfg = vrf_cfg->ports[j];

            if (!sset_add(&ports, port_cfg->name)) {
                /* Duplicate port name.  (We will warn about that later.) */
                continue;
            }
        }
    }

    sset_destroy(&vrfs);
    sset_destroy(&ports);

    sset_init(&vrfs);
    for (i = 0; i < ovs->n_vrfs; i++) {
        struct ovsrec_vrf *vrf_cfg = ovs->vrfs[i];
        struct vsctl_vrf *vrf;
        size_t j;

        if (!sset_add(&vrfs, vrf_cfg->name)) {
            continue;
        }
        vrf = shash_find_data(&vsctl_ctx->vrfs, vrf_cfg->name);
        for (j = 0; j < vrf_cfg->n_ports; j++) {
            struct ovsrec_port *port_cfg = vrf_cfg->ports[j];
            struct vsctl_port *port;
            size_t k;

            port = shash_find_data(&vsctl_ctx->ports, port_cfg->name);
            if (port) {
                if (port_cfg == port->port_cfg) {
                    VLOG_WARN("%s: port is in multiple vrfs (%s and %s)",
                              port_cfg->name, vrf->name, port->vrf->name);
                } else {
                    /* Log as an error because this violates the database's
                     * uniqueness constraints, so the database server shouldn't
                     * have allowed it. */
                    VLOG_ERR("%s: database contains duplicate port name",
                             port_cfg->name);
                }
                continue;
            }

            port = add_vrf_port_to_cache(vsctl_ctx, vrf, port_cfg);
            for (k = 0; k < port_cfg->n_interfaces; k++) {
                struct ovsrec_interface *iface_cfg = port_cfg->interfaces[k];
                struct vsctl_iface *iface;

                iface = shash_find_data(&vsctl_ctx->ifaces, iface_cfg->name);
                if (iface) {
                    if (iface_cfg == iface->iface_cfg) {
                        VLOG_WARN("%s: interface is in multiple ports "
                                  "(%s and %s)",
                                  iface_cfg->name,
                                  iface->port->port_cfg->name,
                                  port->port_cfg->name);
                    } else {
                        /* Log as an error because this violates the database's
                         * uniqueness constraints, so the database server
                         * shouldn't have allowed it. */
                        VLOG_ERR("%s: database contains duplicate interface "
                                 "name", iface_cfg->name);
                    }
                    continue;
                }

                add_iface_to_cache(vsctl_ctx, port, iface_cfg);
            }
        }
    }
    sset_destroy(&vrfs);
#endif
}

static void
check_conflicts(struct vsctl_context *vsctl_ctx, const char *name,
                char *msg)
{
    struct vsctl_iface *iface;
    struct vsctl_port *port;

    verify_ports(vsctl_ctx);

    if (shash_find(&vsctl_ctx->bridges, name)) {
        ctl_fatal("%s because a bridge named %s already exists",
                    msg, name);
    }

    port = shash_find_data(&vsctl_ctx->ports, name);
    if (port) {
        ctl_fatal("%s because a port named %s already exists on "
                    "bridge %s", msg, name, port->bridge->name);
    }

    /* OPS: Note that we are only checking in the ifaces dictionary,
     * not the orphan_ifaces dictionary.  That means that we are only
     * checking for in-use interfaces, since there can be interfaces
     * that exist but aren't referenced by any port.  That means that the
     * following code can safely dereference iface->port.
     */
    iface = shash_find_data(&vsctl_ctx->ifaces, name);
    if (iface) {
        ctl_fatal("%s because an interface named %s already exists "
                    "on bridge %s", msg, name, iface->port->bridge->name);
    }

    free(msg);
}

static struct vsctl_bridge *
find_bridge(struct vsctl_context *vsctl_ctx, const char *name, bool must_exist)
{
    struct vsctl_bridge *br;

    ovs_assert(vsctl_ctx->cache_valid);

    br = shash_find_data(&vsctl_ctx->bridges, name);
    if (must_exist && !br) {
        ctl_fatal("no bridge named %s", name);
    }
    ovsrec_open_vswitch_verify_bridges(vsctl_ctx->ovs);
    return br;
}

#ifndef OPS_TEMP
static struct vsctl_bridge *
find_real_bridge(struct vsctl_context *vsctl_ctx,
                 const char *name, bool must_exist)
{
    struct vsctl_bridge *br = find_bridge(vsctl_ctx, name, must_exist);
    if (br && br->parent) {
        ctl_fatal("%s is a fake bridge", name);
    }
    return br;
}
#endif
static struct vsctl_port *
find_port(struct vsctl_context *vsctl_ctx, const char *name, bool must_exist)
{
    struct vsctl_port *port;

    ovs_assert(vsctl_ctx->cache_valid);

    port = shash_find_data(&vsctl_ctx->ports, name);
    if (port && !strcmp(name, port->bridge->name)) {
        port = NULL;
    }
    if (must_exist && !port) {
        ctl_fatal("no port named %s", name);
    }
    verify_ports(vsctl_ctx);
    return port;
}

#ifdef OPS
static struct vsctl_iface *
find_orphan_iface(struct vsctl_context *ctx, const char *name, bool must_exist)
{
    struct vsctl_iface *iface;

    ovs_assert(ctx->cache_valid);

    iface = shash_find_data(&ctx->orphan_ifaces, name);
    if (must_exist && !iface) {
        ctl_fatal("no interface named %s", name);
    }
    verify_ports(ctx);
    return iface;
}
#endif

static struct vsctl_iface *
find_iface(struct vsctl_context *vsctl_ctx, const char *name, bool must_exist)
{
    struct vsctl_iface *iface;

    ovs_assert(vsctl_ctx->cache_valid);

    iface = shash_find_data(&vsctl_ctx->ifaces, name);
    if (iface && !strcmp(name, iface->port->bridge->name)) {
        iface = NULL;
    }
    if (must_exist && !iface) {
        ctl_fatal("no interface named %s", name);
    }
    verify_ports(vsctl_ctx);
    return iface;
}

static void
bridge_insert_port(struct ovsrec_bridge *br, struct ovsrec_port *port)
{
    struct ovsrec_port **ports;
    size_t i;

    ports = xmalloc(sizeof *br->ports * (br->n_ports + 1));
    for (i = 0; i < br->n_ports; i++) {
        ports[i] = br->ports[i];
    }
    ports[br->n_ports] = port;
    ovsrec_bridge_set_ports(br, ports, br->n_ports + 1);
    free(ports);
}

static void
bridge_delete_port(struct ovsrec_bridge *br, struct ovsrec_port *port)
{
    struct ovsrec_port **ports;
    size_t i, n;

    ports = xmalloc(sizeof *br->ports * br->n_ports);
    for (i = n = 0; i < br->n_ports; i++) {
        if (br->ports[i] != port) {
            ports[n++] = br->ports[i];
        }
    }
    ovsrec_bridge_set_ports(br, ports, n);
    free(ports);
}

static void
ovs_insert_bridge(const struct ovsrec_open_vswitch *ovs,
                  struct ovsrec_bridge *bridge)
{
    struct ovsrec_bridge **bridges;
    size_t i;

    bridges = xmalloc(sizeof *ovs->bridges * (ovs->n_bridges + 1));
    for (i = 0; i < ovs->n_bridges; i++) {
        bridges[i] = ovs->bridges[i];
    }
    bridges[ovs->n_bridges] = bridge;
    ovsrec_open_vswitch_set_bridges(ovs, bridges, ovs->n_bridges + 1);
    free(bridges);
}

static void
cmd_init(struct ctl_context *ctx OVS_UNUSED)
{
}

static struct cmd_show_table cmd_show_tables[] = {
    {&ovsrec_table_open_vswitch,
     NULL,
     {&ovsrec_open_vswitch_col_manager_options,
      &ovsrec_open_vswitch_col_bridges,
#ifdef OPS
      &ovsrec_open_vswitch_col_vrfs,
#endif
#ifndef OPS_TEMP
      &ovsrec_open_vswitch_col_ovs_version},
#else
      &ovsrec_open_vswitch_col_subsystems},
#endif
     {NULL, NULL, NULL}
    },

    {&ovsrec_table_bridge,
     &ovsrec_bridge_col_name,
     {
#ifndef OPS_TEMP
      &ovsrec_bridge_col_controller,
      &ovsrec_bridge_col_fail_mode,
#else
      &ovsrec_bridge_col_vlans,
#endif
      &ovsrec_bridge_col_ports},
     {NULL, NULL, NULL}
    },

#ifdef OPS
    {&ovsrec_table_vrf,
     &ovsrec_vrf_col_name,
     {&ovsrec_vrf_col_ports},
     {NULL, NULL, NULL}
    },

    {&ovsrec_table_vlan,
     &ovsrec_vlan_col_name,
     {&ovsrec_vlan_col_id,
      &ovsrec_vlan_col_description,
      &ovsrec_vlan_col_admin},
     {NULL, NULL, NULL}
    },
#endif
    {&ovsrec_table_port,
     &ovsrec_port_col_name,
     {&ovsrec_port_col_tag,
      &ovsrec_port_col_trunks,
      &ovsrec_port_col_interfaces},
     {NULL, NULL, NULL}
    },

    {&ovsrec_table_interface,
     &ovsrec_interface_col_name,
     {&ovsrec_interface_col_type,
#ifndef OPS_TEMP
      &ovsrec_interface_col_options,
#endif
      &ovsrec_interface_col_error},
     {NULL, NULL, NULL}
    },

#ifndef OPS_TEMP
    {&ovsrec_table_controller,
     &ovsrec_controller_col_target,
     {&ovsrec_controller_col_is_connected,
      NULL,
      NULL},
     {NULL, NULL, NULL}
    },
#endif
#ifdef OPS
    {&ovsrec_table_subsystem,
     &ovsrec_subsystem_col_name,
     {&ovsrec_subsystem_col_fans,
      &ovsrec_subsystem_col_temp_sensors,
      &ovsrec_subsystem_col_other_config},
     {NULL, NULL, NULL}
    },
    {&ovsrec_table_led,
     &ovsrec_led_col_id,
     {&ovsrec_led_col_state,
      &ovsrec_led_col_status,
      NULL},
     {NULL, NULL, NULL}
    },
    {&ovsrec_table_fan,
     &ovsrec_fan_col_name,
     {&ovsrec_fan_col_speed,
      &ovsrec_fan_col_rpm,
      &ovsrec_fan_col_status},
     {NULL, NULL, NULL}
    },
    {&ovsrec_table_temp_sensor,
     &ovsrec_temp_sensor_col_name,
     {&ovsrec_temp_sensor_col_max,
      &ovsrec_temp_sensor_col_min,
      &ovsrec_temp_sensor_col_temperature},
     {NULL, NULL, NULL}
    },
    {&ovsrec_table_power_supply,
     &ovsrec_power_supply_col_name,
     {
      &ovsrec_power_supply_col_status,
      NULL,
      NULL},
     {NULL, NULL, NULL}
    },
    {&ovsrec_table_ospf_router,
     &ovsrec_ospf_router_col_areas,
     {
      &ovsrec_ospf_router_col_router_id,
      &ovsrec_ospf_router_col_passive_interface_default,
      &ovsrec_ospf_router_col_distance},
      {NULL, NULL, NULL}
     },
    {&ovsrec_table_ospf_area,
     &ovsrec_ospf_area_col_abr_summary_lsas,
     {
      &ovsrec_ospf_area_col_area_type,
      &ovsrec_ospf_area_col_ospf_interfaces,
      &ovsrec_ospf_area_col_ospf_auth_type},
      {NULL, NULL, NULL}
     },
    {&ovsrec_table_ospf_vlink,
     &ovsrec_ospf_vlink_col_area_id,
     {
      &ovsrec_ospf_vlink_col_name,
      &ovsrec_ospf_vlink_col_peer_router_id,
      &ovsrec_ospf_vlink_col_other_config},
      {NULL, NULL, NULL}
     },
    {&ovsrec_table_ospf_interface,
     &ovsrec_ospf_interface_col_ifsm_state,
     {
      &ovsrec_ospf_interface_col_name,
      &ovsrec_ospf_interface_col_port,
      &ovsrec_ospf_interface_col_neighbors},
      {NULL, NULL, NULL}
     },
    {&ovsrec_table_ospf_lsa,
     &ovsrec_ospf_lsa_col_area_id,
     {
      &ovsrec_ospf_lsa_col_options,
      &ovsrec_ospf_lsa_col_ls_id,
      &ovsrec_ospf_lsa_col_ls_birth_time},
      {NULL, NULL, NULL}
     },
    {&ovsrec_table_ospf_neighbor,
     &ovsrec_ospf_neighbor_col_bdr,
     {
      &ovsrec_ospf_neighbor_col_dr,
      &ovsrec_ospf_neighbor_col_nbr_router_id,
      &ovsrec_ospf_neighbor_col_nbr_priority},
      {NULL, NULL, NULL}
     },
    {&ovsrec_table_ospf_summary_address,
     &ovsrec_ospf_summary_address_col_other_config,
     {
      &ovsrec_ospf_summary_address_col_prefix,
      NULL,
      NULL},
      {NULL, NULL, NULL}
     },
    {&ovsrec_table_ospf_nbma_neighbor,
     &ovsrec_ospf_nbma_neighbor_col_other_config,
     {
      &ovsrec_ospf_nbma_neighbor_col_nbr_router_id,
      &ovsrec_ospf_nbma_neighbor_col_interface_name,
      &ovsrec_ospf_nbma_neighbor_col_status},
      {NULL, NULL, NULL}
     },
    {&ovsrec_table_ospf_route,
     &ovsrec_ospf_route_col_paths,
     {
      &ovsrec_ospf_route_col_path_type,
      &ovsrec_ospf_route_col_route_info,
      &ovsrec_ospf_route_col_prefix},
     {NULL, NULL, NULL}
    },
    {&ovsrec_table_route,
     &ovsrec_route_col_address_family,
     {
      &ovsrec_route_col_distance,
      &ovsrec_route_col_from,
      &ovsrec_route_col_metric},
     {NULL, NULL, NULL}
    },
    {&ovsrec_table_nexthop,
     &ovsrec_nexthop_col_external_ids,
     {
      &ovsrec_nexthop_col_status,
      &ovsrec_nexthop_col_selected,
      &ovsrec_nexthop_col_weight},
     {NULL, NULL, NULL}
    },
#endif
    {&ovsrec_table_manager,
     &ovsrec_manager_col_target,
     {&ovsrec_manager_col_is_connected,
      NULL,
      NULL},
     {NULL, NULL, NULL}
    },

    {NULL, NULL, {NULL, NULL, NULL}, {NULL, NULL, NULL}}
};

static void
pre_cmd_emer_reset(struct ctl_context *ctx)
{
    ovsdb_idl_add_column(ctx->idl, &ovsrec_open_vswitch_col_manager_options);
    ovsdb_idl_add_column(ctx->idl, &ovsrec_open_vswitch_col_ssl);

#ifndef OPS_TEMP
    ovsdb_idl_add_column(ctx->idl, &ovsrec_bridge_col_controller);
    ovsdb_idl_add_column(ctx->idl, &ovsrec_bridge_col_fail_mode);
    ovsdb_idl_add_column(ctx->idl, &ovsrec_bridge_col_mirrors);
    ovsdb_idl_add_column(ctx->idl, &ovsrec_bridge_col_netflow);
    ovsdb_idl_add_column(ctx->idl, &ovsrec_bridge_col_sflow);
    ovsdb_idl_add_column(ctx->idl, &ovsrec_bridge_col_ipfix);
    ovsdb_idl_add_column(ctx->idl, &ovsrec_bridge_col_flood_vlans);
#endif
    ovsdb_idl_add_column(ctx->idl, &ovsrec_bridge_col_other_config);

    ovsdb_idl_add_column(ctx->idl, &ovsrec_port_col_other_config);

#ifndef OPS_TEMP
    ovsdb_idl_add_column(ctx->idl,
                          &ovsrec_interface_col_ingress_policing_rate);
    ovsdb_idl_add_column(ctx->idl,
                          &ovsrec_interface_col_ingress_policing_burst);
#endif
}

static void
cmd_emer_reset(struct ctl_context *ctx)
{
    struct vsctl_context *vsctl_ctx = vsctl_context_cast(ctx);
    const struct ovsdb_idl *idl = ctx->idl;
    const struct ovsrec_bridge *br;
    const struct ovsrec_port *port;
#ifndef OPS_TEMP
    const struct ovsrec_interface *iface;
    const struct ovsrec_mirror *mirror, *next_mirror;
    const struct ovsrec_controller *ctrl, *next_ctrl;
#endif
    const struct ovsrec_manager *mgr, *next_mgr;
#ifndef OPS_TEMP
    const struct ovsrec_netflow *nf, *next_nf;
#endif
    const struct ovsrec_ssl *ssl, *next_ssl;
#ifndef OPS_TEMP
    const struct ovsrec_sflow *sflow, *next_sflow;
    const struct ovsrec_ipfix *ipfix, *next_ipfix;
    const struct ovsrec_flow_sample_collector_set *fscset, *next_fscset;
#endif
    /* Reset the Open_vSwitch table. */
    ovsrec_open_vswitch_set_manager_options(vsctl_ctx->ovs, NULL, 0);
    ovsrec_open_vswitch_set_ssl(vsctl_ctx->ovs, NULL);

    OVSREC_BRIDGE_FOR_EACH (br, idl) {
        const char *hwaddr;

#ifndef OPS_TEMP
        ovsrec_bridge_set_controller(br, NULL, 0);
        ovsrec_bridge_set_fail_mode(br, NULL);
        ovsrec_bridge_set_mirrors(br, NULL, 0);
        ovsrec_bridge_set_netflow(br, NULL);
        ovsrec_bridge_set_sflow(br, NULL);
        ovsrec_bridge_set_ipfix(br, NULL);
        ovsrec_bridge_set_flood_vlans(br, NULL, 0);
#endif

        /* We only want to save the "hwaddr" key from other_config. */
        hwaddr = smap_get(&br->other_config, "hwaddr");
        if (hwaddr) {
            const struct smap smap = SMAP_CONST1(&smap, "hwaddr", hwaddr);
            ovsrec_bridge_set_other_config(br, &smap);
        } else {
            ovsrec_bridge_set_other_config(br, NULL);
        }
    }

    OVSREC_PORT_FOR_EACH (port, idl) {
        ovsrec_port_set_other_config(port, NULL);
    }

#ifndef OPS_TEMP
    OVSREC_INTERFACE_FOR_EACH (iface, idl) {
        /* xxx What do we do about gre/patch devices created by mgr? */

        ovsrec_interface_set_ingress_policing_rate(iface, 0);
        ovsrec_interface_set_ingress_policing_burst(iface, 0);
    }

    OVSREC_MIRROR_FOR_EACH_SAFE (mirror, next_mirror, idl) {
        ovsrec_mirror_delete(mirror);
    }

    OVSREC_CONTROLLER_FOR_EACH_SAFE (ctrl, next_ctrl, idl) {
        ovsrec_controller_delete(ctrl);
    }
#endif
    OVSREC_MANAGER_FOR_EACH_SAFE (mgr, next_mgr, idl) {
        ovsrec_manager_delete(mgr);
    }

#ifndef OPS_TEMP
    OVSREC_NETFLOW_FOR_EACH_SAFE (nf, next_nf, idl) {
        ovsrec_netflow_delete(nf);
    }
#endif
    OVSREC_SSL_FOR_EACH_SAFE (ssl, next_ssl, idl) {
        ovsrec_ssl_delete(ssl);
    }

#ifndef OPS_TEMP
    OVSREC_SFLOW_FOR_EACH_SAFE (sflow, next_sflow, idl) {
        ovsrec_sflow_delete(sflow);
    }

    OVSREC_IPFIX_FOR_EACH_SAFE (ipfix, next_ipfix, idl) {
        ovsrec_ipfix_delete(ipfix);
    }

    OVSREC_FLOW_SAMPLE_COLLECTOR_SET_FOR_EACH_SAFE (fscset, next_fscset, idl) {
        ovsrec_flow_sample_collector_set_delete(fscset);
    }
#endif
    vsctl_context_invalidate_cache(ctx);
}

static void
cmd_add_br(struct ctl_context *ctx)
{
    struct vsctl_context *vsctl_ctx = vsctl_context_cast(ctx);
    bool may_exist = shash_find(&ctx->options, "--may-exist") != NULL;
    const char *br_name, *parent_name;
#ifndef OPS
    struct ovsrec_interface *iface;
    int vlan;
#endif

    br_name = ctx->argv[1];
    if (ctx->argc == 2) {
        parent_name = NULL;
#ifndef OPS
        vlan = 0;
#endif
    }
#ifndef OPS
    else if (ctx->argc == 4) {
        parent_name = ctx->argv[2];
        vlan = atoi(ctx->argv[3]);
        if (vlan < 0 || vlan > 4095) {
            ctl_fatal("%s: vlan must be between 0 and 4095", ctx->argv[0]);
        }
    }
#endif
    else {
#ifndef OPS
        ctl_fatal("'%s' command takes exactly 1 or 3 arguments",
#else
        ctl_fatal("'%s' command takes exactly 1 argument",
#endif
                    ctx->argv[0]);
    }

    vsctl_context_populate_cache(ctx);
    if (may_exist) {
        struct vsctl_bridge *br;

        br = find_bridge(vsctl_ctx, br_name, false);
        if (br) {
#ifndef OPS
            if (!parent_name) {
                if (br->parent) {
                    ctl_fatal("\"--may-exist add-br %s\" but %s is "
                                "a VLAN bridge for VLAN %d",
                                br_name, br_name, br->vlan);
                }
            } else {
                if (!br->parent) {
                    ctl_fatal("\"--may-exist add-br %s %s %d\" but %s "
                                "is not a VLAN bridge",
                                br_name, parent_name, vlan, br_name);
                } else if (strcmp(br->parent->name, parent_name)) {
                    ctl_fatal("\"--may-exist add-br %s %s %d\" but %s "
                                "has the wrong parent %s",
                                br_name, parent_name, vlan,
                                br_name, br->parent->name);
                } else if (br->vlan != vlan) {
                    ctl_fatal("\"--may-exist add-br %s %s %d\" but %s "
                                "is a VLAN bridge for the wrong VLAN %d",
                                br_name, parent_name, vlan, br_name, br->vlan);
                }
            }
#endif
            return;
        }
    }
    check_conflicts(vsctl_ctx, br_name,
                    xasprintf("cannot create a bridge named %s", br_name));

    if (!parent_name) {
#ifndef OPS
        struct ovsrec_port *port;
#endif
        struct ovsrec_bridge *br;

#ifndef OPS
        iface = ovsrec_interface_insert(ctx->txn);
        ovsrec_interface_set_name(iface, br_name);
        ovsrec_interface_set_type(iface, "internal");

        port = ovsrec_port_insert(ctx->txn);
        ovsrec_port_set_name(port, br_name);
        ovsrec_port_set_interfaces(port, &iface, 1);
#endif

        br = ovsrec_bridge_insert(ctx->txn);
        ovsrec_bridge_set_name(br, br_name);
#ifndef OPS
        ovsrec_bridge_set_ports(br, &port, 1);
#endif

        ovs_insert_bridge(vsctl_ctx->ovs, br);
    }
#ifndef OPS
    else {
        struct vsctl_bridge *conflict;
        struct vsctl_bridge *parent;
        struct ovsrec_port *port;
        struct ovsrec_bridge *br;
        int64_t tag = vlan;

        parent = find_bridge(vsctl_ctx, parent_name, false);
        if (parent && parent->parent) {
            ctl_fatal("cannot create bridge with fake bridge as parent");
        }
        if (!parent) {
            ctl_fatal("parent bridge %s does not exist", parent_name);
        }
        conflict = find_vlan_bridge(parent, vlan);
        if (conflict) {
            ctl_fatal("bridge %s already has a child VLAN bridge %s "
                        "on VLAN %d", parent_name, conflict->name, vlan);
        }
        br = parent->br_cfg;

        iface = ovsrec_interface_insert(ctx->txn);
        ovsrec_interface_set_name(iface, br_name);
        ovsrec_interface_set_type(iface, "internal");

        port = ovsrec_port_insert(ctx->txn);
        ovsrec_port_set_name(port, br_name);
        ovsrec_port_set_interfaces(port, &iface, 1);
        ovsrec_port_set_fake_bridge(port, true);
        ovsrec_port_set_tag(port, &tag, 1);

        bridge_insert_port(br, port);
    }

    post_db_reload_expect_iface(iface);
#endif
    vsctl_context_invalidate_cache(ctx);
}

static void
del_port(struct vsctl_context *vsctl_ctx, struct vsctl_port *port)
{
    struct vsctl_iface *iface, *next_iface;

    bridge_delete_port((port->bridge->parent
                        ? port->bridge->parent->br_cfg
                        : port->bridge->br_cfg), port->port_cfg);

    LIST_FOR_EACH_SAFE (iface, next_iface, ifaces_node, &port->ifaces) {
        del_cached_iface(vsctl_ctx, iface);
    }
    del_cached_port(vsctl_ctx, port);
}

static void
del_bridge(struct vsctl_context *vsctl_ctx, struct vsctl_bridge *br)
{
#ifndef OPS
    struct vsctl_bridge *child, *next_child;
#endif
    struct vsctl_port *port, *next_port;
#ifndef OPS_TEMP
    const struct ovsrec_flow_sample_collector_set *fscset, *next_fscset;

    HMAP_FOR_EACH_SAFE (child, next_child, children_node, &br->children) {
        del_bridge(vsctl_ctx, child);
    }
#endif

    LIST_FOR_EACH_SAFE (port, next_port, ports_node, &br->ports) {
        del_port(vsctl_ctx, port);
    }

#ifndef OPS_TEMP
    OVSREC_FLOW_SAMPLE_COLLECTOR_SET_FOR_EACH_SAFE (fscset, next_fscset,
                                                    vsctl_ctx->base.idl) {
        if (fscset->bridge == br->br_cfg) {
            ovsrec_flow_sample_collector_set_delete(fscset);
        }
    }
#endif
    del_cached_bridge(vsctl_ctx, br);
}

static void
cmd_del_br(struct ctl_context *ctx)
{
    struct vsctl_context *vsctl_ctx = vsctl_context_cast(ctx);
    bool must_exist = !shash_find(&ctx->options, "--if-exists");
    struct vsctl_bridge *bridge;

    vsctl_context_populate_cache(ctx);
    bridge = find_bridge(vsctl_ctx, ctx->argv[1], must_exist);
    if (bridge) {
        del_bridge(vsctl_ctx, bridge);
    }
#ifdef OPS
    /* There is a difference in behavior between OPS and standard OVS:
     * (interface automatic deletion) means that we don't know if
     * the cache is correct or not after deleting a port
     */
    vsctl_context_invalidate_cache(ctx);
#endif
}

static void
output_sorted(struct svec *svec, struct ds *output)
{
    const char *name;
    size_t i;

    svec_sort(svec);
    SVEC_FOR_EACH (i, name, svec) {
        ds_put_format(output, "%s\n", name);
    }
}

static void
cmd_list_br(struct ctl_context *ctx)
{
    struct vsctl_context *vsctl_ctx = vsctl_context_cast(ctx);
    struct shash_node *node;
    struct svec bridges;
    bool real = shash_find(&ctx->options, "--real");
    bool fake = shash_find(&ctx->options, "--fake");

    /* If neither fake nor real were requested, return both. */
    if (!real && !fake) {
        real = fake = true;
    }

    vsctl_context_populate_cache(ctx);

    svec_init(&bridges);
    SHASH_FOR_EACH (node, &vsctl_ctx->bridges) {
        struct vsctl_bridge *br = node->data;

        if (br->parent ? fake : real) {
            svec_add(&bridges, br->name);
        }
    }
    output_sorted(&bridges, &ctx->output);
    svec_destroy(&bridges);
}

static void
cmd_br_exists(struct ctl_context *ctx)
{
    struct vsctl_context *vsctl_ctx = vsctl_context_cast(ctx);

    vsctl_context_populate_cache(ctx);
    if (!find_bridge(vsctl_ctx, ctx->argv[1], false)) {
        vsctl_exit(2);
    }
}

static void
set_external_id(struct smap *old, struct smap *new,
                char *key, char *value)
{
    smap_clone(new, old);

    if (value) {
        smap_replace(new, key, value);
    } else {
        smap_remove(new, key);
    }
}

static void
pre_cmd_br_set_external_id(struct ctl_context *ctx)
{
    pre_get_info(ctx);
    ovsdb_idl_add_column(ctx->idl, &ovsrec_bridge_col_external_ids);
    ovsdb_idl_add_column(ctx->idl, &ovsrec_port_col_external_ids);
}

static void
cmd_br_set_external_id(struct ctl_context *ctx)
{
    struct vsctl_context *vsctl_ctx = vsctl_context_cast(ctx);
    struct vsctl_bridge *bridge;
    struct smap new;

    vsctl_context_populate_cache(ctx);
    bridge = find_bridge(vsctl_ctx, ctx->argv[1], true);
    if (bridge->br_cfg) {

        set_external_id(&bridge->br_cfg->external_ids, &new, ctx->argv[2],
                        ctx->argc >= 4 ? ctx->argv[3] : NULL);
        ovsrec_bridge_verify_external_ids(bridge->br_cfg);
        ovsrec_bridge_set_external_ids(bridge->br_cfg, &new);
    } else {
        char *key = xasprintf("fake-bridge-%s", ctx->argv[2]);
        struct vsctl_port *port = shash_find_data(&vsctl_ctx->ports,
                                                  ctx->argv[1]);
        set_external_id(&port->port_cfg->external_ids, &new,
                        key, ctx->argc >= 4 ? ctx->argv[3] : NULL);
        ovsrec_port_verify_external_ids(port->port_cfg);
        ovsrec_port_set_external_ids(port->port_cfg, &new);
        free(key);
    }
    smap_destroy(&new);
}

static void
get_external_id(struct smap *smap, const char *prefix, const char *key,
                struct ds *output)
{
    if (key) {
        char *prefix_key = xasprintf("%s%s", prefix, key);
        const char *value = smap_get(smap, prefix_key);

        if (value) {
            ds_put_format(output, "%s\n", value);
        }
        free(prefix_key);
    } else {
        const struct smap_node **sorted = smap_sort(smap);
        size_t prefix_len = strlen(prefix);
        size_t i;

        for (i = 0; i < smap_count(smap); i++) {
            const struct smap_node *node = sorted[i];
            if (!strncmp(node->key, prefix, prefix_len)) {
                ds_put_format(output, "%s=%s\n", node->key + prefix_len,
                              node->value);
            }
        }
        free(sorted);
    }
}

static void
pre_cmd_br_get_external_id(struct ctl_context *ctx)
{
    pre_cmd_br_set_external_id(ctx);
}

static void
cmd_br_get_external_id(struct ctl_context *ctx)
{
    struct vsctl_context *vsctl_ctx = vsctl_context_cast(ctx);
    struct vsctl_bridge *bridge;

    vsctl_context_populate_cache(ctx);

    bridge = find_bridge(vsctl_ctx, ctx->argv[1], true);
    if (bridge->br_cfg) {
        ovsrec_bridge_verify_external_ids(bridge->br_cfg);
        get_external_id(&bridge->br_cfg->external_ids, "",
                        ctx->argc >= 3 ? ctx->argv[2] : NULL, &ctx->output);
    } else {
        struct vsctl_port *port = shash_find_data(&vsctl_ctx->ports,
                                                  ctx->argv[1]);
        ovsrec_port_verify_external_ids(port->port_cfg);
        get_external_id(&port->port_cfg->external_ids, "fake-bridge-",
                        ctx->argc >= 3 ? ctx->argv[2] : NULL, &ctx->output);
    }
}

#ifdef OPS
/* We only support VLAN IDs 1-4094. */
#define VALID_VLAN_ID(v)  ((v) > 0 && (v) < 4095)

static void
pre_get_vlan_info(struct ctl_context *ctx)
{
    ovsdb_idl_add_column(ctx->idl, &ovsrec_open_vswitch_col_bridges);

    ovsdb_idl_add_table(ctx->idl, &ovsrec_table_bridge);
    ovsdb_idl_add_column(ctx->idl, &ovsrec_bridge_col_name);
    ovsdb_idl_add_column(ctx->idl, &ovsrec_bridge_col_vlans);

    ovsdb_idl_add_table(ctx->idl, &ovsrec_table_vlan);
    ovsdb_idl_add_column(ctx->idl, &ovsrec_vlan_col_name);
    ovsdb_idl_add_column(ctx->idl, &ovsrec_vlan_col_id);
}

static void
bridge_insert_vlan(struct ovsrec_bridge *br, struct ovsrec_vlan *vlan)
{
    struct ovsrec_vlan **vlans;
    size_t i;

    vlans = xmalloc(sizeof *br->vlans * (br->n_vlans + 1));
    for (i = 0; i < br->n_vlans; i++) {
        vlans[i] = br->vlans[i];
    }
    vlans[br->n_vlans] = vlan;
    ovsrec_bridge_set_vlans(br, vlans, br->n_vlans + 1);
    free(vlans);
}

static void
bridge_delete_vlan(struct ovsrec_bridge *br, struct ovsrec_vlan *vlan)
{
    struct ovsrec_vlan **vlans;
    size_t i, n;

    vlans = xmalloc(sizeof *br->vlans * br->n_vlans);
    for (i = n = 0; i < br->n_vlans; i++) {
        if (br->vlans[i] != vlan) {
            vlans[n++] = br->vlans[i];
        }
    }
    ovsrec_bridge_set_vlans(br, vlans, n);
    free(vlans);
}

static void
cmd_list_vlans(struct ctl_context *ctx)
{
    struct vsctl_context *vsctl_ctx = vsctl_context_cast(ctx);
    int i;
    struct vsctl_bridge *br;
    struct svec vlans;

    vsctl_context_populate_cache(ctx);
    br = find_bridge(vsctl_ctx, ctx->argv[1], true);

    svec_init(&vlans);
    for (i = 0; i < br->br_cfg->n_vlans; i++) {
        struct ovsrec_vlan *vlan_cfg = br->br_cfg->vlans[i];
        svec_add(&vlans, vlan_cfg->name);
    }

    output_sorted(&vlans, &ctx->output);
    svec_destroy(&vlans);
}

static void
cmd_add_vlan(struct ctl_context *ctx)
{
    struct vsctl_context *vsctl_ctx = vsctl_context_cast(ctx);
    int i, vid;
    char vlan_name[16];
    char *br_name;
    int n_settings;
    char **settings;
    struct vsctl_bridge *bridge = NULL;
    struct ovsrec_vlan *vlan = NULL;
    bool may_exist = shash_find(&ctx->options, "--may-exist") != NULL;

    br_name = ctx->argv[1];
    vid = atoi(ctx->argv[2]);
    if (!VALID_VLAN_ID(vid)) {
        ds_put_format(&ctx->output, "Invalid VLAN ID %s\n", ctx->argv[2]);
        return;
    } else {
        /* For the purpose of ovs-vsctl tool, construct
         * fixed VLAN name based on VID. */
        snprintf(vlan_name, 16, "VLAN%d", vid);
    }
    settings = &ctx->argv[3];
    n_settings = ctx->argc - 3;

    vsctl_context_populate_cache(ctx);
    bridge = find_bridge(vsctl_ctx, br_name, true);

    if (may_exist) {
        int i;
        for (i=0; i<bridge->br_cfg->n_vlans; i++) {
            if (vid == bridge->br_cfg->vlans[i]->id) {
                ds_put_format(&ctx->output, "VLAN %d already exists on bridge %s\n",
                              vid, bridge->br_cfg->name);
                return;
            }
        }
    }

    ds_put_format(&ctx->output, "Creating VLAN %d on bridge %s\n",
                  vid, bridge->br_cfg->name);

    vlan = ovsrec_vlan_insert(ctx->txn);
    ovsrec_vlan_set_name(vlan, vlan_name);
    ovsrec_vlan_set_id(vlan, vid);
    ovsrec_vlan_set_oper_state(vlan,
                               OVSREC_VLAN_OPER_STATE_UNKNOWN);
    ovsrec_vlan_set_oper_state_reason(vlan,
                                      OVSREC_VLAN_OPER_STATE_REASON_UNKNOWN);
    for (i = 0; i < n_settings; i++) {
        ctl_set_column("VLAN", &vlan->header_, settings[i],
                   ctx->symtab);
    }

    bridge_insert_vlan(bridge->br_cfg, vlan);
}

static void
cmd_del_vlan(struct ctl_context *ctx)
{
    struct vsctl_context *vsctl_ctx = vsctl_context_cast(ctx);
    int i, vid;
    bool found = false;
    char *br_name;
    struct vsctl_bridge *bridge = NULL;
    struct ovsrec_vlan *vlan = NULL;
    bool if_exist = shash_find(&ctx->options, "--if-exists") != NULL;

    br_name = ctx->argv[1];
    vid = atoi(ctx->argv[2]);
    if (!VALID_VLAN_ID(vid)) {
        ds_put_format(&ctx->output, "Invalid VLAN ID %s\n", ctx->argv[2]);
        return;
    }

    vsctl_context_populate_cache(ctx);
    bridge = find_bridge(vsctl_ctx, br_name, true);

    for (i=0; i<bridge->br_cfg->n_vlans; i++) {
        if (vid == bridge->br_cfg->vlans[i]->id) {
            found = true;
            vlan = bridge->br_cfg->vlans[i];
            break;
        }
    }

    if (!found) {
        if (if_exist) {
            ds_put_format(&ctx->output, "VLAN %d does not exist on bridge %s\n",
                          vid, bridge->br_cfg->name);
            return;
        } else {
            ctl_fatal("Error - VLAN %d does not exist on bridge %s!\n",
                        vid, bridge->br_cfg->name);
        }
    }

    ds_put_format(&ctx->output, "Deleting VLAN %d on bridge %s\n",
                  vid, bridge->br_cfg->name);

    bridge_delete_vlan(bridge->br_cfg, vlan);
}

static void
pre_get_vrf_info(struct ctl_context *ctx)
{
    ovsdb_idl_add_column(ctx->idl, &ovsrec_open_vswitch_col_vrfs);

    ovsdb_idl_add_column(ctx->idl, &ovsrec_vrf_col_name);
    ovsdb_idl_add_column(ctx->idl, &ovsrec_vrf_col_ports);

    ovsdb_idl_add_column(ctx->idl, &ovsrec_port_col_name);
    ovsdb_idl_add_column(ctx->idl, &ovsrec_port_col_interfaces);

    ovsdb_idl_add_column(ctx->idl, &ovsrec_interface_col_name);
    /* FIXME: We currently use presence of subsystems as an indicator
     * that this is a physical switch instead of a virtual switch.  This
     * isn't ideal, and we may want a more direct method of determining
     * this distinction.
     */
    ovsdb_idl_add_column(ctx->idl, &ovsrec_open_vswitch_col_subsystems);
}

static void
verify_vrf_ports(struct vsctl_context *ctx)
{
    if (!ctx->verified_ports) {
        const struct ovsrec_vrf *vrf;
        const struct ovsrec_port *port;

        ovsrec_open_vswitch_verify_vrfs(ctx->ovs);
        OVSREC_VRF_FOR_EACH (vrf, ctx->base.idl) {
            ovsrec_vrf_verify_ports(vrf);
        }
        OVSREC_PORT_FOR_EACH (port, ctx->base.idl) {
            ovsrec_port_verify_interfaces(port);
        }

        ctx->verified_ports = true;
    }
}

static struct vsctl_iface *
find_orphan_vrf_iface(struct vsctl_context *ctx, const char *name, bool must_exist)
{
    struct vsctl_iface *iface;

    ovs_assert(ctx->cache_valid);

    iface = shash_find_data(&ctx->orphan_ifaces, name);
    if (must_exist && !iface) {
        ctl_fatal("no interface named %s", name);
    }
    verify_vrf_ports(ctx);
    return iface;
}

static void
check_vrf_conflicts(struct vsctl_context *ctx, const char *name,
                    char *msg)
{
    verify_vrf_ports(ctx);

    if (shash_find(&ctx->vrfs, name)) {
        ctl_fatal("%s because a vrf named %s already exists",
                    msg, name);
    }

    free(msg);
}

static struct vsctl_vrf *
find_vrf(struct vsctl_context *ctx, const char *name, bool must_exist)
{
    struct vsctl_vrf *vrf;

    ovs_assert(ctx->cache_valid);

    vrf = shash_find_data(&ctx->vrfs, name);
    if (must_exist && !vrf) {
        ctl_fatal("no vrf named %s", name);
    }
    ovsrec_open_vswitch_verify_vrfs(ctx->ovs);
    return vrf;
}

static struct vsctl_port *
find_vrf_port(struct vsctl_context *ctx, const char *name, bool must_exist)
{
    struct vsctl_port *port;

    ovs_assert(ctx->cache_valid);

    port = shash_find_data(&ctx->ports, name);
    if (must_exist && !port) {
        ctl_fatal("no port named %s", name);
    }
    verify_vrf_ports(ctx);
    return port;
}

static void
cmd_list_vrf_ports(struct ctl_context *ctx)
{
    struct vsctl_context *vsctl_ctx = vsctl_context_cast(ctx);
    struct vsctl_vrf *vrf;
    struct vsctl_port *port;
    struct svec ports;

    vsctl_context_populate_cache(ctx);
    vrf = find_vrf(vsctl_ctx, ctx->argv[1], true);
    ovsrec_vrf_verify_ports(vrf->vrf_cfg);

    svec_init(&ports);
    LIST_FOR_EACH (port, ports_node, &vrf->ports) {
        if (strcmp(port->port_cfg->name, vrf->name)) {
            svec_add(&ports, port->port_cfg->name);
        }
    }
    output_sorted(&ports, &ctx->output);
    svec_destroy(&ports);
}

static void
vrf_insert_port(struct ovsrec_vrf *vrf, struct ovsrec_port *port)
{
    struct ovsrec_port **ports;
    size_t i;

    ports = xmalloc(sizeof *vrf->ports * (vrf->n_ports + 1));
    for (i = 0; i < vrf->n_ports; i++) {
        ports[i] = vrf->ports[i];
    }
    ports[vrf->n_ports] = port;
    ovsrec_vrf_set_ports(vrf, ports, vrf->n_ports + 1);
    free(ports);
}

static void
add_vrf_port(struct ctl_context *ctx,
         const char *vrf_name, const char *port_name,
         bool may_exist,
         char *iface_names[], int n_ifaces)
{
    struct vsctl_context *vsctl_ctx = vsctl_context_cast(ctx);
    struct vsctl_port *vsctl_port;
    struct vsctl_vrf *vrf = NULL;
    struct ovsrec_interface **ifaces;
    struct ovsrec_port *port;
    size_t i;

    vsctl_context_populate_cache(ctx);
    if (may_exist) {
        struct vsctl_port *vsctl_port;

        vsctl_port = find_vrf_port(vsctl_ctx, port_name, false);
        if (vsctl_port) {
            if (vsctl_port->port_type == VRF_PORT &&
                strcmp(vsctl_port->vrf->name, vrf_name)) {
                char *command = vsctl_context_to_string(ctx);
                ctl_fatal("\"%s\" but %s is actually attached to vrf %s",
                            command, port_name, vsctl_port->vrf->name);
            } else if (vsctl_port->port_type == BR_PORT) {
                char *command = vsctl_context_to_string(ctx);
                ctl_fatal("\"%s\" but %s is actually attached to bridge %s",
                            command, port_name, vsctl_port->bridge->name);
            }
            return;
        }
    }

    vrf = find_vrf(vsctl_ctx, vrf_name, true);

    ifaces = xmalloc(n_ifaces * sizeof *ifaces);
    for (i = 0; i < n_ifaces; i++) {
        /* find the existing interface in the orphan_ifaces dictionary */
        if (vsctl_ctx->subsystems_exist) {
            struct vsctl_iface *iface;
            iface = find_orphan_vrf_iface(vsctl_ctx, iface_names[i], true);
            ifaces[i] = (struct ovsrec_interface *)iface->iface_cfg;
        } else {
            ifaces[i] = ovsrec_interface_insert(ctx->txn);
            ovsrec_interface_set_name(ifaces[i], iface_names[i]);
            post_db_reload_expect_iface(ifaces[i]);
        }
    }

    port = ovsrec_port_insert(ctx->txn);
    ovsrec_port_set_name(port, port_name);
    ovsrec_port_set_interfaces(port, ifaces, n_ifaces);

    vrf_insert_port(vrf->vrf_cfg, port);

    vsctl_port = add_vrf_port_to_cache(vsctl_ctx, vrf, port);
    for (i = 0; i < n_ifaces; i++) {
        if (vsctl_ctx->subsystems_exist) {
            move_orphan_iface_to_cache(vsctl_ctx, vsctl_port, ifaces[i]);
        } else {
            add_iface_to_cache(vsctl_ctx, vsctl_port, ifaces[i]);
        }
    }
    free(ifaces);
}

static void
cmd_add_vrf_port(struct ctl_context *ctx)
{
    bool may_exist = shash_find(&ctx->options, "--may-exist") != NULL;
    add_vrf_port(ctx, ctx->argv[1], ctx->argv[2], may_exist, &ctx->argv[2], 1);
}

static void
vrf_delete_port(struct ovsrec_vrf *vrf, struct ovsrec_port *port)
{
    struct ovsrec_port **ports;
    size_t i, n;

    ports = xmalloc(sizeof *vrf->ports * vrf->n_ports);
    for (i = n = 0; i < vrf->n_ports; i++) {
        if (vrf->ports[i] != port) {
            ports[n++] = vrf->ports[i];
        }
    }
    ovsrec_vrf_set_ports(vrf, ports, n);
    free(ports);
}

static void
del_vrf_port(struct vsctl_context *ctx, struct vsctl_port *port)
{
    struct vsctl_iface *iface, *next_iface;

    vrf_delete_port(port->vrf->vrf_cfg, port->port_cfg);

    LIST_FOR_EACH_SAFE (iface, next_iface, ifaces_node, &port->ifaces) {
        del_cached_iface(ctx, iface);
    }
    del_cached_port(ctx, port);
}

static void
cmd_del_vrf_port(struct ctl_context *ctx)
{
    struct vsctl_context *vsctl_ctx = vsctl_context_cast(ctx);
    bool must_exist = !shash_find(&ctx->options, "--if-exists");
    const char *target = ctx->argv[ctx->argc - 1];
    struct vsctl_port *port;

    vsctl_context_populate_cache(ctx);

    port = find_vrf_port(vsctl_ctx, target, must_exist);

    if (port) {
        if (ctx->argc == 3) {
            struct vsctl_vrf *vrf;

            vrf = find_vrf(vsctl_ctx, ctx->argv[1], true);
            if (port->vrf != vrf) {
                ctl_fatal("vrf %s does not have a port %s",
                            ctx->argv[1], ctx->argv[2]);
            }
        }

        del_vrf_port(vsctl_ctx, port);
    }
    /* difference in behavior between OPS and standard OVS
     * (interface automatic deletion) means that we don't know if
     * the cache is correct or not after deleting a port
     */
    vsctl_context_invalidate_cache(ctx);
}

static void
cmd_port_to_vrf(struct ctl_context *ctx)
{
    struct vsctl_context *vsctl_ctx = vsctl_context_cast(ctx);
    struct vsctl_port *port;

    vsctl_context_populate_cache(ctx);

    port = find_vrf_port(vsctl_ctx, ctx->argv[1], true);
    ds_put_format(&ctx->output, "%s\n", port->vrf->name);
}

static void
cmd_list_vrf(struct ctl_context *ctx)
{
    struct vsctl_context *vsctl_ctx = vsctl_context_cast(ctx);
    struct shash_node *node;
    struct svec vrfs;

    vsctl_context_populate_cache(ctx);

    svec_init(&vrfs);
    SHASH_FOR_EACH (node, &vsctl_ctx->vrfs) {
        struct vsctl_vrf *vrf = node->data;
        svec_add(&vrfs, vrf->name);
    }
    output_sorted(&vrfs, &ctx->output);
    svec_destroy(&vrfs);
}

static void
ovs_insert_vrf(const struct ovsrec_open_vswitch *ovs,
               struct ovsrec_vrf *vrf)
{
    struct ovsrec_vrf **vrfs;
    size_t i;

    vrfs = xmalloc(sizeof *ovs->vrfs * (ovs->n_vrfs + 1));
    for (i = 0; i < ovs->n_vrfs; i++) {
        vrfs[i] = ovs->vrfs[i];
    }
    vrfs[ovs->n_vrfs] = vrf;
    ovsrec_open_vswitch_set_vrfs(ovs, vrfs, ovs->n_vrfs + 1);
    free(vrfs);
}

static void
cmd_add_vrf(struct ctl_context *ctx)
{
    struct vsctl_context *vsctl_ctx = vsctl_context_cast(ctx);
    struct ovsrec_vrf *vrf;
    const char *vrf_name;
    bool may_exist = shash_find(&ctx->options, "--may-exist") != NULL;

    vrf_name = ctx->argv[1];

    vsctl_context_populate_cache(ctx);
    if (may_exist) {
        struct vsctl_vrf *vrf;

        vrf = find_vrf(vsctl_ctx, vrf_name, false);
        if (vrf) {
            return;
        }
    }
    check_vrf_conflicts(vsctl_ctx, vrf_name,
                        xasprintf("cannot create a vrf named %s", vrf_name));

    vrf = ovsrec_vrf_insert(ctx->txn);
    ovsrec_vrf_set_name(vrf, vrf_name);

    ovs_insert_vrf(vsctl_ctx->ovs, vrf);

    vsctl_context_invalidate_cache(ctx);
}

static void
ovs_delete_vrf(const struct ovsrec_open_vswitch *ovs,
                  struct ovsrec_vrf *vrf)
{
    struct ovsrec_vrf **vrfs;
    size_t i, n;

    vrfs = xmalloc((sizeof *ovs->vrfs) * ovs->n_vrfs);
    for (i = n = 0; i < ovs->n_vrfs; i++) {
        if (ovs->vrfs[i] != vrf) {
            vrfs[n++] = ovs->vrfs[i];
        }
    }
    ovsrec_open_vswitch_set_vrfs(ovs, vrfs, n);
    free(vrfs);
}

static void
del_cached_vrf(struct vsctl_context *ctx, struct vsctl_vrf *vrf)
{
    ovs_assert(list_is_empty(&vrf->ports));

    if (vrf->vrf_cfg) {
        ovsrec_vrf_delete(vrf->vrf_cfg);
        ovs_delete_vrf(ctx->ovs, vrf->vrf_cfg);
    }
    shash_find_and_delete(&ctx->vrfs, vrf->name);
    free(vrf->name);
    free(vrf);
}

static void
del_vrf(struct vsctl_context *ctx, struct vsctl_vrf *vrf)
{
    struct vsctl_port *port, *next_port;

    LIST_FOR_EACH_SAFE (port, next_port, ports_node, &vrf->ports) {
        del_vrf_port(ctx, port);
    }

    del_cached_vrf(ctx, vrf);
}

static void
cmd_del_vrf(struct ctl_context *ctx)
{
    struct vsctl_context *vsctl_ctx = vsctl_context_cast(ctx);
    struct vsctl_vrf *vrf;
    bool must_exist = !shash_find(&ctx->options, "--if-exists");

    vsctl_context_populate_cache(ctx);
    vrf = find_vrf(vsctl_ctx, ctx->argv[1], must_exist);
    if (vrf) {
        del_vrf(vsctl_ctx, vrf);
    }
}

#endif

static void
cmd_list_ports(struct ctl_context *ctx)
{
    struct vsctl_context *vsctl_ctx = vsctl_context_cast(ctx);
    struct vsctl_bridge *br;
    struct vsctl_port *port;
    struct svec ports;

    vsctl_context_populate_cache(ctx);
    br = find_bridge(vsctl_ctx, ctx->argv[1], true);
    ovsrec_bridge_verify_ports(br->br_cfg ? br->br_cfg : br->parent->br_cfg);

    svec_init(&ports);
    LIST_FOR_EACH (port, ports_node, &br->ports) {
        if (strcmp(port->port_cfg->name, br->name)) {
            svec_add(&ports, port->port_cfg->name);
        }
    }
    output_sorted(&ports, &ctx->output);
    svec_destroy(&ports);
}

static void
add_port(struct ctl_context *ctx,
         const char *br_name, const char *port_name,
         bool may_exist,
#ifndef OPS_TEMP
         bool fake_iface,
#else
         bool fake_iface OVS_UNUSED,
#endif
         char *iface_names[], int n_ifaces,
         char *settings[], int n_settings)
{
    struct vsctl_context *vsctl_ctx = vsctl_context_cast(ctx);
    struct vsctl_port *vsctl_port;
    struct vsctl_bridge *bridge;
    struct ovsrec_interface **ifaces;
    struct ovsrec_port *port;
    size_t i;

    vsctl_context_populate_cache(ctx);
    if (may_exist) {
        struct vsctl_port *vsctl_port;

        vsctl_port = find_port(vsctl_ctx, port_name, false);
        if (vsctl_port) {
            struct svec want_names, have_names;

            svec_init(&want_names);
            for (i = 0; i < n_ifaces; i++) {
                svec_add(&want_names, iface_names[i]);
            }
            svec_sort(&want_names);

            svec_init(&have_names);
            for (i = 0; i < vsctl_port->port_cfg->n_interfaces; i++) {
                svec_add(&have_names,
                         vsctl_port->port_cfg->interfaces[i]->name);
            }
            svec_sort(&have_names);

#ifdef OPS
            if (vsctl_port->port_type == BR_PORT && 
                strcmp(vsctl_port->bridge->name, br_name)) {
                char *command = vsctl_context_to_string(ctx);
                ctl_fatal("\"%s\" but %s is actually attached to bridge %s",
                            command, port_name, vsctl_port->bridge->name);
            } else if (vsctl_port->port_type == VRF_PORT) {
                char *command = vsctl_context_to_string(ctx);
                ctl_fatal("\"%s\" but %s is actually attached to vrf %s",
                            command, port_name, vsctl_port->vrf->name);
            }
#else
            if (strcmp(vsctl_port->bridge->name, br_name)) {
                char *command = vsctl_context_to_string(ctx);
                ctl_fatal("\"%s\" but %s is actually attached to bridge %s",
                            command, port_name, vsctl_port->bridge->name);
            }
#endif

            if (!svec_equal(&want_names, &have_names)) {
                char *have_names_string = svec_join(&have_names, ", ", "");
                char *command = vsctl_context_to_string(ctx);

                ctl_fatal("\"%s\" but %s actually has interface(s) %s",
                            command, port_name, have_names_string);
            }

            svec_destroy(&want_names);
            svec_destroy(&have_names);

            return;
        }
    }
    check_conflicts(vsctl_ctx, port_name,
                    xasprintf("cannot create a port named %s", port_name));
    for (i = 0; i < n_ifaces; i++) {
        check_conflicts(vsctl_ctx, iface_names[i],
                        xasprintf("cannot create an interface named %s",
                                  iface_names[i]));
    }
    bridge = find_bridge(vsctl_ctx, br_name, true);

    ifaces = xmalloc(n_ifaces * sizeof *ifaces);
    for (i = 0; i < n_ifaces; i++) {
#ifdef OPS
        /* find the existing interface in the orphan_ifaces dictionary */
        if (vsctl_ctx->subsystems_exist) {
            struct vsctl_iface *iface;
            iface = find_orphan_iface(vsctl_ctx, iface_names[i], true);
            ifaces[i] = (struct ovsrec_interface *)iface->iface_cfg;
        } else
#endif
        {
            ifaces[i] = ovsrec_interface_insert(ctx->txn);
            ovsrec_interface_set_name(ifaces[i], iface_names[i]);
            post_db_reload_expect_iface(ifaces[i]);
        }
    }

    port = ovsrec_port_insert(ctx->txn);
    ovsrec_port_set_name(port, port_name);
    ovsrec_port_set_interfaces(port, ifaces, n_ifaces);
#ifndef OPS_TEMP
    ovsrec_port_set_bond_fake_iface(port, fake_iface);
#endif

    if (bridge->parent) {
        int64_t tag = bridge->vlan;
        ovsrec_port_set_tag(port, &tag, 1);
    }

    for (i = 0; i < n_settings; i++) {
        ctl_set_column("Port", &port->header_, settings[i],
                       ctx->symtab);
    }

    bridge_insert_port((bridge->parent ? bridge->parent->br_cfg
                        : bridge->br_cfg), port);

    vsctl_port = add_port_to_cache(vsctl_ctx, bridge, port);
    for (i = 0; i < n_ifaces; i++) {
#ifdef OPS
        if (vsctl_ctx->subsystems_exist) {
            move_orphan_iface_to_cache(vsctl_ctx, vsctl_port, ifaces[i]);
        } else
#endif
        {
            add_iface_to_cache(vsctl_ctx, vsctl_port, ifaces[i]);
        }
    }
    free(ifaces);
}

static void
cmd_add_port(struct ctl_context *ctx)
{
    bool may_exist = shash_find(&ctx->options, "--may-exist") != NULL;

    add_port(ctx, ctx->argv[1], ctx->argv[2], may_exist, false,
             &ctx->argv[2], 1, &ctx->argv[3], ctx->argc - 3);
}

static void
cmd_add_bond(struct ctl_context *ctx)
{
    bool may_exist = shash_find(&ctx->options, "--may-exist") != NULL;
    bool fake_iface = shash_find(&ctx->options, "--fake-iface");
    int n_ifaces;
    int i;

    n_ifaces = ctx->argc - 3;
    for (i = 3; i < ctx->argc; i++) {
        if (strchr(ctx->argv[i], '=')) {
            n_ifaces = i - 3;
            break;
        }
    }
    if (n_ifaces < 2) {
        ctl_fatal("add-bond requires at least 2 interfaces, but only "
                    "%d were specified", n_ifaces);
    }

    add_port(ctx, ctx->argv[1], ctx->argv[2], may_exist, fake_iface,
             &ctx->argv[3], n_ifaces,
             &ctx->argv[n_ifaces + 3], ctx->argc - 3 - n_ifaces);
}

static void
cmd_del_port(struct ctl_context *ctx)
{
    struct vsctl_context *vsctl_ctx = vsctl_context_cast(ctx);
    bool must_exist = !shash_find(&ctx->options, "--if-exists");
    bool with_iface = shash_find(&ctx->options, "--with-iface") != NULL;
    const char *target = ctx->argv[ctx->argc - 1];
    struct vsctl_port *port;

    vsctl_context_populate_cache(ctx);
    if (find_bridge(vsctl_ctx, target, false)) {
        if (must_exist) {
            ctl_fatal("cannot delete port %s because it is the local port "
                        "for bridge %s (deleting this port requires deleting "
                        "the entire bridge)", target, target);
        }
        port = NULL;
    } else if (!with_iface) {
        port = find_port(vsctl_ctx, target, must_exist);
    } else {
        struct vsctl_iface *iface;

        port = find_port(vsctl_ctx, target, false);
        if (!port) {
            iface = find_iface(vsctl_ctx, target, false);
            if (iface) {
                port = iface->port;
            }
        }
        if (must_exist && !port) {
            ctl_fatal("no port or interface named %s", target);
        }
    }

    if (port) {
        if (ctx->argc == 3) {
            struct vsctl_bridge *bridge;

            bridge = find_bridge(vsctl_ctx, ctx->argv[1], true);
            if (port->bridge != bridge) {
                if (port->bridge->parent == bridge) {
                    ctl_fatal("bridge %s does not have a port %s (although "
                                "its parent bridge %s does)",
                                ctx->argv[1], ctx->argv[2],
                                bridge->parent->name);
                } else {
                    ctl_fatal("bridge %s does not have a port %s",
                                ctx->argv[1], ctx->argv[2]);
                }
            }
        }

        del_port(vsctl_ctx, port);
    }
#ifdef OPS
    /* difference in behavior between OPS and standard OVS
     * (interface automatic deletion) means that we don't know if
     * the cache is correct or not after deleting a port
     */
    vsctl_context_invalidate_cache(ctx);
#endif
}

static void
cmd_port_to_br(struct ctl_context *ctx)
{
    struct vsctl_context *vsctl_ctx = vsctl_context_cast(ctx);
    struct vsctl_port *port;

    vsctl_context_populate_cache(ctx);

    port = find_port(vsctl_ctx, ctx->argv[1], true);
    ds_put_format(&ctx->output, "%s\n", port->bridge->name);
}

static void
cmd_br_to_vlan(struct ctl_context *ctx)
{
    struct vsctl_context *vsctl_ctx = vsctl_context_cast(ctx);
    struct vsctl_bridge *bridge;

    vsctl_context_populate_cache(ctx);

    bridge = find_bridge(vsctl_ctx, ctx->argv[1], true);
    ds_put_format(&ctx->output, "%d\n", bridge->vlan);
}

static void
cmd_br_to_parent(struct ctl_context *ctx)
{
    struct vsctl_context *vsctl_ctx = vsctl_context_cast(ctx);
    struct vsctl_bridge *bridge;

    vsctl_context_populate_cache(ctx);

    bridge = find_bridge(vsctl_ctx, ctx->argv[1], true);
    if (bridge->parent) {
        bridge = bridge->parent;
    }
    ds_put_format(&ctx->output, "%s\n", bridge->name);
}

static void
cmd_list_ifaces(struct ctl_context *ctx)
{
    struct vsctl_context *vsctl_ctx = vsctl_context_cast(ctx);
    struct vsctl_bridge *br;
    struct vsctl_port *port;
    struct svec ifaces;

    vsctl_context_populate_cache(ctx);

    br = find_bridge(vsctl_ctx, ctx->argv[1], true);
    verify_ports(vsctl_ctx);

    svec_init(&ifaces);
    LIST_FOR_EACH (port, ports_node, &br->ports) {
        struct vsctl_iface *iface;

        LIST_FOR_EACH (iface, ifaces_node, &port->ifaces) {
            if (strcmp(iface->iface_cfg->name, br->name)) {
                svec_add(&ifaces, iface->iface_cfg->name);
            }
        }
    }
    output_sorted(&ifaces, &ctx->output);
    svec_destroy(&ifaces);
}

static void
cmd_iface_to_br(struct ctl_context *ctx)
{
    struct vsctl_context *vsctl_ctx = vsctl_context_cast(ctx);
    struct vsctl_iface *iface;

    vsctl_context_populate_cache(ctx);

    iface = find_iface(vsctl_ctx, ctx->argv[1], true);
    ds_put_format(&ctx->output, "%s\n", iface->port->bridge->name);
}

#ifndef OPS_TEMP
static void
verify_controllers(struct ovsrec_bridge *bridge)
{
    size_t i;

    ovsrec_bridge_verify_controller(bridge);
    for (i = 0; i < bridge->n_controller; i++) {
        ovsrec_controller_verify_target(bridge->controller[i]);
    }
}

static void
pre_controller(struct ctl_context *ctx)
{
    pre_get_info(ctx);

    ovsdb_idl_add_column(ctx->idl, &ovsrec_controller_col_target);
}

static void
cmd_get_controller(struct ctl_context *ctx)
{
    struct vsctl_context *vsctl_ctx = vsctl_context_cast(ctx);
    struct vsctl_bridge *br;
    struct svec targets;
    size_t i;

    vsctl_context_populate_cache(ctx);

    br = find_bridge(vsctl_ctx, ctx->argv[1], true);
    if (br->parent) {
        br = br->parent;
    }
    verify_controllers(br->br_cfg);

    /* Print the targets in sorted order for reproducibility. */
    svec_init(&targets);
    for (i = 0; i < br->br_cfg->n_controller; i++) {
        svec_add(&targets, br->br_cfg->controller[i]->target);
    }

    svec_sort(&targets);
    for (i = 0; i < targets.n; i++) {
        ds_put_format(&ctx->output, "%s\n", targets.names[i]);
    }
    svec_destroy(&targets);
}

static void
delete_controllers(struct ovsrec_controller **controllers,
                   size_t n_controllers)
{
    size_t i;

    for (i = 0; i < n_controllers; i++) {
        ovsrec_controller_delete(controllers[i]);
    }
}

static void
cmd_del_controller(struct ctl_context *ctx)
{
    struct vsctl_context *vsctl_ctx = vsctl_context_cast(ctx);
    struct ovsrec_bridge *br;

    vsctl_context_populate_cache(ctx);

    br = find_real_bridge(vsctl_ctx, ctx->argv[1], true)->br_cfg;
    verify_controllers(br);

    if (br->controller) {
        delete_controllers(br->controller, br->n_controller);
        ovsrec_bridge_set_controller(br, NULL, 0);
    }
}

static struct ovsrec_controller **
insert_controllers(struct ovsdb_idl_txn *txn, char *targets[], size_t n)
{
    struct ovsrec_controller **controllers;
    size_t i;

    controllers = xmalloc(n * sizeof *controllers);
    for (i = 0; i < n; i++) {
        if (vconn_verify_name(targets[i]) && pvconn_verify_name(targets[i])) {
            VLOG_WARN("target type \"%s\" is possibly erroneous", targets[i]);
        }
        controllers[i] = ovsrec_controller_insert(txn);
        ovsrec_controller_set_target(controllers[i], targets[i]);
    }

    return controllers;
}

static void
cmd_set_controller(struct ctl_context *ctx)
{
    struct vsctl_context *vsctl_ctx = vsctl_context_cast(ctx);
    struct ovsrec_controller **controllers;
    struct ovsrec_bridge *br;
    size_t n;

    vsctl_context_populate_cache(ctx);

    br = find_real_bridge(vsctl_ctx, ctx->argv[1], true)->br_cfg;
    verify_controllers(br);

    delete_controllers(br->controller, br->n_controller);

    n = ctx->argc - 2;
    controllers = insert_controllers(ctx->txn, &ctx->argv[2], n);
    ovsrec_bridge_set_controller(br, controllers, n);
    free(controllers);
}

static void
cmd_get_fail_mode(struct ctl_context *ctx)
{
    struct vsctl_context *vsctl_ctx = vsctl_context_cast(ctx);
    struct vsctl_bridge *br;
    const char *fail_mode;

    vsctl_context_populate_cache(ctx);
    br = find_bridge(vsctl_ctx, ctx->argv[1], true);

    if (br->parent) {
        br = br->parent;
    }
    ovsrec_bridge_verify_fail_mode(br->br_cfg);

    fail_mode = br->br_cfg->fail_mode;
    if (fail_mode && strlen(fail_mode)) {
        ds_put_format(&ctx->output, "%s\n", fail_mode);
    }
}

static void
cmd_del_fail_mode(struct ctl_context *ctx)
{
    struct vsctl_context *vsctl_ctx = vsctl_context_cast(ctx);
    struct vsctl_bridge *br;

    vsctl_context_populate_cache(ctx);

    br = find_real_bridge(vsctl_ctx, ctx->argv[1], true);

    ovsrec_bridge_set_fail_mode(br->br_cfg, NULL);
}

static void
cmd_set_fail_mode(struct ctl_context *ctx)
{
    struct vsctl_context *vsctl_ctx = vsctl_context_cast(ctx);
    struct vsctl_bridge *br;
    const char *fail_mode = ctx->argv[2];

    vsctl_context_populate_cache(ctx);

    br = find_real_bridge(vsctl_ctx, ctx->argv[1], true);

    if (strcmp(fail_mode, "standalone") && strcmp(fail_mode, "secure")) {
        ctl_fatal("fail-mode must be \"standalone\" or \"secure\"");
    }

    ovsrec_bridge_set_fail_mode(br->br_cfg, fail_mode);
}
#endif
static void
verify_managers(const struct ovsrec_open_vswitch *ovs)
{
    size_t i;

    ovsrec_open_vswitch_verify_manager_options(ovs);

    for (i = 0; i < ovs->n_manager_options; ++i) {
        const struct ovsrec_manager *mgr = ovs->manager_options[i];

        ovsrec_manager_verify_target(mgr);
    }
}

static void
pre_manager(struct ctl_context *ctx)
{
    ovsdb_idl_add_column(ctx->idl, &ovsrec_open_vswitch_col_manager_options);
    ovsdb_idl_add_column(ctx->idl, &ovsrec_manager_col_target);
}

static void
cmd_get_manager(struct ctl_context *ctx)
{
    struct vsctl_context *vsctl_ctx = vsctl_context_cast(ctx);
    const struct ovsrec_open_vswitch *ovs = vsctl_ctx->ovs;
    struct svec targets;
    size_t i;

    verify_managers(ovs);

    /* Print the targets in sorted order for reproducibility. */
    svec_init(&targets);

    for (i = 0; i < ovs->n_manager_options; i++) {
        svec_add(&targets, ovs->manager_options[i]->target);
    }

    svec_sort_unique(&targets);
    for (i = 0; i < targets.n; i++) {
        ds_put_format(&ctx->output, "%s\n", targets.names[i]);
    }
    svec_destroy(&targets);
}

static void
delete_managers(const struct ovsrec_open_vswitch *ovs)
{
    size_t i;

    /* Delete Manager rows pointed to by 'manager_options' column. */
    for (i = 0; i < ovs->n_manager_options; i++) {
        ovsrec_manager_delete(ovs->manager_options[i]);
    }

    /* Delete 'Manager' row refs in 'manager_options' column. */
    ovsrec_open_vswitch_set_manager_options(ovs, NULL, 0);
}

static void
cmd_del_manager(struct ctl_context *ctx)
{
    struct vsctl_context *vsctl_ctx = vsctl_context_cast(ctx);
    const struct ovsrec_open_vswitch *ovs = vsctl_ctx->ovs;

    verify_managers(ovs);
    delete_managers(ovs);
}

static void
insert_managers(struct vsctl_context *vsctl_ctx, char *targets[], size_t n)
{
    struct ovsrec_manager **managers;
    size_t i;

    /* Insert each manager in a new row in Manager table. */
    managers = xmalloc(n * sizeof *managers);
    for (i = 0; i < n; i++) {
        if (stream_verify_name(targets[i]) && pstream_verify_name(targets[i])) {
            VLOG_WARN("target type \"%s\" is possibly erroneous", targets[i]);
        }
        managers[i] = ovsrec_manager_insert(vsctl_ctx->base.txn);
        ovsrec_manager_set_target(managers[i], targets[i]);
    }

    /* Store uuids of new Manager rows in 'manager_options' column. */
    ovsrec_open_vswitch_set_manager_options(vsctl_ctx->ovs, managers, n);
    free(managers);
}

static void
cmd_set_manager(struct ctl_context *ctx)
{
    struct vsctl_context *vsctl_ctx = vsctl_context_cast(ctx);
    const size_t n = ctx->argc - 1;

    verify_managers(vsctl_ctx->ovs);
    delete_managers(vsctl_ctx->ovs);
    insert_managers(vsctl_ctx, &ctx->argv[1], n);
}

static void
pre_cmd_get_ssl(struct ctl_context *ctx)
{
    ovsdb_idl_add_column(ctx->idl, &ovsrec_open_vswitch_col_ssl);

    ovsdb_idl_add_column(ctx->idl, &ovsrec_ssl_col_private_key);
    ovsdb_idl_add_column(ctx->idl, &ovsrec_ssl_col_certificate);
    ovsdb_idl_add_column(ctx->idl, &ovsrec_ssl_col_ca_cert);
    ovsdb_idl_add_column(ctx->idl, &ovsrec_ssl_col_bootstrap_ca_cert);
}

static void
cmd_get_ssl(struct ctl_context *ctx)
{
    struct vsctl_context *vsctl_ctx = vsctl_context_cast(ctx);
    struct ovsrec_ssl *ssl = vsctl_ctx->ovs->ssl;

    ovsrec_open_vswitch_verify_ssl(vsctl_ctx->ovs);
    if (ssl) {
        ovsrec_ssl_verify_private_key(ssl);
        ovsrec_ssl_verify_certificate(ssl);
        ovsrec_ssl_verify_ca_cert(ssl);
        ovsrec_ssl_verify_bootstrap_ca_cert(ssl);

        ds_put_format(&ctx->output, "Private key: %s\n", ssl->private_key);
        ds_put_format(&ctx->output, "Certificate: %s\n", ssl->certificate);
        ds_put_format(&ctx->output, "CA Certificate: %s\n", ssl->ca_cert);
        ds_put_format(&ctx->output, "Bootstrap: %s\n",
                ssl->bootstrap_ca_cert ? "true" : "false");
    }
}

static void
pre_cmd_del_ssl(struct ctl_context *ctx)
{
    ovsdb_idl_add_column(ctx->idl, &ovsrec_open_vswitch_col_ssl);
}

static void
cmd_del_ssl(struct ctl_context *ctx)
{
    struct vsctl_context *vsctl_ctx = vsctl_context_cast(ctx);
    struct ovsrec_ssl *ssl = vsctl_ctx->ovs->ssl;

    if (ssl) {
        ovsrec_open_vswitch_verify_ssl(vsctl_ctx->ovs);
        ovsrec_ssl_delete(ssl);
        ovsrec_open_vswitch_set_ssl(vsctl_ctx->ovs, NULL);
    }
}

static void
pre_cmd_set_ssl(struct ctl_context *ctx)
{
    ovsdb_idl_add_column(ctx->idl, &ovsrec_open_vswitch_col_ssl);
}

static void
cmd_set_ssl(struct ctl_context *ctx)
{
    struct vsctl_context *vsctl_ctx = vsctl_context_cast(ctx);
    bool bootstrap = shash_find(&ctx->options, "--bootstrap");
    struct ovsrec_ssl *ssl = vsctl_ctx->ovs->ssl;

    ovsrec_open_vswitch_verify_ssl(vsctl_ctx->ovs);
    if (ssl) {
        ovsrec_ssl_delete(ssl);
    }
    ssl = ovsrec_ssl_insert(ctx->txn);

    ovsrec_ssl_set_private_key(ssl, ctx->argv[1]);
    ovsrec_ssl_set_certificate(ssl, ctx->argv[2]);
    ovsrec_ssl_set_ca_cert(ssl, ctx->argv[3]);

    ovsrec_ssl_set_bootstrap_ca_cert(ssl, bootstrap);

    ovsrec_open_vswitch_set_ssl(vsctl_ctx->ovs, ssl);
}

#ifndef OPS_TEMP
static void
autoattach_insert_mapping(struct ovsrec_autoattach *aa,
                          int64_t isid,
                          int64_t vlan)
{
    int64_t *key_mappings, *value_mappings;
    size_t i;

    key_mappings = xmalloc(sizeof *aa->key_mappings * (aa->n_mappings + 1));
    value_mappings = xmalloc(sizeof *aa->value_mappings * (aa->n_mappings + 1));

    for (i = 0; i < aa->n_mappings; i++) {
        key_mappings[i] = aa->key_mappings[i];
        value_mappings[i] = aa->value_mappings[i];
    }
    key_mappings[aa->n_mappings] = isid;
    value_mappings[aa->n_mappings] = vlan;

    ovsrec_autoattach_set_mappings(aa, key_mappings, value_mappings,
                                   aa->n_mappings + 1);

    free(key_mappings);
    free(value_mappings);
}

static void
cmd_add_aa_mapping(struct ctl_context *ctx)
{
    struct vsctl_context *vsctl_ctx = vsctl_context_cast(ctx);
    struct vsctl_bridge *br;
    int64_t isid, vlan;
    char *nptr = NULL;

    isid = strtoull(ctx->argv[2], &nptr, 10);
    if (nptr == ctx->argv[2] || nptr == NULL) {
        ctl_fatal("Invalid argument %s", ctx->argv[2]);
        return;
    }

    vlan = strtoull(ctx->argv[3], &nptr, 10);
    if (nptr == ctx->argv[3] || nptr == NULL) {
        ctl_fatal("Invalid argument %s", ctx->argv[3]);
        return;
    }

    vsctl_context_populate_cache(ctx);

    br = find_bridge(vsctl_ctx, ctx->argv[1], true);
    if (br->parent) {
        br = br->parent;
    }

    if (br->br_cfg) {
        if (!br->br_cfg->auto_attach) {
            struct ovsrec_autoattach *aa = ovsrec_autoattach_insert(ctx->txn);
            ovsrec_bridge_set_auto_attach(br->br_cfg, aa);
        }
        autoattach_insert_mapping(br->br_cfg->auto_attach, isid, vlan);
    }
}

static void
del_aa_mapping(struct ovsrec_autoattach *aa,
               int64_t isid,
               int64_t vlan)
{
    int64_t *key_mappings, *value_mappings;
    size_t i, n;

    key_mappings = xmalloc(sizeof *aa->key_mappings * (aa->n_mappings));
    value_mappings = xmalloc(sizeof *value_mappings * (aa->n_mappings));

    for (i = n = 0; i < aa->n_mappings; i++) {
        if (aa->key_mappings[i] != isid && aa->value_mappings[i] != vlan) {
            key_mappings[n] = aa->key_mappings[i];
            value_mappings[n++] = aa->value_mappings[i];
        }
    }

    ovsrec_autoattach_set_mappings(aa, key_mappings, value_mappings, n);

    free(key_mappings);
    free(value_mappings);
}

static void
cmd_del_aa_mapping(struct ctl_context *ctx)
{
    struct vsctl_context *vsctl_ctx = vsctl_context_cast(ctx);
    struct vsctl_bridge *br;
    int64_t isid, vlan;
    char *nptr = NULL;

    isid = strtoull(ctx->argv[2], &nptr, 10);
    if (nptr == ctx->argv[2] || nptr == NULL) {
        ctl_fatal("Invalid argument %s", ctx->argv[2]);
        return;
    }

    vlan = strtoull(ctx->argv[3], &nptr, 10);
    if (nptr == ctx->argv[3] || nptr == NULL) {
        ctl_fatal("Invalid argument %s", ctx->argv[3]);
        return;
    }

    vsctl_context_populate_cache(ctx);

    br = find_bridge(vsctl_ctx, ctx->argv[1], true);
    if (br->parent) {
        br = br->parent;
    }

    if (br->br_cfg && br->br_cfg->auto_attach &&
        br->br_cfg->auto_attach->key_mappings &&
        br->br_cfg->auto_attach->value_mappings) {
        size_t i;

        for (i = 0; i < br->br_cfg->auto_attach->n_mappings; i++) {
            if (br->br_cfg->auto_attach->key_mappings[i] == isid &&
                br->br_cfg->auto_attach->value_mappings[i] == vlan) {
                del_aa_mapping(br->br_cfg->auto_attach, isid, vlan);
                break;
            }
        }
    }
}

static void
pre_aa_mapping(struct ctl_context *ctx)
{
    pre_get_info(ctx);

    ovsdb_idl_add_column(ctx->idl, &ovsrec_bridge_col_auto_attach);
    ovsdb_idl_add_column(ctx->idl, &ovsrec_autoattach_col_mappings);
}

static void
verify_auto_attach(struct ovsrec_bridge *bridge)
{
    if (bridge) {
        ovsrec_bridge_verify_auto_attach(bridge);

        if (bridge->auto_attach) {
            ovsrec_autoattach_verify_mappings(bridge->auto_attach);
        }
    }
}

static void
cmd_get_aa_mapping(struct ctl_context *ctx)
{
    struct vsctl_context *vsctl_ctx = vsctl_context_cast(ctx);
    struct vsctl_bridge *br;

    vsctl_context_populate_cache(ctx);

    br = find_bridge(vsctl_ctx, ctx->argv[1], true);
    if (br->parent) {
        br = br->parent;
    }

    verify_auto_attach(br->br_cfg);

    if (br->br_cfg && br->br_cfg->auto_attach &&
        br->br_cfg->auto_attach->key_mappings &&
        br->br_cfg->auto_attach->value_mappings) {
        size_t i;

        for (i = 0; i < br->br_cfg->auto_attach->n_mappings; i++) {
            ds_put_format(&ctx->output, "%"PRId64" %"PRId64"\n",
                          br->br_cfg->auto_attach->key_mappings[i],
                          br->br_cfg->auto_attach->value_mappings[i]);
        }
    }
}
#endif


static const struct ctl_table_class tables[] = {
    {&ovsrec_table_bridge,
     {{&ovsrec_table_bridge, &ovsrec_bridge_col_name, NULL},
#ifndef OPS_TEMP
      {&ovsrec_table_flow_sample_collector_set, NULL,
       &ovsrec_flow_sample_collector_set_col_bridge}
#endif
    }},

#ifndef OPS_TEMP
    {&ovsrec_table_controller,
     {{&ovsrec_table_bridge,
       &ovsrec_bridge_col_name,
       &ovsrec_bridge_col_controller}}},
#endif
#ifdef OPS
    {&ovsrec_table_subsystem,
     {{&ovsrec_table_subsystem, &ovsrec_subsystem_col_name, NULL},
     {NULL, NULL, NULL}}},
    {&ovsrec_table_led,
     {{&ovsrec_table_led, &ovsrec_led_col_id, NULL},
     {NULL, NULL, NULL}}},
    {&ovsrec_table_fan,
     {{&ovsrec_table_fan, &ovsrec_fan_col_name, NULL},
     {NULL, NULL, NULL}}},
    {&ovsrec_table_temp_sensor,
     {{&ovsrec_table_temp_sensor, &ovsrec_temp_sensor_col_name, NULL},
     {NULL, NULL, NULL}}},
    {&ovsrec_table_power_supply,
     {{&ovsrec_table_power_supply, &ovsrec_power_supply_col_name, NULL},
     {NULL, NULL, NULL}}},
    {&ovsrec_table_daemon,
     {{&ovsrec_table_daemon, &ovsrec_daemon_col_name, NULL},
     {NULL, NULL, NULL}}},
    {&ovsrec_table_vlan,
     {{&ovsrec_table_vlan, &ovsrec_vlan_col_name, NULL},
     {NULL, NULL, NULL}}},
    {&ovsrec_table_vrf,
     {{&ovsrec_table_vrf, &ovsrec_vrf_col_name, NULL},
     {NULL, NULL, NULL}}},
    {&ovsrec_table_ospf_router,
     {{&ovsrec_table_ospf_router, &ovsrec_ospf_router_col_areas, NULL},
     {NULL, NULL, NULL}}},
    {&ovsrec_table_ospf_area,
     {{&ovsrec_table_ospf_area, &ovsrec_ospf_area_col_abr_summary_lsas, NULL},
     {NULL, NULL, NULL}}},
    {&ovsrec_table_ospf_vlink,
     {{&ovsrec_table_ospf_vlink, &ovsrec_ospf_vlink_col_area_id, NULL},
     {NULL, NULL, NULL}}},
    {&ovsrec_table_ospf_interface,
     {{&ovsrec_table_ospf_interface, &ovsrec_ospf_interface_col_ifsm_state, NULL},
     {NULL, NULL, NULL}}},
    {&ovsrec_table_ospf_nbma_neighbor,
     {{&ovsrec_table_ospf_nbma_neighbor,
     &ovsrec_ospf_nbma_neighbor_col_status, NULL},
     {NULL, NULL, NULL}}},
    {&ovsrec_table_ospf_lsa,
     {{&ovsrec_table_ospf_lsa, &ovsrec_ospf_lsa_col_area_id, NULL},
     {NULL, NULL, NULL}}},
    {&ovsrec_table_ospf_neighbor,
     {{&ovsrec_table_ospf_neighbor, &ovsrec_ospf_neighbor_col_bdr, NULL},
     {NULL, NULL, NULL}}},
     {&ovsrec_table_ospf_summary_address,
     {{&ovsrec_table_ospf_summary_address,
       &ovsrec_ospf_summary_address_col_other_config, NULL},
     {NULL, NULL, NULL}}},
     {&ovsrec_table_ospf_route,
     {{&ovsrec_table_ospf_route,
       &ovsrec_ospf_route_col_paths, NULL},
     {NULL, NULL, NULL}}},
     {&ovsrec_table_route,
     {{&ovsrec_table_route,
       &ovsrec_route_col_from, NULL},
     {NULL, NULL, NULL}}},
     {&ovsrec_table_nexthop,
     {{&ovsrec_table_nexthop,
       &ovsrec_nexthop_col_external_ids, NULL},
     {NULL, NULL, NULL}}},
#endif
    {&ovsrec_table_interface,
     {{&ovsrec_table_interface, &ovsrec_interface_col_name, NULL},
      {NULL, NULL, NULL}}},

#ifndef OPS_TEMP
    {&ovsrec_table_mirror,
     {{&ovsrec_table_mirror, &ovsrec_mirror_col_name, NULL},
      {NULL, NULL, NULL}}},
#endif
    {&ovsrec_table_manager,
     {{&ovsrec_table_manager, &ovsrec_manager_col_target, NULL},
      {NULL, NULL, NULL}}},

#ifndef OPS_TEMP
    {&ovsrec_table_netflow,
     {{&ovsrec_table_bridge,
       &ovsrec_bridge_col_name,
       &ovsrec_bridge_col_netflow},
      {NULL, NULL, NULL}}},
#endif
    {&ovsrec_table_open_vswitch,
     {{&ovsrec_table_open_vswitch, NULL, NULL},
      {NULL, NULL, NULL}}},

    {&ovsrec_table_port,
     {{&ovsrec_table_port, &ovsrec_port_col_name, NULL},
      {NULL, NULL, NULL}}},

#ifndef OPS_TEMP
    {&ovsrec_table_qos,
     {{&ovsrec_table_port, &ovsrec_port_col_name, &ovsrec_port_col_qos},
      {NULL, NULL, NULL}}},

    {&ovsrec_table_queue,
     {{NULL, NULL, NULL},
      {NULL, NULL, NULL}}},
#endif
    {&ovsrec_table_ssl,
     {{&ovsrec_table_open_vswitch, NULL, &ovsrec_open_vswitch_col_ssl}}},

#ifndef OPS_TEMP
    {&ovsrec_table_sflow,
     {{&ovsrec_table_bridge,
       &ovsrec_bridge_col_name,
       &ovsrec_bridge_col_sflow},
      {NULL, NULL, NULL}}},

    {&ovsrec_table_flow_table,
     {{&ovsrec_table_flow_table, &ovsrec_flow_table_col_name, NULL},
      {NULL, NULL, NULL}}},

    {&ovsrec_table_ipfix,
     {{&ovsrec_table_bridge,
       &ovsrec_bridge_col_name,
       &ovsrec_bridge_col_ipfix},
      {&ovsrec_table_flow_sample_collector_set, NULL,
       &ovsrec_flow_sample_collector_set_col_ipfix}}},

    {&ovsrec_table_autoattach,
     {{&ovsrec_table_bridge,
       &ovsrec_bridge_col_name,
       &ovsrec_bridge_col_auto_attach},
      {NULL, NULL, NULL}}},

    {&ovsrec_table_flow_sample_collector_set,
     {{NULL, NULL, NULL},
      {NULL, NULL, NULL}}},
#endif
    {NULL, {{NULL, NULL, NULL}, {NULL, NULL, NULL}}}
};

static void
post_db_reload_check_init(void)
{
    n_neoteric_ifaces = 0;
}

static void
post_db_reload_expect_iface(const struct ovsrec_interface *iface)
{
    if (n_neoteric_ifaces >= allocated_neoteric_ifaces) {
        neoteric_ifaces = x2nrealloc(neoteric_ifaces,
                                     &allocated_neoteric_ifaces,
                                     sizeof *neoteric_ifaces);
    }
    neoteric_ifaces[n_neoteric_ifaces++] = iface->header_.uuid;
}

#ifndef OPS_TEMP
static void
post_db_reload_do_checks(const struct vsctl_context *vsctl_ctx)
{
    struct ds dead_ifaces = DS_EMPTY_INITIALIZER;
    size_t i;

    for (i = 0; i < n_neoteric_ifaces; i++) {
        const struct uuid *uuid;

        uuid = ovsdb_idl_txn_get_insert_uuid(vsctl_ctx->base.txn,
                                             &neoteric_ifaces[i]);
        if (uuid) {
            const struct ovsrec_interface *iface;

            iface = ovsrec_interface_get_for_uuid(vsctl_ctx->base.idl, uuid);
            if (iface && (!iface->ofport || *iface->ofport == -1)) {
                ds_put_format(&dead_ifaces, "'%s', ", iface->name);
            }
        }
    }

    if (dead_ifaces.length) {
        dead_ifaces.length -= 2; /* Strip off trailing comma and space. */
        ovs_error(0, "Error detected while setting up %s.  See ovs-vswitchd "
                  "log for details.", ds_cstr(&dead_ifaces));
    }

    ds_destroy(&dead_ifaces);
}
#endif


static void
vsctl_context_init_command(struct vsctl_context *vsctl_ctx,
                           struct ctl_command *command)
{
    ctl_context_init_command(&vsctl_ctx->base, command);
    vsctl_ctx->verified_ports = false;
}

static void
vsctl_context_init(struct vsctl_context *vsctl_ctx,
                   struct ctl_command *command, struct ovsdb_idl *idl,
                   struct ovsdb_idl_txn *txn,
                   const struct ovsrec_open_vswitch *ovs,
                   struct ovsdb_symbol_table *symtab)
{
    ctl_context_init(&vsctl_ctx->base, command, idl, txn, symtab,
                     vsctl_context_invalidate_cache);
    if (command) {
        vsctl_ctx->verified_ports = false;
    }
    vsctl_ctx->ovs = ovs;
    vsctl_ctx->cache_valid = false;
}

static void
vsctl_context_done_command(struct vsctl_context *vsctl_ctx,
                           struct ctl_command *command)
{
    ctl_context_done_command(&vsctl_ctx->base, command);
}

static void
vsctl_context_done(struct vsctl_context *vsctl_ctx,
                   struct ctl_command *command)
{
    ctl_context_done(&vsctl_ctx->base, command);
}

static void
run_prerequisites(struct ctl_command *commands, size_t n_commands,
                  struct ovsdb_idl *idl)
{
    struct ctl_command *c;

    ovsdb_idl_add_table(idl, &ovsrec_table_open_vswitch);
    if (wait_for_reload) {
        ovsdb_idl_add_column(idl, &ovsrec_open_vswitch_col_cur_cfg);
    }
    for (c = commands; c < &commands[n_commands]; c++) {
        if (c->syntax->prerequisites) {
            struct vsctl_context vsctl_ctx;

            ds_init(&c->output);
            c->table = NULL;

            vsctl_context_init(&vsctl_ctx, c, idl, NULL, NULL, NULL);
            (c->syntax->prerequisites)(&vsctl_ctx.base);
            vsctl_context_done(&vsctl_ctx, c);

            ovs_assert(!c->output.string);
            ovs_assert(!c->table);
        }
    }
}

static void
do_vsctl(const char *args, struct ctl_command *commands, size_t n_commands,
         struct ovsdb_idl *idl)
{
    struct ovsdb_idl_txn *txn;
    const struct ovsrec_open_vswitch *ovs;
    enum ovsdb_idl_txn_status status;
    struct ovsdb_symbol_table *symtab;
    struct vsctl_context vsctl_ctx;
    struct ctl_command *c;
    struct shash_node *node;
    int64_t next_cfg = 0;
    char *error = NULL;

    txn = the_idl_txn = ovsdb_idl_txn_create(idl);
    if (dry_run) {
        ovsdb_idl_txn_set_dry_run(txn);
    }

    ovsdb_idl_txn_add_comment(txn, "ovs-vsctl: %s", args);

    ovs = ovsrec_open_vswitch_first(idl);
    if (!ovs) {
        /* XXX add verification that table is empty */
        ovs = ovsrec_open_vswitch_insert(txn);
    }

    if (wait_for_reload) {
        ovsdb_idl_txn_increment(txn, &ovs->header_,
                                &ovsrec_open_vswitch_col_next_cfg);
    }

    post_db_reload_check_init();
    symtab = ovsdb_symbol_table_create();
    for (c = commands; c < &commands[n_commands]; c++) {
        ds_init(&c->output);
        c->table = NULL;
    }
    vsctl_context_init(&vsctl_ctx, NULL, idl, txn, ovs, symtab);
    for (c = commands; c < &commands[n_commands]; c++) {
        vsctl_context_init_command(&vsctl_ctx, c);
        if (c->syntax->run) {
            (c->syntax->run)(&vsctl_ctx.base);
        }
        vsctl_context_done_command(&vsctl_ctx, c);

        if (vsctl_ctx.base.try_again) {
            vsctl_context_done(&vsctl_ctx, NULL);
            goto try_again;
        }
    }
    vsctl_context_done(&vsctl_ctx, NULL);

    SHASH_FOR_EACH (node, &symtab->sh) {
        struct ovsdb_symbol *symbol = node->data;
        if (!symbol->created) {
            ctl_fatal("row id \"%s\" is referenced but never created (e.g. "
                        "with \"-- --id=%s create ...\")",
                        node->name, node->name);
        }
        if (!symbol->strong_ref) {
            if (!symbol->weak_ref) {
                VLOG_WARN("row id \"%s\" was created but no reference to it "
                          "was inserted, so it will not actually appear in "
                          "the database", node->name);
            } else {
                VLOG_WARN("row id \"%s\" was created but only a weak "
                          "reference to it was inserted, so it will not "
                          "actually appear in the database", node->name);
            }
        }
    }

    status = ovsdb_idl_txn_commit_block(txn);
    if (wait_for_reload && status == TXN_SUCCESS) {
        next_cfg = ovsdb_idl_txn_get_increment_new_value(txn);
    }
    if (status == TXN_UNCHANGED || status == TXN_SUCCESS) {
        for (c = commands; c < &commands[n_commands]; c++) {
            if (c->syntax->postprocess) {
                vsctl_context_init(&vsctl_ctx, c, idl, txn, ovs, symtab);
                (c->syntax->postprocess)(&vsctl_ctx.base);
                vsctl_context_done(&vsctl_ctx, c);
            }
        }
    }
    error = xstrdup(ovsdb_idl_txn_get_error(txn));

    switch (status) {
    case TXN_UNCOMMITTED:
    case TXN_INCOMPLETE:
        OVS_NOT_REACHED();

    case TXN_ABORTED:
        /* Should not happen--we never call ovsdb_idl_txn_abort(). */
        ctl_fatal("transaction aborted");

    case TXN_UNCHANGED:
    case TXN_SUCCESS:
        break;

    case TXN_TRY_AGAIN:
        goto try_again;

    case TXN_ERROR:
        ctl_fatal("transaction error: %s", error);

    case TXN_NOT_LOCKED:
        /* Should not happen--we never call ovsdb_idl_set_lock(). */
        ctl_fatal("database not locked");

    default:
        OVS_NOT_REACHED();
    }
    free(error);

    ovsdb_symbol_table_destroy(symtab);

    for (c = commands; c < &commands[n_commands]; c++) {
        struct ds *ds = &c->output;

        if (c->table) {
            table_print(c->table, &table_style);
        } else if (oneline) {
            size_t j;

            ds_chomp(ds, '\n');
            for (j = 0; j < ds->length; j++) {
                int ch = ds->string[j];
                switch (ch) {
                case '\n':
                    fputs("\\n", stdout);
                    break;

                case '\\':
                    fputs("\\\\", stdout);
                    break;

                default:
                    putchar(ch);
                }
            }
            putchar('\n');
        } else {
            fputs(ds_cstr(ds), stdout);
        }
        ds_destroy(&c->output);
        table_destroy(c->table);
        free(c->table);

        shash_destroy_free_data(&c->options);
    }
    free(commands);

    if (wait_for_reload && status != TXN_UNCHANGED) {
        /* Even, if --retry flag was not specified, ovs-vsctl still
         * has to retry to establish OVSDB connection, if wait_for_reload
         * was set.  Otherwise, ovs-vsctl would end up waiting forever
         * until cur_cfg would be updated. */
        ovsdb_idl_enable_reconnect(idl);
        for (;;) {
            ovsdb_idl_run(idl);
            OVSREC_OPEN_VSWITCH_FOR_EACH (ovs, idl) {
                if (ovs->cur_cfg >= next_cfg) {
#ifndef OPS_TEMP
                    post_db_reload_do_checks(&vsctl_ctx);
#endif
                    goto done;
                }
            }
            ovsdb_idl_wait(idl);
            poll_block();
        }
    done: ;
    }
    ovsdb_idl_txn_destroy(txn);
    ovsdb_idl_destroy(idl);

    exit(EXIT_SUCCESS);

try_again:
    /* Our transaction needs to be rerun, or a prerequisite was not met.  Free
     * resources and return so that the caller can try again. */
    if (txn) {
        ovsdb_idl_txn_abort(txn);
        ovsdb_idl_txn_destroy(txn);
        the_idl_txn = NULL;
    }
    ovsdb_symbol_table_destroy(symtab);
    for (c = commands; c < &commands[n_commands]; c++) {
        ds_destroy(&c->output);
        table_destroy(c->table);
        free(c->table);
    }
    free(error);
}

/* Frees the current transaction and the underlying IDL and then calls
 * exit(status).
 *
 * Freeing the transaction and the IDL is not strictly necessary, but it makes
 * for a clean memory leak report from valgrind in the normal case.  That makes
 * it easier to notice real memory leaks. */
static void
vsctl_exit(int status)
{
    if (the_idl_txn) {
        ovsdb_idl_txn_abort(the_idl_txn);
        ovsdb_idl_txn_destroy(the_idl_txn);
    }
    ovsdb_idl_destroy(the_idl);
    exit(status);
}

/*
 * Developers who add new commands to the 'struct ctl_command_syntax' must
 * define the 'arguments' member of the struct.  The following keywords are
 * available for composing the argument format:
 *
 *    TABLE     RECORD       BRIDGE       PARENT         PORT
 *    KEY       VALUE        ARG          KEY=VALUE      ?KEY=VALUE
 *    IFACE     SYSIFACE     COLUMN       COLUMN?:KEY    COLUMN?:KEY=VALUE
 *    MODE      CA-CERT      CERTIFICATE  PRIVATE-KEY
 *    TARGET    NEW-* (e.g. NEW-PORT)
 *
 * For argument types not listed above, just uses 'ARG' as place holder.
 *
 * Encloses the keyword with '[]' if it is optional.  Appends '...' to
 * keyword or enclosed keyword to indicate that the argument can be specified
 * multiple times.
 *
 * */
static const struct ctl_command_syntax vsctl_commands[] = {
    /* Open vSwitch commands. */
    {"init", 0, 0, "", NULL, cmd_init, NULL, "", RW},

    /* Bridge commands. */
    {"add-br", 1, 3, "NEW-BRIDGE [PARENT] [NEW-VLAN]", pre_get_info,
     cmd_add_br, NULL, "--may-exist", RW},
    {"del-br", 1, 1, "BRIDGE", pre_get_info, cmd_del_br,
     NULL, "--if-exists", RW},
#ifndef OPS_TEMP
    {"list-br", 0, 0, "", pre_get_info, cmd_list_br, NULL, "--real,--fake",
     RO},
#else
    {"list-br", 0, 0, "", pre_get_info, cmd_list_br, NULL, "", RO},
#endif
    {"br-exists", 1, 1, "BRIDGE", pre_get_info, cmd_br_exists, NULL, "", RO},
    {"br-to-vlan", 1, 1, "BRIDGE", pre_get_info, cmd_br_to_vlan, NULL, "",
     RO},
    {"br-to-parent", 1, 1, "BRIDGE", pre_get_info, cmd_br_to_parent, NULL,
     "", RO},
    {"br-set-external-id", 2, 3, "BRIDGE KEY [VALUE]",
     pre_cmd_br_set_external_id, cmd_br_set_external_id, NULL, "", RW},
    {"br-get-external-id", 1, 2, "BRIDGE [KEY]", pre_cmd_br_get_external_id,
     cmd_br_get_external_id, NULL, "", RO},

#ifdef OPS
    /* VLAN commands. */
    {"list-vlans", 1, 1, "", pre_get_vlan_info, cmd_list_vlans, NULL, "", RO},
    {"add-vlan", 2, INT_MAX, "", pre_get_vlan_info, cmd_add_vlan, NULL,
     "--may-exist", RW},
    {"del-vlan", 2, 2, "", pre_get_vlan_info, cmd_del_vlan, NULL, "--if-exists",
     RW},

    /* VRF commands */
    {"list-vrf", 0, 0, "", pre_get_vrf_info, cmd_list_vrf, NULL, "", RO},
    {"add-vrf", 1, 1, "", pre_get_vrf_info, cmd_add_vrf, NULL,
     "--may-exist", RW},
    {"del-vrf", 1, 1, "", pre_get_vrf_info, cmd_del_vrf, NULL, "--if-exists",
     RW},

    /* VRF Port commands. */
    {"list-vrf-ports", 1, 1, "", pre_get_vrf_info, cmd_list_vrf_ports,
     NULL, "", RO},
    {"add-vrf-port", 2, INT_MAX, "", pre_get_vrf_info, cmd_add_vrf_port,
     NULL, "--may-exist", RW},
    {"del-vrf-port", 1, 2, "", pre_get_vrf_info, cmd_del_vrf_port, NULL,
     "--if-exists", RW},
    {"port-to-vrf", 1, 1, "", pre_get_vrf_info, cmd_port_to_vrf, NULL, "", RO},

#endif
    /* Port commands. */
    {"list-ports", 1, 1, "BRIDGE", pre_get_info, cmd_list_ports, NULL, "",
     RO},
    {"add-port", 2, INT_MAX, "BRIDGE NEW-PORT [COLUMN[:KEY]=VALUE]...",
     pre_get_info, cmd_add_port, NULL, "--may-exist", RW},
    {"add-bond", 4, INT_MAX,
     "BRIDGE NEW-BOND-PORT SYSIFACE... [COLUMN[:KEY]=VALUE]...", pre_get_info,
     cmd_add_bond, NULL, "--may-exist,--fake-iface", RW},
    {"del-port", 1, 2, "[BRIDGE] PORT|IFACE", pre_get_info, cmd_del_port, NULL,
     "--if-exists,--with-iface", RW},
    {"port-to-br", 1, 1, "PORT", pre_get_info, cmd_port_to_br, NULL, "", RO},

    /* Interface commands. */
    {"list-ifaces", 1, 1, "BRIDGE", pre_get_info, cmd_list_ifaces, NULL, "",
     RO},
    {"iface-to-br", 1, 1, "IFACE", pre_get_info, cmd_iface_to_br, NULL, "",
     RO},

#ifndef OPS_TEMP
    /* Controller commands. */
    {"get-controller", 1, 1, "BRIDGE", pre_controller, cmd_get_controller,
     NULL, "", RO},
    {"del-controller", 1, 1, "BRIDGE", pre_controller, cmd_del_controller,
     NULL, "", RW},
    {"set-controller", 1, INT_MAX, "BRIDGE TARGET...", pre_controller,
     cmd_set_controller, NULL, "", RW},
    {"get-fail-mode", 1, 1, "BRIDGE", pre_get_info, cmd_get_fail_mode, NULL,
     "", RO},
    {"del-fail-mode", 1, 1, "BRIDGE", pre_get_info, cmd_del_fail_mode, NULL,
     "", RW},
    {"set-fail-mode", 2, 2, "BRIDGE MODE", pre_get_info, cmd_set_fail_mode,
     NULL, "", RW},
#endif
    /* Manager commands. */
    {"get-manager", 0, 0, "", pre_manager, cmd_get_manager, NULL, "", RO},
    {"del-manager", 0, 0, "", pre_manager, cmd_del_manager, NULL, "", RW},
    {"set-manager", 1, INT_MAX, "TARGET...", pre_manager, cmd_set_manager,
     NULL, "", RW},

    /* SSL commands. */
    {"get-ssl", 0, 0, "", pre_cmd_get_ssl, cmd_get_ssl, NULL, "", RO},
    {"del-ssl", 0, 0, "", pre_cmd_del_ssl, cmd_del_ssl, NULL, "", RW},
    {"set-ssl", 3, 3, "PRIVATE-KEY CERTIFICATE CA-CERT", pre_cmd_set_ssl,
     cmd_set_ssl, NULL, "--bootstrap", RW},

#ifndef OPS_TEMP
    /* Auto Attach commands. */
    {"add-aa-mapping", 3, 3, "BRIDGE ARG ARG", pre_aa_mapping, cmd_add_aa_mapping,
     NULL, "", RW},
    {"del-aa-mapping", 3, 3, "BRIDGE ARG ARG", pre_aa_mapping, cmd_del_aa_mapping,
     NULL, "", RW},
    {"get-aa-mapping", 1, 1, "BRIDGE", pre_aa_mapping, cmd_get_aa_mapping,
     NULL, "", RO},
#endif
    /* Switch commands. */
    {"emer-reset", 0, 0, "", pre_cmd_emer_reset, cmd_emer_reset, NULL, "", RW},

    {NULL, 0, 0, NULL, NULL, NULL, NULL, NULL, RO},
};

/* Registers vsctl and common db commands. */
static void
vsctl_cmd_init(void)
{
    ctl_init(tables, cmd_show_tables, vsctl_exit);
    ctl_register_commands(vsctl_commands);
}
