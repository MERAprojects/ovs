/* Copyright (c) 2009, 2010, 2011, 2012, 2013 Nicira, Inc.
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

#ifndef OVSDB_OVSDB_H
#define OVSDB_OVSDB_H 1

#include "column.h"
#include "compiler.h"
#include "hmap.h"
#include "list.h"
#include "shash.h"

struct json;
struct ovsdb_log;
struct ovsdb_session;
struct ovsdb_txn;
struct simap;
struct uuid;

/* Database schema. */
struct ovsdb_schema {
    char *name;
    char *version;
    char *cksum;
    struct shash tables;        /* Contains "struct ovsdb_table_schema *"s. */
};

struct ovsdb_schema *ovsdb_schema_create(const char *name,
                                         const char *version,
                                         const char *cksum);
struct ovsdb_schema *ovsdb_schema_clone(const struct ovsdb_schema *);
void ovsdb_schema_destroy(struct ovsdb_schema *);

struct ovsdb_error *ovsdb_schema_from_file(const char *file_name,
                                           struct ovsdb_schema **)
    OVS_WARN_UNUSED_RESULT;
struct ovsdb_error *ovsdb_schema_from_json(struct json *,
                                           struct ovsdb_schema **)
    OVS_WARN_UNUSED_RESULT;
struct json *ovsdb_schema_to_json(const struct ovsdb_schema *);

bool ovsdb_schema_equal(const struct ovsdb_schema *,
                        const struct ovsdb_schema *);

/* Database. */
struct ovsdb {
    struct ovsdb_schema *schema;
    struct ovs_list replicas;   /* Contains "struct ovsdb_replica"s. */
    struct shash tables;        /* Contains "struct ovsdb_table *"s. */

    /* Transaction Context */
    struct hmap contexts; /* Contains "struct ovsdb_txn_ctx"s. */

    /* Triggers. */
    struct ovs_list triggers;   /* Contains "struct ovsdb_trigger"s. */
    /* TODO: what data structure fits best here?
     * It will be storing ids and a counter?
     */
    struct hmap blocked_waits; /* Contains "struct ovsdb_blocked_wait"s.*/
    size_t blocked_wait_id; /* Used to generate the blocked wait ids. */

    bool run_triggers;
};

struct ovsdb_txn_ctx {
    struct hmap_node node;
    const struct ovsdb *db;
    const struct ovsdb_session *session;
    const struct json *params;

    /* General operation data */
    size_t current_operation; /* Current operation index */
    size_t max_successful_operation; /* Max operation index reached ever */

    /* blocking_wait */
    struct ovs_list blocking_sessions;
    long long int blocking_wait_id;
    bool blocking_wait_id_is_set;
    bool blocking_wait_aborted;
};

/* struct ctx_to_session allows to model the relation between a transaction
 * (transaction's context) and a session wait blocking that transaction.     */
struct ctx_to_session {
    struct ovs_list node_ctx;
    struct ovs_list node_session;
    struct ovsdb_txn_ctx *ctx;
    struct ovsdb_jsonrpc_session *session;
};

struct ovsdb *ovsdb_create(struct ovsdb_schema *);
void ovsdb_destroy(struct ovsdb *);

void ovsdb_get_memory_usage(const struct ovsdb *, struct simap *usage);

struct ovsdb_table *ovsdb_get_table(const struct ovsdb *, const char *);

struct json *ovsdb_execute(struct ovsdb *, const struct ovsdb_session *,
                           const struct json *params,
                           long long int elapsed_msec,
                           long long int *timeout_msec);
struct ovsdb_txn_ctx *ovsdb_get_context(const struct ovsdb *,
                                        const struct json *);
struct ovsdb_txn_ctx *ovsdb_create_context(const struct ovsdb *,
                                           const struct json *params);
void ovsdb_destroy_context(const struct ovsdb *db, const struct json *params);


/* Database replication. */

struct ovsdb_replica {
    struct ovs_list node;       /* Element in "struct ovsdb" replicas list. */
    const struct ovsdb_replica_class *class;
};

struct ovsdb_replica_class {
    struct ovsdb_error *(*commit)(struct ovsdb_replica *,
                                  const struct ovsdb_txn *, bool durable);
    void (*destroy)(struct ovsdb_replica *);
};

void ovsdb_replica_init(struct ovsdb_replica *,
                        const struct ovsdb_replica_class *);

void ovsdb_add_replica(struct ovsdb *, struct ovsdb_replica *);
void ovsdb_remove_replica(struct ovsdb *, struct ovsdb_replica *);

/* Wait monitor */
struct wait_monitored_data {

    struct ovsdb_jsonrpc_session *session;
    bool wait_updated;                    /* Indicates if request sent to this session was responded */
    struct uuid *uuid;
    size_t uuid_n;
    struct ovsdb_column_set columns;
    struct hmap_node hmap_node;
};

struct blocked_wait {
    struct hmap_node node;
    long long int wait_id;
    int sessions_n;
    struct hmap wait_monitored_data; /* "struct wait_monitored_data" hashed by "struct ovsdb_jsonrpc_session *" */
};

#endif /* ovsdb/ovsdb.h */
