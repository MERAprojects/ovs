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

#include <config.h>

#include <limits.h>

#include "column.h"
#include "condition.h"
#include "file.h"
#include "json.h"
#include "jsonrpc-server.h"
#include "list.h"
#include "mutation.h"
#include "ovsdb-data.h"
#include "ovsdb-error.h"
#include "ovsdb-parser.h"
#include "ovsdb.h"
#include "poll-loop.h"
#include "query.h"
#include "row.h"
#include "server.h"
#include "table.h"
#include "timeval.h"
#include "transaction.h"
#include "hash.h"

struct ovsdb_execution {
    struct ovsdb *db;
    const struct ovsdb_session *session;
    struct ovsdb_txn *txn;
    struct ovsdb_symbol_table *symtab;
    struct ovsdb_txn_ctx *context;
    bool durable;

    /* Triggers. */
    long long int elapsed_msec;
    long long int timeout_msec;
};

enum wait_update_state {
    WAIT_UPDATE_START,
    WAIT_UPDATE_DONE
};

static bool ovsdb_wait_update_build_params(
        struct blocked_wait *,
        struct ovsdb_table *,
        struct ovsdb_column_set *,
        struct uuid *uuids,
        size_t uuids_n);

static void ovsdb_wait_update_send(
        struct ovsdb_table *,
        struct wait_monitored_data *,
        int update_id,
        enum wait_update_state);

typedef struct ovsdb_error *ovsdb_operation_executor(struct ovsdb_execution *,
                                                     struct ovsdb_parser *,
                                                     struct json *result);

static ovsdb_operation_executor ovsdb_execute_insert;
static ovsdb_operation_executor ovsdb_execute_select;
static ovsdb_operation_executor ovsdb_execute_update;
static ovsdb_operation_executor ovsdb_execute_mutate;
static ovsdb_operation_executor ovsdb_execute_delete;
static ovsdb_operation_executor ovsdb_execute_wait;
static ovsdb_operation_executor ovsdb_execute_blocking_wait;
static ovsdb_operation_executor ovsdb_execute_commit;
static ovsdb_operation_executor ovsdb_execute_abort;
static ovsdb_operation_executor ovsdb_execute_comment;
static ovsdb_operation_executor ovsdb_execute_assert;

static ovsdb_operation_executor *
lookup_executor(const char *name)
{
    struct ovsdb_operation {
        const char *name;
        ovsdb_operation_executor *executor;
    };

    static const struct ovsdb_operation operations[] = {
        { "insert", ovsdb_execute_insert },
        { "select", ovsdb_execute_select },
        { "update", ovsdb_execute_update },
        { "mutate", ovsdb_execute_mutate },
        { "delete", ovsdb_execute_delete },
        { "wait", ovsdb_execute_wait },
        { "blocking_wait", ovsdb_execute_blocking_wait },
        { "commit", ovsdb_execute_commit },
        { "abort", ovsdb_execute_abort },
        { "comment", ovsdb_execute_comment },
        { "assert", ovsdb_execute_assert },
    };

    size_t i;

    for (i = 0; i < ARRAY_SIZE(operations); i++) {
        const struct ovsdb_operation *c = &operations[i];
        if (!strcmp(c->name, name)) {
            return c->executor;
        }
    }
    return NULL;
}

struct json *
ovsdb_execute(struct ovsdb *db, const struct ovsdb_session *session,
              const struct json *params,
              long long int elapsed_msec, long long int *timeout_msec)
{
    struct ovsdb_execution x;
    struct ovsdb_error *error;
    struct json *results;
    size_t n_operations;
    size_t i;

    /* Load or create the context for this transaction */
    struct ovsdb_txn_ctx *ctx = ovsdb_get_context(db, params);
    if (!ctx) {
        ctx = ovsdb_create_context(db, params);
    }
    ctx->session = session;

    if (params->type != JSON_ARRAY
        || !params->u.array.n
        || params->u.array.elems[0]->type != JSON_STRING
        || strcmp(params->u.array.elems[0]->u.string, db->schema->name)) {
        if (params->type != JSON_ARRAY) {
            error = ovsdb_syntax_error(params, NULL, "array expected");
        } else {
            error = ovsdb_syntax_error(params, NULL, "database name expected "
                                       "as first parameter");
        }

        results = ovsdb_error_to_json(error);
        ovsdb_error_destroy(error);
        if (results) {
            ovsdb_destroy_context(db, params);
        }
        return results;
    }

    x.db = db;
    x.session = session;
    x.txn = ovsdb_txn_create(db);
    x.symtab = ovsdb_symbol_table_create();
    x.durable = false;
    x.elapsed_msec = elapsed_msec;
    x.timeout_msec = LLONG_MAX;
    x.context = ctx;
    results = NULL;

    results = json_array_create_empty();
    n_operations = params->u.array.n - 1;
    error = NULL;
    for (i = 1; i <= n_operations; i++) {
        struct json *operation = params->u.array.elems[i];
        struct ovsdb_error *parse_error;
        struct ovsdb_parser parser;
        struct json *result;
        const struct json *op;

        /* Update the operation index in context */
        ctx->current_operation = i;

        /* Parse and execute operation. */
        ovsdb_parser_init(&parser, operation,
                          "ovsdb operation %"PRIuSIZE" of %"PRIuSIZE, i, n_operations);
        op = ovsdb_parser_member(&parser, "op", OP_ID);
        result = json_object_create();
        if (op) {
            const char *op_name = json_string(op);
            ovsdb_operation_executor *executor = lookup_executor(op_name);
            if (executor) {
                error = executor(&x, &parser, result);
            } else {
                ovsdb_parser_raise_error(&parser, "No operation \"%s\"",
                                         op_name);
            }
        } else {
            ovs_assert(ovsdb_parser_has_error(&parser));
        }

        /* A parse error overrides any other error.
         * An error overrides any other result. */
        parse_error = ovsdb_parser_finish(&parser);
        if (parse_error) {
            ovsdb_error_destroy(error);
            error = parse_error;
        }
        if (error) {
            json_destroy(result);
            result = ovsdb_error_to_json(error);
        }
        if (error && !strcmp(ovsdb_error_get_tag(error), "not supported")
            && timeout_msec) {
            ovsdb_txn_abort(x.txn);
            *timeout_msec = x.timeout_msec;

            json_destroy(result);
            json_destroy(results);
            results = NULL;
            goto exit;
        }

        /* Add result to array. */
        json_array_add(results, result);
        if (error) {
            break;
        } else {
            ctx->max_successful_operation = i;
        }
    }

    if (!error) {
        error = ovsdb_txn_commit(x.txn, x.durable);
        if (error) {
            json_array_add(results, ovsdb_error_to_json(error));
        }
    } else {
        ovsdb_txn_abort(x.txn);
    }

    while (json_array(results)->n < n_operations) {
        json_array_add(results, json_null_create());
    }

exit:
    ovsdb_error_destroy(error);
    ovsdb_symbol_table_destroy(x.symtab);

    if (results) {
        ovsdb_destroy_context(db, params);
    }

    return results;
}

static struct ovsdb_error *
ovsdb_execute_commit(struct ovsdb_execution *x, struct ovsdb_parser *parser,
                     struct json *result OVS_UNUSED)
{
    const struct json *durable;

    durable = ovsdb_parser_member(parser, "durable", OP_BOOLEAN);
    if (durable && json_boolean(durable)) {
        x->durable = true;
    }
    return NULL;
}

static struct ovsdb_error *
ovsdb_execute_abort(struct ovsdb_execution *x OVS_UNUSED,
                    struct ovsdb_parser *parser OVS_UNUSED,
                    struct json *result OVS_UNUSED)
{
    return ovsdb_error("aborted", "aborted by request");
}

static struct ovsdb_table *
parse_table(struct ovsdb_execution *x,
            struct ovsdb_parser *parser, const char *member)
{
    struct ovsdb_table *table;
    const char *table_name;
    const struct json *json;

    json = ovsdb_parser_member(parser, member, OP_ID);
    if (!json) {
        return NULL;
    }
    table_name = json_string(json);

    table = shash_find_data(&x->db->tables, table_name);
    if (!table) {
        ovsdb_parser_raise_error(parser, "No table named %s.", table_name);
    }
    return table;
}

static OVS_WARN_UNUSED_RESULT struct ovsdb_error *
parse_row(const struct json *json, const struct ovsdb_table *table,
          struct ovsdb_symbol_table *symtab,
          struct ovsdb_row **rowp, struct ovsdb_column_set *columns)
{
    struct ovsdb_error *error;
    struct ovsdb_row *row;

    *rowp = NULL;

    if (!table) {
        return OVSDB_BUG("null table");
    }
    if (!json) {
        return OVSDB_BUG("null row");
    }

    row = ovsdb_row_create(table);
    error = ovsdb_row_from_json(row, json, symtab, columns);
    if (error) {
        ovsdb_row_destroy(row);
        return error;
    } else {
        *rowp = row;
        return NULL;
    }
}

static struct ovsdb_error *
ovsdb_execute_insert(struct ovsdb_execution *x, struct ovsdb_parser *parser,
                     struct json *result)
{
    struct ovsdb_table *table;
    struct ovsdb_row *row = NULL;
    const struct json *uuid_name, *row_json;
    struct ovsdb_error *error;
    struct uuid row_uuid;

    table = parse_table(x, parser, "table");
    uuid_name = ovsdb_parser_member(parser, "uuid-name", OP_ID | OP_OPTIONAL);
    row_json = ovsdb_parser_member(parser, "row", OP_OBJECT);
    error = ovsdb_parser_get_error(parser);
    if (error) {
        return error;
    }

    if (uuid_name) {
        struct ovsdb_symbol *symbol;

        symbol = ovsdb_symbol_table_insert(x->symtab, json_string(uuid_name));
        if (symbol->created) {
            return ovsdb_syntax_error(uuid_name, "duplicate uuid-name",
                                      "This \"uuid-name\" appeared on an "
                                      "earlier \"insert\" operation.");
        }
        row_uuid = symbol->uuid;
        symbol->created = true;
    } else {
        uuid_generate(&row_uuid);
    }

    if (!error) {
        error = parse_row(row_json, table, x->symtab, &row, NULL);
    }
    if (!error) {
        /* Check constraints for columns not included in "row", in case the
         * default values do not satisfy the constraints.  We could check only
         * the columns that have their default values by supplying an
         * ovsdb_column_set to parse_row() above, but I suspect that this is
         * cheaper.  */
        const struct shash_node *node;

        SHASH_FOR_EACH (node, &table->schema->columns) {
            const struct ovsdb_column *column = node->data;
            const struct ovsdb_datum *datum = &row->fields[column->index];

            /* If there are 0 keys or pairs, there's nothing to check.
             * If there is 1, it might be a default value.
             * If there are more, it can't be a default value, so the value has
             * already been checked. */
            if (datum->n == 1) {
                error = ovsdb_datum_check_constraints(datum, &column->type);
                if (error) {
                    error = ovsdb_wrap_error(error, "Table - \"%s\", Column - "
                                             "\"%s\"", table->schema->name,
                                             column->name);
                    ovsdb_row_destroy(row);
                    break;
                }
            }
        }
    }
    if (!error) {
        *ovsdb_row_get_uuid_rw(row) = row_uuid;
        ovsdb_txn_row_insert(x->txn, row);
        json_object_put(result, "uuid",
                        ovsdb_datum_to_json(&row->fields[OVSDB_COL_UUID],
                                            &ovsdb_type_uuid));
    }
    return error;
}

static struct ovsdb_error *
ovsdb_execute_select(struct ovsdb_execution *x, struct ovsdb_parser *parser,
                     struct json *result)
{
    struct ovsdb_table *table;
    const struct json *where, *columns_json, *sort_json;
    struct ovsdb_condition condition = OVSDB_CONDITION_INITIALIZER;
    struct ovsdb_column_set columns = OVSDB_COLUMN_SET_INITIALIZER;
    struct ovsdb_column_set sort = OVSDB_COLUMN_SET_INITIALIZER;
    struct ovsdb_error *error;

    table = parse_table(x, parser, "table");
    where = ovsdb_parser_member(parser, "where", OP_ARRAY);
    columns_json = ovsdb_parser_member(parser, "columns",
                                       OP_ARRAY | OP_OPTIONAL);
    sort_json = ovsdb_parser_member(parser, "sort", OP_ARRAY | OP_OPTIONAL);

    error = ovsdb_parser_get_error(parser);
    if (!error) {
        error = ovsdb_condition_from_json(table->schema, where, x->symtab,
                                          &condition);
    }
    if (!error) {
        error = ovsdb_column_set_from_json(columns_json, table->schema,
                                           &columns);
    }
    if (!error) {
        error = ovsdb_column_set_from_json(sort_json, table->schema, &sort);
    }
    if (!error) {
        struct ovsdb_row_set rows = OVSDB_ROW_SET_INITIALIZER;

        ovsdb_query_distinct(table, &condition, &columns, &rows);
        ovsdb_row_set_sort(&rows, &sort);
        json_object_put(result, "rows",
                        ovsdb_row_set_to_json(&rows, &columns));

        ovsdb_row_set_destroy(&rows);
    }

    ovsdb_column_set_destroy(&columns);
    ovsdb_column_set_destroy(&sort);
    ovsdb_condition_destroy(&condition);

    return error;
}

struct update_row_cbdata {
    size_t n_matches;
    struct ovsdb_txn *txn;
    const struct ovsdb_row *row;
    const struct ovsdb_column_set *columns;
};

static bool
update_row_cb(const struct ovsdb_row *row, void *ur_)
{
    struct update_row_cbdata *ur = ur_;

    ur->n_matches++;
    if (!ovsdb_row_equal_columns(row, ur->row, ur->columns)) {
        ovsdb_row_update_columns(ovsdb_txn_row_modify(ur->txn, row),
                                 ur->row, ur->columns);
    }

    return true;
}

static struct ovsdb_error *
ovsdb_execute_update(struct ovsdb_execution *x, struct ovsdb_parser *parser,
                     struct json *result)
{
    struct ovsdb_table *table;
    const struct json *where, *row_json;
    struct ovsdb_condition condition = OVSDB_CONDITION_INITIALIZER;
    struct ovsdb_column_set columns = OVSDB_COLUMN_SET_INITIALIZER;
    struct ovsdb_row *row = NULL;
    struct update_row_cbdata ur;
    struct ovsdb_error *error;

    table = parse_table(x, parser, "table");
    where = ovsdb_parser_member(parser, "where", OP_ARRAY);
    row_json = ovsdb_parser_member(parser, "row", OP_OBJECT);
    error = ovsdb_parser_get_error(parser);
    if (!error) {
        error = parse_row(row_json, table, x->symtab, &row, &columns);
    }
    if (!error) {
        size_t i;

        for (i = 0; i < columns.n_columns; i++) {
            const struct ovsdb_column *column = columns.columns[i];

            if (!column->mutable) {
                error = ovsdb_syntax_error(parser->json,
                                           "constraint violation",
                                           "Cannot update immutable column %s "
                                           "in table %s.",
                                           column->name, table->schema->name);
                break;
            }
        }
    }
    if (!error) {
        error = ovsdb_condition_from_json(table->schema, where, x->symtab,
                                          &condition);
    }
    if (!error) {
        ur.n_matches = 0;
        ur.txn = x->txn;
        ur.row = row;
        ur.columns = &columns;
        ovsdb_query(table, &condition, update_row_cb, &ur);
        json_object_put(result, "count", json_integer_create(ur.n_matches));
    }

    ovsdb_row_destroy(row);
    ovsdb_column_set_destroy(&columns);
    ovsdb_condition_destroy(&condition);

    return error;
}

struct mutate_row_cbdata {
    size_t n_matches;
    struct ovsdb_txn *txn;
    const struct ovsdb_mutation_set *mutations;
    struct ovsdb_error **error;
};

static bool
mutate_row_cb(const struct ovsdb_row *row, void *mr_)
{
    struct mutate_row_cbdata *mr = mr_;

    mr->n_matches++;
    *mr->error = ovsdb_mutation_set_execute(ovsdb_txn_row_modify(mr->txn, row),
                                            mr->mutations);
    return *mr->error == NULL;
}

static struct ovsdb_error *
ovsdb_execute_mutate(struct ovsdb_execution *x, struct ovsdb_parser *parser,
                     struct json *result)
{
    struct ovsdb_table *table;
    const struct json *where;
    const struct json *mutations_json;
    struct ovsdb_condition condition = OVSDB_CONDITION_INITIALIZER;
    struct ovsdb_mutation_set mutations = OVSDB_MUTATION_SET_INITIALIZER;
    struct ovsdb_row *row = NULL;
    struct mutate_row_cbdata mr;
    struct ovsdb_error *error;

    table = parse_table(x, parser, "table");
    where = ovsdb_parser_member(parser, "where", OP_ARRAY);
    mutations_json = ovsdb_parser_member(parser, "mutations", OP_ARRAY);
    error = ovsdb_parser_get_error(parser);
    if (!error) {
        error = ovsdb_mutation_set_from_json(table->schema, mutations_json,
                                             x->symtab, &mutations);
    }
    if (!error) {
        error = ovsdb_condition_from_json(table->schema, where, x->symtab,
                                          &condition);
    }
    if (!error) {
        mr.n_matches = 0;
        mr.txn = x->txn;
        mr.mutations = &mutations;
        mr.error = &error;
        ovsdb_query(table, &condition, mutate_row_cb, &mr);
        json_object_put(result, "count", json_integer_create(mr.n_matches));
    }

    ovsdb_row_destroy(row);
    ovsdb_mutation_set_destroy(&mutations);
    ovsdb_condition_destroy(&condition);

    return error;
}

struct delete_row_cbdata {
    size_t n_matches;
    const struct ovsdb_table *table;
    struct ovsdb_txn *txn;
};

static bool
delete_row_cb(const struct ovsdb_row *row, void *dr_)
{
    struct delete_row_cbdata *dr = dr_;

    dr->n_matches++;
    ovsdb_txn_row_delete(dr->txn, row);

    return true;
}

static struct ovsdb_error *
ovsdb_execute_delete(struct ovsdb_execution *x, struct ovsdb_parser *parser,
                     struct json *result)
{
    struct ovsdb_table *table;
    const struct json *where;
    struct ovsdb_condition condition = OVSDB_CONDITION_INITIALIZER;
    struct ovsdb_error *error;

    where = ovsdb_parser_member(parser, "where", OP_ARRAY);
    table = parse_table(x, parser, "table");
    error = ovsdb_parser_get_error(parser);
    if (!error) {
        error = ovsdb_condition_from_json(table->schema, where, x->symtab,
                                          &condition);
    }
    if (!error) {
        struct delete_row_cbdata dr;

        dr.n_matches = 0;
        dr.table = table;
        dr.txn = x->txn;
        ovsdb_query(table, &condition, delete_row_cb, &dr);

        json_object_put(result, "count", json_integer_create(dr.n_matches));
    }

    ovsdb_condition_destroy(&condition);

    return error;
}

struct wait_auxdata {
    struct ovsdb_row_hash *actual;
    struct ovsdb_row_hash *expected;
    bool *equal;
};

static bool
ovsdb_execute_wait_query_cb(const struct ovsdb_row *row, void *aux_)
{
    struct wait_auxdata *aux = aux_;

    if (ovsdb_row_hash_contains(aux->expected, row)) {
        ovsdb_row_hash_insert(aux->actual, row);
        return true;
    } else {
        /* The query row isn't in the expected result set, so the actual and
         * expected results sets definitely differ and we can short-circuit the
         * rest of the query. */
        *aux->equal = false;
        return false;
    }
}

/* ovsdb_wait_update_build_params_empty_columns builds a blocked_wait
 * struct, when the columns ovsdb_column_set is EMPTY.
 * WARNING: THIS FUNCTION PANICS IF THE COLUMN SET ISN'T EMPTY.
 */
static bool ovsdb_wait_update_build_params_empty_columns(
        struct blocked_wait *blocked_wait,
        struct ovsdb_table *table,
        struct ovsdb_column_set *columns,
        struct uuid *uuids,
        size_t uuids_n)
{
    struct wait_monitored_data *monitored_data;
    struct ovsdb_wait_monitoring *wm;
    struct ovsdb_column *col;
    struct shash_node *node;
    bool exists_monitors;

    hmap_init(&blocked_wait->wait_monitored_data);

    if (columns->n_columns != 0) {
        OVS_NOT_REACHED();
    }

    /* Checks that there is at least one session wait monitoring the columns */
    exists_monitors = false;
    SHASH_FOR_EACH(node, &table->schema->columns) {
        col = node->data;
        /* check if this column can be updated */
        if (!list_is_empty(&col->wait_monitoring)) {
            exists_monitors = true;
            break;
        }
    }
    if (!exists_monitors) {
        return false;
    }

    /* no columns indicated only rows information is sent.
     * Send wait update notifications to all  */
    SHASH_FOR_EACH(node, &table->schema->columns) {
        col = node->data;

        LIST_FOR_EACH (wm, column_node, &col->wait_monitoring) {
            bool session_found = false;
            struct wait_monitored_data *wm_tmp;
            HMAP_FOR_EACH_WITH_HASH(wm_tmp, hmap_node,
                    hash_pointer(wm->session, 0),
                    &blocked_wait->wait_monitored_data) {
                /* check if this session is already listed */
                if (wm_tmp->session == wm->session) {
                    session_found = true;
                    break;
                }
            }

            if (!session_found) {
                /* initialize column set and add column */
                monitored_data = xmalloc(sizeof *monitored_data);
                ovsdb_column_set_init(&monitored_data->columns);
                monitored_data->session = wm->session;
                monitored_data->uuid = uuids;
                monitored_data->uuid_n = uuids_n;

                hmap_insert(&blocked_wait->wait_monitored_data,
                            &monitored_data->hmap_node,
                            hash_pointer(wm->session, 0));
            }
        }
    }

    return true;
}

static bool ovsdb_wait_update_build_params(
        struct blocked_wait *blocked_wait,
        struct ovsdb_table *table,
        struct ovsdb_column_set *columns,
        struct uuid *uuids,
        size_t uuids_n) {

    int i;
    const struct ovsdb_column *column_schema_data;
    struct wait_monitored_data *monitored_data;
    hmap_init(&blocked_wait->wait_monitored_data);

    /* no columns indicated only rows information is sent.
     * Send wait update notifications to all  */
    if (columns->n_columns == 0) {
        return ovsdb_wait_update_build_params_empty_columns(blocked_wait, table,
                                                            columns, uuids,
                                                            uuids_n);
    }

    /* check if all the requested columns can be updated, fail otherwise */
    for (i = 0; i < columns->n_columns; i++) {
        column_schema_data = columns->columns[i];
        /* check if this column can be updated */
        if (list_is_empty(&column_schema_data->wait_monitoring)) {
            hmap_destroy(&blocked_wait->wait_monitored_data);
            return false;
        }
    }

    /* build a hash table with the list of cells for each column */
    for (i = 0; i < columns->n_columns; i++) {
        column_schema_data = columns->columns[i];

        struct ovsdb_wait_monitoring *wm;
        LIST_FOR_EACH(wm, column_node,
                      &column_schema_data->wait_monitoring) {
            /* search for this session in the hmap */
            bool existing_session = false;
            HMAP_FOR_EACH_WITH_HASH(monitored_data, hmap_node,
                    hash_pointer(wm->session, 0),
                    &blocked_wait->wait_monitored_data) {

                if (monitored_data->session == wm->session) {
                    existing_session = true;
                    ovsdb_column_set_add(&monitored_data->columns,
                                         column_schema_data);
                    break;
                }
            }

            if (!existing_session) {
                /* initialize column set and add column */
                monitored_data = xmalloc(sizeof *monitored_data);
                ovsdb_column_set_init(&monitored_data->columns);
                ovsdb_column_set_add(
                        &monitored_data->columns,
                        column_schema_data);

                monitored_data->session = wm->session;
                monitored_data->uuid = uuids;
                monitored_data->uuid_n = uuids_n;

                hmap_insert(&blocked_wait->wait_monitored_data,
                            &monitored_data->hmap_node,
                            hash_pointer(wm->session, 0));
            }
        }
    }

    return true;
}

static struct ovsdb_error *
ovsdb_execute_wait(struct ovsdb_execution *x, struct ovsdb_parser *parser,
                   struct json *result OVS_UNUSED)
{
    struct ovsdb_table *table;
    const struct json *timeout, *where, *columns_json, *until, *rows;
    struct ovsdb_condition condition = OVSDB_CONDITION_INITIALIZER;
    struct ovsdb_column_set columns = OVSDB_COLUMN_SET_INITIALIZER;
    struct ovsdb_row_hash expected = OVSDB_ROW_HASH_INITIALIZER(expected);
    struct ovsdb_row_hash actual = OVSDB_ROW_HASH_INITIALIZER(actual);
    struct ovsdb_error *error;
    struct wait_auxdata aux;
    long long int timeout_msec = 0;
    size_t i;

    timeout = ovsdb_parser_member(parser, "timeout", OP_NUMBER | OP_OPTIONAL);
    where = ovsdb_parser_member(parser, "where", OP_ARRAY);
    columns_json = ovsdb_parser_member(parser, "columns",
                                       OP_ARRAY | OP_OPTIONAL);
    until = ovsdb_parser_member(parser, "until", OP_STRING);
    rows = ovsdb_parser_member(parser, "rows", OP_ARRAY);
    table = parse_table(x, parser, "table");
    error = ovsdb_parser_get_error(parser);
    if (!error) {
        error = ovsdb_condition_from_json(table->schema, where, x->symtab,
                                          &condition);
    }
    if (!error) {
        error = ovsdb_column_set_from_json(columns_json, table->schema,
                                           &columns);
    }
    if (!error) {
        if (timeout) {
            timeout_msec = MIN(LLONG_MAX, json_real(timeout));
            if (timeout_msec < 0) {
                error = ovsdb_syntax_error(timeout, NULL,
                                           "timeout must be nonnegative");
            } else if (timeout_msec < x->timeout_msec) {
                x->timeout_msec = timeout_msec;
            }
        } else {
            timeout_msec = LLONG_MAX;
        }
    }
    if (!error) {
        if (strcmp(json_string(until), "==")
            && strcmp(json_string(until), "!=")) {
            error = ovsdb_syntax_error(until, NULL,
                                       "\"until\" must be \"==\" or \"!=\"");
        }
    }
    if (!error) {
        /* Parse "rows" into 'expected'. */
        ovsdb_row_hash_init(&expected, &columns);
        for (i = 0; i < rows->u.array.n; i++) {
            struct ovsdb_row *row;

            row = ovsdb_row_create(table);
            error = ovsdb_row_from_json(row, rows->u.array.elems[i], x->symtab,
                                        NULL);
            if (error) {
                ovsdb_row_destroy(row);
                break;
            }

            if (!ovsdb_row_hash_insert(&expected, row)) {
                /* XXX Perhaps we should abort with an error or log a
                 * warning. */
                ovsdb_row_destroy(row);
            }
        }
    }
    if (!error) {
        /* Execute query. */
        bool equal = true;
        ovsdb_row_hash_init(&actual, &columns);
        aux.actual = &actual;
        aux.expected = &expected;
        aux.equal = &equal;
        ovsdb_query(table, &condition, ovsdb_execute_wait_query_cb, &aux);
        if (equal) {
            /* We know that every row in 'actual' is also in 'expected'.  We
             * also know that all of the rows in 'actual' are distinct and that
             * all of the rows in 'expected' are distinct.  Therefore, if
             * 'actual' and 'expected' have the same number of rows, then they
             * have the same content. */
            size_t n_actual = ovsdb_row_hash_count(&actual);
            size_t n_expected = ovsdb_row_hash_count(&expected);
            equal = n_actual == n_expected;
        }
        if (!strcmp(json_string(until), "==") != equal) {
            if (timeout && x->elapsed_msec >= timeout_msec) {
                if (x->elapsed_msec) {
                    error = ovsdb_error("timed out",
                                        "\"wait\" timed out after %lld ms",
                                        x->elapsed_msec);
                } else {
                    error = ovsdb_error("timed out", "\"wait\" timed out");
                }
            } else {
                /* ovsdb_execute() will change this, if triggers really are
                 * supported. */
                error = ovsdb_error("not supported", "triggers not supported");
            }
        }
    }


    ovsdb_row_hash_destroy(&expected, true);
    ovsdb_row_hash_destroy(&actual, false);
    ovsdb_column_set_destroy(&columns);
    ovsdb_condition_destroy(&condition);

    return error;
}

static struct ovsdb_error *
ovsdb_execute_blocking_wait(struct ovsdb_execution *x,
                                   struct ovsdb_parser *parser,
                                   struct json *result OVS_UNUSED)
{
    struct uuid *uuid = NULL;
    struct ovsdb_table *table;
    const struct json *timeout, *columns_json, *rows;
    struct ovsdb_column_set columns = OVSDB_COLUMN_SET_INITIALIZER;
    struct ovsdb_error *error;
    long long int timeout_msec = 0;
    bool wait_unblocked = false; /* True if a blocked wait should be
                                  * unblocked. */

    timeout = ovsdb_parser_member(parser, "timeout", OP_NUMBER | OP_OPTIONAL);
    /*TODO: why is columns marked as optional?
     * The RFC defines it as required. */
    columns_json = ovsdb_parser_member(parser, "columns",
                                       OP_ARRAY | OP_OPTIONAL);
    rows = ovsdb_parser_member(parser, "rows", OP_ARRAY);
    table = parse_table(x, parser, "table");
    error = ovsdb_parser_get_error(parser);

    if (!error && x->context->blocking_wait_aborted) {
        error = ovsdb_error("wait unsatisfiable",
                            "wait condition cannot be unblocked by "
                            "currently connected sessions");
    }

    /* Checks if the operation was already performed successfully */
    if (!error &&
        x->context->current_operation <= x->context->max_successful_operation) {
        goto finish;
    }
    if (!error) {
        error = ovsdb_column_set_from_json(columns_json, table->schema,
                                           &columns);
    }
    if (!error) {
        if (timeout) {
            timeout_msec = MIN(LLONG_MAX, json_real(timeout));
            if (timeout_msec < 0) {
                error = ovsdb_syntax_error(timeout, NULL,
                                           "timeout must be nonnegative");
            } else if (timeout_msec < x->timeout_msec) {
                x->timeout_msec = timeout_msec;
            }
        } else {
            timeout_msec = LLONG_MAX;
        }
    }
    if (!error) {
        uuid = xmalloc(rows->u.array.n * sizeof *uuid);
        for (int i = 0; i < rows->u.array.n; i++) {
            if (!error) {
                if (rows->u.array.elems[i]->type != JSON_STRING) {
                    error = ovsdb_syntax_error(rows, NULL,
                                           "Rows must contain a single "
                                           "string element when \"until\" "
                                           "equals \"unblocked\"");
                }
            }
            if (!error) {
                if (!uuid_from_string(&uuid[i], rows->u.array.elems[i]->u.string)) {
                    error = ovsdb_syntax_error(rows, NULL,
                                           "Rows must contain the UUID of "
                                           "row that the wait is blocking "
                                           "when \"until\" equals "
                                           "\"unblocked\"");
                }
            }
        }
        if (!error) {
            if (x->context->blocking_wait_id_is_set) {
                /* Check if wait id is in the list of unblocked waits */
                bool is_blocked = false;
                struct blocked_wait *blocked_wait;
                HMAP_FOR_EACH_WITH_HASH(blocked_wait, node,
                        hash_int(x->context->blocking_wait_id, 0),
                        &x->db->blocked_waits) {

                    if (x->context->blocking_wait_id == blocked_wait->wait_id) {
                        is_blocked = true;
                        break;
                    }
                }

                /* check if wait_monitor should be unblocked */
                if (is_blocked && (blocked_wait->sessions_n == 0)) {
                    poll_immediate_wake_at(NULL);
                    x->db->run_triggers = true;
                    hmap_remove(&x->db->blocked_waits, &blocked_wait->node);

                    /* send final wait_update message */
                    struct wait_monitored_data *monitored_data, *next;
                    HMAP_FOR_EACH_SAFE(monitored_data, next, hmap_node,
                            &blocked_wait->wait_monitored_data) {

                        ovsdb_wait_update_send(table, monitored_data,
                                               x->context->blocking_wait_id,
                                               WAIT_UPDATE_DONE);
                        /* Releases monitored_data */
                        hmap_remove(&blocked_wait->wait_monitored_data,
                                    &monitored_data->hmap_node);
                        ovsdb_column_set_destroy(&monitored_data->columns);
                        free(monitored_data);
                    }
                    /* Releases blocked_wait */
                    hmap_destroy(&blocked_wait->wait_monitored_data);
                    free(blocked_wait);
                    wait_unblocked = true;
                    x->context->blocking_wait_id_is_set = false;
                }
            } else { /* new blocked wait */

                struct blocked_wait *blocked_wait = xmalloc(sizeof *blocked_wait);

                bool params_status;
                params_status = ovsdb_wait_update_build_params(
                        blocked_wait,
                        table,
                        &columns,
                        uuid,
                        rows->u.array.n);

                if (params_status) {
                    x->context->blocking_wait_id = x->db->blocked_wait_id++;
                    x->context->blocking_wait_id_is_set = true;
                    blocked_wait->wait_id = x->context->blocking_wait_id;

                    /* One wait_update request should be sent for each
                     * IDL wait monitoring columns included in the client
                     * request
                     */
                    struct wait_monitored_data *monitored_data;

                    int wait_update_sessions = 0;

                    /* send a wait_update request for each session
                     * monitoring the wait */
                    HMAP_FOR_EACH(monitored_data, hmap_node,
                            &blocked_wait->wait_monitored_data) {

                        ovsdb_jsonrpc_blocking_wait_add_to_txn(monitored_data->session, x->context);
                        monitored_data->wait_updated = false;
                        ovsdb_wait_update_send(table, monitored_data,
                                               x->context->blocking_wait_id,
                                               WAIT_UPDATE_START);


                        wait_update_sessions++;
                    }

                    /* check sessions associated with this request*/
                    blocked_wait->sessions_n = wait_update_sessions;

                    /* insert monitor_wait to the database list */
                    hmap_insert(&x->db->blocked_waits, &blocked_wait->node,
                            hash_int(blocked_wait->wait_id, 0));
                } else {
                    /* blocking wait should fail immediately */
                    error = ovsdb_error("wait unsatisfiable",
                            "wait condition cannot be unblocked by "
                            "currently connected sessions");
                }
            }
        }
    }
    if (!error) {
        if (timeout && x->elapsed_msec >= timeout_msec) {

            /* Check if wait id is in the list of unblocked waits */
            struct blocked_wait *blocked_wait;
            HMAP_FOR_EACH_WITH_HASH(blocked_wait, node,
                    hash_int(x->context->blocking_wait_id,0),
                    &x->db->blocked_waits) {

                if (x->context->blocking_wait_id == blocked_wait->wait_id) {
                    break;
                }
            }

            hmap_remove(&x->db->blocked_waits, &blocked_wait->node);

            /* send final wait_update timeout*/
            struct wait_monitored_data *monitored_data, *next;
            HMAP_FOR_EACH_SAFE(monitored_data, next, hmap_node,
                    &blocked_wait->wait_monitored_data) {

                ovsdb_wait_update_send(table, monitored_data,
                                       x->context->blocking_wait_id,
                                       WAIT_UPDATE_DONE);
                /* Releases monitored_data */
                hmap_remove(&blocked_wait->wait_monitored_data,
                            &monitored_data->hmap_node);
                ovsdb_column_set_destroy(&monitored_data->columns);
                free(monitored_data);
            }
            /* Releases blocked_wait */
            hmap_destroy(&blocked_wait->wait_monitored_data);
            free(blocked_wait);

            if (x->elapsed_msec) {
                error = ovsdb_error("timed out",
                                    "\"wait until unblocked\" timed out after %lld ms",
                                    x->elapsed_msec);
            } else {
                error = ovsdb_error("timed out", "\"wait until unblocked\" timed out");
            }
        } else if (!wait_unblocked) {
            /* ovsdb_execute() will change this, if triggers really are
             * supported. */
            error = ovsdb_error("not supported", "triggers not supported");
        }
    }
finish:
    ovsdb_column_set_destroy(&columns);

    return error;
}

static struct ovsdb_error *
ovsdb_execute_comment(struct ovsdb_execution *x, struct ovsdb_parser *parser,
                      struct json *result OVS_UNUSED)
{
    const struct json *comment;

    comment = ovsdb_parser_member(parser, "comment", OP_STRING);
    if (!comment) {
        return NULL;
    }
    ovsdb_txn_add_comment(x->txn, json_string(comment));

    return NULL;
}

static struct ovsdb_error *
ovsdb_execute_assert(struct ovsdb_execution *x, struct ovsdb_parser *parser,
                     struct json *result OVS_UNUSED)
{
    const struct json *lock_name;

    lock_name = ovsdb_parser_member(parser, "lock", OP_ID);
    if (!lock_name) {
        return NULL;
    }

    if (x->session) {
        const struct ovsdb_lock_waiter *waiter;

        waiter = ovsdb_session_get_lock_waiter(x->session,
                                               json_string(lock_name));
        if (waiter && ovsdb_lock_waiter_is_owner(waiter)) {
            return NULL;
        }
    }

    return ovsdb_error("not owner", "Asserted lock %s not held.",
                       json_string(lock_name));
}

static void
ovsdb_wait_update_send(struct ovsdb_table *table,
                       struct wait_monitored_data *monitored_data,
                       int update_id,
                       enum wait_update_state state)
{
    struct ovsdb_column_set *columns = &monitored_data->columns;
    struct uuid *uuids = monitored_data->uuid;
    size_t uuid_n = monitored_data->uuid_n;
    struct ovsdb_jsonrpc_session *session = monitored_data->session;
    size_t i;
    struct json *wait_update_msg = json_object_create();
    struct json *params = json_array_create_empty();
    struct json *rows_array = json_array_create_empty();
    struct json *columns_array = json_array_create_empty();
    char uuid_buffer[UUID_LEN+1];

    for (i = 0; i < columns->n_columns; i++) {
        json_array_add(columns_array, json_string_create(
                columns->columns[i]->name));
    }

    for (i = 0; i < uuid_n; i++) {
        sprintf(uuid_buffer, UUID_FMT, UUID_ARGS(&uuids[i]));
        json_array_add(rows_array, json_string_create(uuid_buffer));
    }

    /* wait_update message UUID */
    json_object_put(wait_update_msg, "update_id", json_integer_create(update_id));

    /* wait_update message table */
    json_object_put(wait_update_msg, "table",
            json_string_create(table->schema->name));

    /* insert columns array */
    json_object_put(wait_update_msg, "columns", columns_array);

    /* insert rows array */
    json_object_put(wait_update_msg, "rows", rows_array);

    /* wait_update message state */
    if (state == WAIT_UPDATE_START) {
        json_object_put(wait_update_msg, "state", json_string_create("start"));
    } else {
        json_object_put(wait_update_msg, "state", json_string_create("done"));
    }

    /* params is required to be a JSON_ARRAY */
    json_array_add(params, wait_update_msg);

    /* send notification */
    struct jsonrpc_msg *msg = jsonrpc_create_notify(
                "wait_update", params);
    ovsdb_jsonrpc_server_session_send(session, msg);
}
