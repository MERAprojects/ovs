# Using indexes for special operations

----------

## Introduction

The OVSDB database was designed for powering a virtual switch, therefore not all
common DB functionality was needed at the time it was implemented. However, as
part of the OpenSwitch project, developers found scenarios where this common
functionality would be useful. One of this functionality is using indexes for
operations such as sorting or fast lookup.

This design document describes a proposed solution for implementing this extra
functionality based on the concept of indexes.

Please note that in this document, the term "index" refers to the common
database term defined as "a data structure that improves data retrieval". Unless
stated otherwise, the definition for index from the OVSDB RFC (RFC 7047) is not
used.

## Problem statement

Some of the teams working on OpenSwitch would like to have available tools for
special operations. This tools are not currently available on the OVSDB engine.

The most common tools requested are for doing fast lookups of big tables and for
a mechanism to retrieve data in lexicographic order.

## Use cases

### Fast lookups

Depending on the topology, the route table of a network device could manage
thousands of routes. Commands such as "show ip route <*specific route*>" would
need to do a sequential lookup of the routing table to find the specific route.
With an index created, the lookup time could be faster.

This same scenario could be applied to other features such as Access List rules
and even interfaces lists.

### Lexicographic order

There are several cases where retrieving data in lexicographic order is needed.
For example, SNMP. When an administrator or even a NMS would like to retrieve
data from a specific device, it's possible that they will request data from full
tables instead of just specific values. Also, they would like to have this
information displayed in lexicographic order. This operation could be done by
the SNMP daemon or by the CLI, but it would be better if the database could
provide the data ready for consumption. Also, duplicate efforts by different
processes will be avoided. Another use case for requesting data in lexicographic
order is for user interfaces (web or CLI) where it would be better and quicker
if the DB sends the data sorted instead of letting each process to sort the data
by itself.

## Implementation

The proposal is to create a data structure in memory that contains pointers to
the rows where the desired information is stored. This data structure can be
traversed in the order specified when creating the index.

An index can be defined over any number of columns, and support the following
options:

-   Add a column with type string, int or real (using default comparators).
-   Select ordering direction of a column (must be selected when creating the
    index).
-   Use a custom iterator (eg: treat a string column like a IP, or sort by the
    value of "config" key in a map).

For querying the index the user must create a cursor. That cursor points to a
position in the sorted data structure. With that, the user can perform lookups
(by key) and/or get the following rows. The user can also compare the current
value of the cursor to a record.

For faster lookups, user would need to provide a key which will be used for finding
the specific rows that meet this criteria. This key could be an IP address, a
MAC address, an ACL rule, etc. When the information is found in the data
structure the user's cursor is updated to point to the row. If several rows
match the query then the user can get easily the next row updating the cursor.

For accessing data in lexicographic order, the user can use the ranged iterators.
Those iterators needs a cursor, and a "from" and "to" value.

One of the potential issues of this solution is the memory consumption of the
new data structures. However, since it will only contain pointers, it's not
expected that it consumes too much memory.

Another potential issue is the time needed to create the data structure and the
time needed to add/remove elements. The indexes are always synchronized with the
replica. For this reason it must be important that the comparison functions
(built-in and user provided) are FAST. However, these operations are not as
common as looking up for data, so it's not expected these operations affects the
system significatively.

At this point, a skiplist is the data structure selected as the best fit.

It's important to mention that all changes will be done in the IDL. There are no
changes to the OVSDB server or the OVSDB engine.

                     +---------------------------------------------------------+
                     |                                                         |
      +-----------------+Client changes to data                            IDL |
      |              |                                                         |
  +---v---+          |                                                         |
  | OVSDB +------------->OVSDB Notification                                    |
  +-------+          |   +                                                     |
                     |   |   +------------+                                    |
                     |   |   |            |                                    |
                     |   |   | Insert Row +----> Insert row to indexes         |
                     |   |   |            |                   ^                |
                     |   +-> | Modify Row +-------------------+                |
                     |       |            |                   v                |
                     |       | Delete Row +----> Delete row from indexes       |
                     |       |            |                                    |
                     |       +----+-------+                                    |
                     |            |                                            |
                     |            +-> IDL Replica                              |
                     |                                                         |
                     +---------------------------------------------------------+

## C IDL API

This functionality is going to be implemented using the skiplist. A skiplist is
a datastructure that offers log( n ) retrieval/insertions/deletions, and O(1)
"find next" operations.

To implement the indexes in the C IDL the following changes in the IDL are going
to be made:

-   Create a special function `int comparator(void*, void*)` per column, that
    allows to compare two ovsrec structs (by that column).This function is
    created only for the columns with type string, int or real. Each column has
    a pointer to this function (or NULL).
-   Each table, has a hash table with the indexes (keyed by index name).

### Indexes

The indexes are inserted in a hash table in each table in the IDL. This allow to
specify any number of indexes per table, with a custom collection of columns.

    /* Definition of the index's struct. It's a opaque type. */
    struct ovsdb_idl_index {
        struct skiplist *skiplist;
        const struct ovsdb_idl_column **columns;
        column_comparator *comparers;
        int *sorting_order;
        size_t n_columns;
        bool row_sync;
    };

### Cursors

The queries are going to be made with a cursor. A cursor is a struct that
contains the current ovsrec, current node (on the skiplist) and memory allocated
to save temporal records (needed by the comparator).

    /* Definition of the cursor structure. */
    struct ovsdb_idl_index_cursor {
        struct ovsdb_idl_index *index;
        struct skiplist_node *position;
    };

## API

### Index Creation

    struct ovsdb_idl_index *ovsdb_idl_create_index(
        struct ovsdb_idl *idl,
        const struct ovsdb_idl_table_class *tc,
        const char *index_name
    );

Creates an index in a table. The columns must be configured afterwards. The
returned pointer doesn't need to be saved anywhere, except until all the index's
columns had been inserted.

    void ovsdb_idl_index_add_column(struct ovsdb_idl_index *,
                               const struct ovsdb_idl_column *,
                               int order,
                               column_comparator custom_comparer
                               );

Allows to add a column to an existing index. If the column has a default
comparator then the custom comparator can be NULL, otherwise a custom comparator
must be passed.

#### Index Creation Example

    /* Custom comparator for the column stringField at table Test */
    int stringField_comparator(const void *a, const void *b) {
        struct ovsrec_test *AAA, *BBB;
        AAA = (struct ovsrec_test *)a;
        BBB = (struct ovsrec_test *)b;
        return strcmp(AAA->stringField, BBB->stringField);
    }

    void init_idl(struct ovsdb_idl **, char *remote) {
        /* Add the columns to the IDL */
        *idl = ovsdb_idl_create(remote, &ovsrec_idl_class, false, true);
        ovsdb_idl_add_table(*idl, &ovsrec_table_test);
        ovsdb_idl_add_column(*idl, &ovsrec_test_col_stringField);
        ovsdb_idl_add_column(*idl, &ovsrec_test_col_numericField);
        ovsdb_idl_add_column(*idl, &ovsrec_test_col_enumField);
        ovsdb_idl_add_column(*idl, &ovsrec_test_col_boolField);

        /* Create a index
         * This index is created using (stringField, numericField) as key. Also shows the usage
         * of some arguments of add column, althought for a string column is unnecesary to pass
         * a custom comparator.
         */
        struct ovsdb_idl_index *index;
        index = ovsdb_idl_create_index(*idl, &ovsrec_table_test, "by_stringField");
        ovsdb_idl_index_add_column(index, &ovsrec_test_col_stringField, OVSDB_INDEX_ASC, stringField_comparator);
        ovsdb_idl_index_add_column(index, &ovsrec_test_col_numericField, OVSDB_INDEX_DESC, NULL);
        /* Done. */
    }

## Indexes Querying

### Iterators

The recommended way to do queries is using a "ranged foreach", an "equal
foreach" or a "full foreach" over an index. The mechanism works as follow:

1) Create a cursor 2) Pass the cursor, a row (ovsrec_...) and the values to the
iterator 3) Use the values

To create the cursor use the following code:

    ovsdb_idl_index_cursor my_cursor;
    ovsdb_idl_initialize_cursor(idl, &ovsrec_table_test, "by_stringField", &my_cursor);

Then that cursor can be used to do additional queries. The library implements
three different iterators: a range iterator, an equal iterator and iterator
over all the index. The range iterator receives two values and iterates over
all the records that are within that range (including both). The equal iterator
only iterates over the records that exactly match the value passed. The full
iterator iterates over all the rows in the index, in order.

Note that the index are *sorted by the "concatenation" of the values in each
indexed column*, so the ranged iterators returns all the values between
"from.col1 from.col2 ... from.coln" and "to.col1 to.col2 ... to.coln", *NOT
the rows with a value in column 1 between from.col1 and to.col1, and so on*.

The iterators are macros especific to each table. To use those iterators
consider the following code:

    /* Equal Iterator
     * Iterates over all the records equal to value (by the indexed value)
     */
    ovsrec_test *record;
    ovsrec_test value;
    value.stringField = "hello world";
    OVSREC_TEST_FOR_EACH_EQUAL(record, &my_cursor, &value) {
        /* Can return zero, one or more records */
        assert(strcmp(record->stringField, "hello world") == 0);
        printf("Found one record with %s", record->stringField);
    }

    /*
     * Ranged iterator
     * Iterates over all the records between two values (including both)
     */
    ovsrec_test value_from, value_to;
    value_from.stringField = "aaa";
    value_from.stringField = "mmm";
    OVSREC_TEST_FOR_EACH_RANGE(record, &my_cursor, &value_from, &value_to) {
        /* Can return zero, one or more records */
        assert(strcmp("aaa", record->stringField) <= 0);
        assert(strcmp(record->stringField, "mmm") <= 0);
        printf("Found one record with %s", record->stringField);
    }

    /*
     * Iterator over all the index
     * Iterates over all the records in the index
     */
    OVSREC_TEST_FOR_EACH_BYINDEX(record, &my_cursor) {
        /* Can return zero, one or more records */
        printf("Found one record with %s", record->stringField);
    }

### General Index Access

Although the iterators allow many use cases eventually thay may not fit some. In
that case the indexes can be queried by a more general API. In fact, the
iterators were built over that functions.

    int ovsrec_<table>_index_compare(struct ovsdb_idl_index_cursor *, const struct ovsrec_<table> *, const struct ovsrec_<table> *)

`ovsrec_<table>_index_compare` compares two rows using the same comparator used
in the cursor's index. The returned value is the same as strcmp, but defines a
specific behaviour when comparing pointers to NULL (NULL is always greater than
any other value, but when comparing NULL against NULL by definition return 1).

    const struct ovsrec_<table> *ovsrec_<table>_index_first(struct ovsdb_idl_index_cursor *)

`ovsrec_<table>_index_next` moves the cursor to the first record in the index,
and return the replica's pointer to that row.

    const struct ovsrec_<table> *ovsrec_<table>_index_next(struct ovsdb_idl_index_cursor *)

`ovsrec_<table>_index_next` moves the cursor to the next record in the index,
and return the replica's pointer to that row. If the cursor was in the last row
(or was already NULL) then returns NULL.

    const struct ovsrec_<table> *ovsrec_<table>_index_find(struct ovsdb_idl_index_cursor *, const struct ovsrec_<table> *)

`ovsrec_<table>_index_find` moves the cursor to the first record in the index
that matches (by the index comparator) the given value, or NULL if none found.

    const struct ovsrec_<table> *ovsrec_<table>_index_forward_to(struct ovsdb_idl_index_cursor *, const struct ovsrec_<table> *)

`ovsrec_<table>_index_forward_to` moves the cursor to the first record in the
index equal or greater than (by the index comparator) the given value, or NULL
if none found.

    const struct ovsrec_<table> *ovsrec_<table>_index_get_data(const struct ovsdb_idl_index_cursor *)

`ovsrec_<table>_index_get_data` returns a pointer to the replica's row that is
pointed by the cursor, or NULL.
