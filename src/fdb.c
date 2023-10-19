#include <stdbool.h>
#include <stddef.h>

typedef struct {
	unsigned int data_type;
	char* name;
} Column;

void fix_pointers_column(Column* col, unsigned int fdb) {
	col->name = (char*) (fdb + (unsigned int) col->name);
}

typedef struct {
	unsigned int ncolumns;
	char* name;
	Column* columns;
} TableDescription;

void fix_pointers_table_desc(TableDescription* desc, unsigned int fdb) {
	desc->name = (char*) (fdb + (unsigned int) desc->name);
	desc->columns = (Column*) (fdb + (unsigned int) desc->columns);
	for (unsigned int i = 0; i < desc->ncolumns; i++) {
		fix_pointers_column(&desc->columns[i], fdb);
	}
}

enum fdb_data_type {
	FDB_NULL = 0,
	FDB_I32 = 1,
	FDB_U32 = 2,
	FDB_REAL = 3,
	FDB_NVARCHAR = 4,
	FDB_BOOLEAN = 5,
	FDB_I64 = 6,
	FDB_U64 = 7,
	FDB_TEXT = 8,
} fdb_data_type;

typedef struct {
	unsigned int data_type;
	union value {
		int i32;
		unsigned int u32;
		float real;
		bool boolean;
		long long* i64p;
		unsigned long long* u64p;
		char* text;
	} value;
} Value;

void fix_pointers_value(Value* value, unsigned int fdb) {
	if (value->data_type == FDB_I64 || value->data_type == FDB_U64 || value->data_type == FDB_NVARCHAR || value->data_type == FDB_TEXT) {
		value->value.u32 = (fdb + (unsigned int) value->value.u32);
	}
}

typedef struct {
	unsigned int nvalues;
	Value* values;
} Row;

void fix_pointers_row(Row* row, unsigned int fdb) {
	row->values = (Value*) (fdb + (unsigned int) row->values);
	for (unsigned int i = 0; i < row->nvalues; i++) {
		fix_pointers_value(&row->values[i], fdb);
	}
}

typedef struct Bucket {
	Row* row;
	struct Bucket* next;
} Bucket;

void fix_pointers_bucket(Bucket* bucket, unsigned int fdb) {
	bucket->row = (Row*) (fdb + (unsigned int) bucket->row);
	fix_pointers_row(bucket->row, fdb);

	if ((int) bucket->next == -1) {
		bucket->next = NULL;
	} else {
		bucket->next = (Bucket*) (fdb + (unsigned int) bucket->next);
		fix_pointers_bucket(bucket->next, fdb);
	}
}

typedef struct {
	unsigned int nbuckets;
	Bucket** buckets;
} HashTable;

void fix_pointers_hash_table(HashTable* hash_table, unsigned int fdb) {
	hash_table->buckets = (Bucket**) (fdb + (unsigned int) hash_table->buckets);
	for (unsigned int i = 0; i < hash_table->nbuckets; i++) {
		if ((int) hash_table->buckets[i] == -1) {
			hash_table->buckets[i] = NULL;
		} else {
			hash_table->buckets[i] = (Bucket*) (fdb + (unsigned int) hash_table->buckets[i]);
			fix_pointers_bucket(hash_table->buckets[i], fdb);
		}
	}
}

typedef struct {
	TableDescription* desc;
	HashTable* hash_table;
} Table;

void fix_pointers_table(Table* table, unsigned int fdb) {
	table->desc = (TableDescription*) (fdb + (unsigned int) table->desc);
	table->hash_table = (HashTable*) (fdb + (unsigned int) table->hash_table);
	fix_pointers_table_desc(table->desc, fdb);
	fix_pointers_hash_table(table->hash_table, fdb);
}

typedef struct {
	unsigned int ntables;
	Table* tables;
} Fdb;

void fix_pointers(Fdb* fdb) {
	fdb->tables = (Table*) ((unsigned int) fdb + (unsigned int) fdb->tables);
	for (unsigned int i = 0; i < fdb->ntables; i++) {
		fix_pointers_table(&fdb->tables[i], (unsigned int) fdb);
	}
}
