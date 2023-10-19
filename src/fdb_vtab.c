#if !defined(SQLITEINT_H)
#include "sqlite3/sqlite3ext.h"
#endif
SQLITE_EXTENSION_INIT1
#include "fdb.c"
#include <string.h>
#include <stdint.h>
#include <stdio.h>

/* fdb_vtab is a subclass of sqlite3_vtab which is
** underlying representation of the virtual table
*/
typedef struct fdb_vtab fdb_vtab;
struct fdb_vtab {
	sqlite3_vtab base;	/* Base class - must be first */
	/* Add new fields here, as necessary */
	Table* table;
};

/* fdb_cursor is a subclass of sqlite3_vtab_cursor which will
** serve as the underlying representation of a cursor that scans
** over rows of the result
*/
typedef struct fdb_cursor fdb_cursor;
struct fdb_cursor {
	sqlite3_vtab_cursor base;  /* Base class - must be first */
	/* Add new fields here, as necessary */
	Table* table;
	uint64_t bucketIndex;
	uint64_t stopIndex;
	Bucket* curBucket;
};

const char* SQLITE_TYPE[9] = {"none", "int32", "uint32", "real", "text_4", "int_bool", "int64", "uint64", "text_8"};

/*
** The fdbConnect() method is invoked to create a new
** template virtual table.
**
** Think of this routine as the constructor for fdb_vtab objects.
**
** All this routine needs to do is:
**
**		(1) Allocate the fdb_vtab object and initialize all fields.
**
**		(2) Tell SQLite (via the sqlite3_declare_vtab() interface) what the
**				result set of queries against the virtual table will look like.
*/
static int fdbConnect(
	sqlite3 *db,
	void *pAux,
	int argc, const char *const*argv,
	sqlite3_vtab **ppVtab,
	char **pzErr
){
	if (argc < 1) {
		return SQLITE_ERROR;
	}

  Fdb* fdb = pAux;

	for (uint32_t i = 0; i < fdb->ntables; i++) {
		TableDescription* desc = fdb->tables[i].desc;

		if (strcmp(desc->name, argv[0]) != 0) {
			continue;
		}

		char declaration[8192];
		sprintf(declaration, "CREATE TABLE x(");

		for (uint32_t j = 0; j < desc->ncolumns; j++) {
			uint32_t data_type = desc->columns[j].data_type;
			if (data_type > 8) {
				return SQLITE_ERROR;
			}
			sprintf(declaration+strlen(declaration), "'%s' %s,", desc->columns[j].name, SQLITE_TYPE[data_type]);
		}
		sprintf(declaration+strlen(declaration)-(desc->ncolumns > 0), ")");

		int rc = sqlite3_declare_vtab(db, declaration);
		//printf("create table statement: %s, rc: %i\n", declaration, rc);
		fdb_vtab *pNew;

		if(rc == SQLITE_OK) {
			pNew = sqlite3_malloc(sizeof(*pNew));
			*ppVtab = (sqlite3_vtab*)pNew;
			if (pNew == NULL) {
				return SQLITE_NOMEM;
			}
			memset(pNew, 0, sizeof(*pNew));
			pNew->table = &fdb->tables[i];
		}
		return rc;
  }

  return SQLITE_ERROR;
}

/*
** This method is the destructor for fdb_vtab objects.
*/
static int fdbDisconnect(sqlite3_vtab *pVtab) {
	//printf("Disconnect!\n");
	fdb_vtab *p = (fdb_vtab*)pVtab;
	sqlite3_free(p);
	return SQLITE_OK;
}

/*
** Constructor for a new fdb_cursor object.
*/
static int fdbOpen(sqlite3_vtab* pVtab, sqlite3_vtab_cursor** ppCursor) {
	fdb_vtab *p = (fdb_vtab*)pVtab;
	//printf("%s Open!\n", p->table->desc->name);
	fdb_cursor *pCur;
	pCur = sqlite3_malloc(sizeof(*pCur));
	if (pCur == NULL) return SQLITE_NOMEM;
	memset(pCur, 0, sizeof(*pCur));
	*ppCursor = &pCur->base;
	pCur->table = p->table;
	return SQLITE_OK;
}

/*
** Destructor for a fdb_cursor.
*/
static int fdbClose(sqlite3_vtab_cursor *cur) {
	fdb_cursor *pCur = (fdb_cursor*)cur;
	//printf("%s Close!\n", pCur->table->desc->name);
	sqlite3_free(pCur);
	return SQLITE_OK;
}

/*
** Advance a fdb_cursor to its next row of output.
*/
static int fdbNext(sqlite3_vtab_cursor *cur) {
	fdb_cursor *pCur = (fdb_cursor*)cur;
	//printf("\n%s Next! bucketIndex %lli curBucket %i\n", pCur->table->desc->name, pCur->bucketIndex, (uint32_t) pCur->curBucket);

	if (pCur->curBucket != NULL && pCur->curBucket->next != NULL) {
		pCur->curBucket = pCur->curBucket->next;
	} else {
		pCur->curBucket = NULL;
		uint32_t nbuckets = pCur->table->hash_table->nbuckets;
		// fast forward the cursor to the first valid bucket
		for (uint64_t i = pCur->bucketIndex+1; i < pCur->stopIndex; i++) {
			Bucket* bucket = pCur->table->hash_table->buckets[i % nbuckets];
			if (bucket == NULL) {
				//printf("Skipping empty bucket %lli\n", i);
				continue;
			}
			pCur->curBucket = bucket;
			pCur->bucketIndex = i;
			break;
		}
		// fast forwarding skipped the entire remaining table, mark as EOF
		if (pCur->curBucket == NULL) {
			pCur->bucketIndex = pCur->stopIndex;
		}
	}
	return SQLITE_OK;
}

/*
** Return values of columns for the row at which the fdb_cursor
** is currently pointing.
*/
static int fdbColumn(
	sqlite3_vtab_cursor *cur,	 /* The cursor */
	sqlite3_context *ctx,			 /* First argument to sqlite3_result_...() */
	int i											 /* Which column to return */
) {
	if (sqlite3_vtab_nochange(ctx)) {
		return SQLITE_OK;
	}

	fdb_cursor *pCur = (fdb_cursor*)cur;

	Value value = pCur->curBucket->row->values[i];
	switch(value.data_type) {
		case FDB_NULL:
			//printf("| NULL ");
			sqlite3_result_null(ctx);
			break;
		case FDB_I32:
			//printf("| %i ", value.value.i32);
			sqlite3_result_int(ctx, value.value.i32);
			break;
		case FDB_U32:
			//printf("| %i ", value.value.u32);
			sqlite3_result_int(ctx, value.value.u32);
			break;
		case FDB_REAL:
			//printf("| %f ", value.value.real);
			sqlite3_result_double(ctx, (double) value.value.real);
			break;
		case FDB_BOOLEAN:
			//printf("| %i ", value.value.boolean);
			sqlite3_result_int(ctx, value.value.boolean);
			break;
		case FDB_I64:
			//printf("| %lli ", *value.value.i64p);
			sqlite3_result_int64(ctx, *value.value.i64p);
			break;
		case FDB_U64:
			//printf("| %lli ", *value.value.u64p);
			sqlite3_result_int64(ctx, *value.value.u64p);
			break;
		case FDB_NVARCHAR:
		case FDB_TEXT:
			//printf("| %s ", value.value.text);
			sqlite3_result_text(ctx, value.value.text, -1, SQLITE_STATIC);
			break;
		default:
			return SQLITE_ERROR;
			break;
	}
	return SQLITE_OK;
}

#ifdef __GNUC__
#define ctz(x) __builtin_ctz(x)
#endif
#ifdef _MSC_VER
#include <intrin.h>
uint32_t __inline ctz(uint32_t value) {
	uint32_t trailing_zero = 0;
	if (_BitScanForward(&trailing_zero, value)) return trailing_zero;
	return 0;
}
#endif

/*
** Return the rowid for the current row.	In this implementation, the
** rowid is the same as the output value.
*/
static int fdbRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid){
	fdb_cursor *pCur = (fdb_cursor*)cur;
	uint32_t nbuckets = pCur->table->hash_table->nbuckets;
	uint32_t nbits = ctz(nbuckets);
	uint32_t i = 0;
	Bucket* bucket = pCur->table->hash_table->buckets[pCur->bucketIndex % nbuckets];
	while (bucket != pCur->curBucket) {
		i += 1;
		bucket = bucket->next;
	}
	sqlite3_int64 rowid = pCur->bucketIndex % nbuckets;
	rowid |= (i << nbits);
	*pRowid = rowid;
	//printf("Rowid = %lli\n", rowid);
	return SQLITE_OK;
}

/*
** Return TRUE if the cursor has been moved off of the last
** row of output.
*/
static int fdbEof(sqlite3_vtab_cursor *cur){
	fdb_cursor *pCur = (fdb_cursor*)cur;
	//printf("%s eof! bucketIndex %lli, stopIndex %lli, is eof? %i\n", pCur->table->desc->name, pCur->bucketIndex, pCur->stopIndex, pCur->bucketIndex == pCur->stopIndex);
	return pCur->bucketIndex == pCur->stopIndex;
}

void print_sqlite3_value(sqlite3_value* value) {
	if (value == NULL) {
		//printf("NULL POINTER\n");
		return;
	}
	switch (sqlite3_value_type(value)) {
		case SQLITE_NULL:
			//printf("NULL");
			break;
		case SQLITE_INTEGER:
			//printf("%lli", sqlite3_value_int64(value));
			break;
		case SQLITE_FLOAT:
			//printf("%f", sqlite3_value_double(value));
			break;
		case SQLITE_TEXT:
			//printf("%s", sqlite3_value_text(value));
			break;
		default:
			//printf("unknown value type %i", sqlite3_value_type(value));
			break;
	}
}

/*
** This method is called to "rewind" the fdb_cursor object back
** to the first row of output.	This method is always called at least
** once prior to any call to fdbColumn() or fdbRowid() or
** fdbEof().
*/
static int fdbFilter(
	sqlite3_vtab_cursor *pVtabCursor,
	int idxNum, const char *idxStr,
	int argc, sqlite3_value **argv
){
	fdb_cursor *pCur = (fdb_cursor *)pVtabCursor;

	//printf("%s Filter! idxNum: %i, idxStr: %s, argc: %i\n", pCur->table->desc->name, idxNum, idxStr, argc);

	for (int32_t i = 0; i < argc; i++) {
		char op = idxStr[i];
		if (op == SQLITE_INDEX_CONSTRAINT_LT) {
			//printf("<");
		} else if (op == SQLITE_INDEX_CONSTRAINT_LE) {
			//printf("<=");
		} else if (op == SQLITE_INDEX_CONSTRAINT_EQ) {
			//printf("=");
		} else if (op == SQLITE_INDEX_CONSTRAINT_GE) {
			//printf(">=");
		} else if (op == SQLITE_INDEX_CONSTRAINT_GT) {
			//printf(">");
		} else {
			//printf("UNKNOWN OP");
		}
		//printf(" ");
		print_sqlite3_value(argv[i]);
		//printf("\n");
	}

	// find min and max of the range to consider

	int64_t min = INT64_MIN;
	int64_t max = INT64_MAX;

	for (int32_t i = 0; i < argc; i++) {
		char op = idxStr[i];
		long long value = sqlite3_value_int64(argv[i]);
		switch (op) {
			case SQLITE_INDEX_CONSTRAINT_GT:
				if (min < value+1) {
					min = value+1;
				}
				break;
			case SQLITE_INDEX_CONSTRAINT_GE:
				if (min < value) {
					min = value;
				}
				break;
			case SQLITE_INDEX_CONSTRAINT_EQ:
				min = value;
				max = value+1;
				break;
			case SQLITE_INDEX_CONSTRAINT_LE:
				if (max > value+1) {
					max = value+1;
				}
				break;
			case SQLITE_INDEX_CONSTRAINT_LT:
				if (max > value) {
					max = value;
				}
				break;
			default:
				//printf("OPERATOR %i NOT HANDLED\n", op);
				return SQLITE_ERROR;
		}
	}

	// nonsensical range
	if (max < min) {
		// this forces EOF to immediately return true
		max = min;
		return SQLITE_OK;
	}

	uint32_t nbuckets = pCur->table->hash_table->nbuckets;
	uint64_t span = max - min;
	//printf("filter arrived at a range of [%lli, %lli), max - min: %lli, max - min < nbuckets %i\n", min, max, span, span < nbuckets);

	if (span < nbuckets) {
		// min and max are close enough not to span the entire table
		// so only fetch that range
		//printf("partial table span!\n");
		pCur->bucketIndex = min,
		pCur->stopIndex = max;
	} else {
		// min and max are apart far enough
		// that we will be traversing the entire table anyway
		// so no filtering necessary
		//printf("whole table span!\n");
		pCur->bucketIndex = 0;
		pCur->stopIndex = nbuckets;
	}

	//printf("filter set bucketIndex, stopIndex to [%lli, %lli)\n", pCur->bucketIndex, pCur->stopIndex);

	// step back by one to counter the call to fdbNext()
	pCur->bucketIndex -= 1;
	pCur->curBucket = NULL;
	// fast forward the cursor to the first valid bucket
	return fdbNext(pVtabCursor);
}

/*
** SQLite will invoke this method one or more times while planning a query
** that uses the virtual table.	This routine needs to create
** a query plan for each invocation and compute an estimated cost for that
** plan.
*/
static int fdbBestIndex(
	sqlite3_vtab* tab,
	sqlite3_index_info* pIdxInfo
){
	fdb_vtab *pVtab = (fdb_vtab*)tab;

	//printf("%s BestIndex! nConstraint %i\n", pVtab->table->desc->name, pIdxInfo->nConstraint);

	uint32_t curIndex = 0;

	for (int32_t i = 0; i < pIdxInfo->nConstraint; i++) {
		struct sqlite3_index_constraint cons = pIdxInfo->aConstraint[i];
		sqlite3_value* value;
		sqlite3_vtab_rhs_value(pIdxInfo, i, &value);
		//printf("\tis_usable %i: [column %i] ", cons.usable, cons.iColumn);

		uint32_t op =  cons.op;
		if (op == SQLITE_INDEX_CONSTRAINT_LT) {
			//printf("<");
		} else if (op == SQLITE_INDEX_CONSTRAINT_LE) {
			//printf("<=");
		} else if (op == SQLITE_INDEX_CONSTRAINT_EQ) {
			//printf("=");
		} else if (op == SQLITE_INDEX_CONSTRAINT_GE) {
			//printf(">=");
		} else if (op == SQLITE_INDEX_CONSTRAINT_GT) {
			//printf(">");
		} else if (op == SQLITE_INDEX_CONSTRAINT_ISNULL) {
			//printf("IS NULL\n");
		} else if (op == SQLITE_INDEX_CONSTRAINT_ISNOTNULL) {
			//printf("IS NOT NULL\n");
		} else {
			//printf("UNKNOWN OP %i\n", op);
		}
		print_sqlite3_value(value);
		//printf("\n");

		if (cons.usable && cons.iColumn == 0 && (
			// TODO: support string indexes sometime
			   pVtab->table->desc->columns[0].data_type != FDB_NVARCHAR
			&& pVtab->table->desc->columns[0].data_type != FDB_TEXT
		) && (
			   op == SQLITE_INDEX_CONSTRAINT_LT
			|| op == SQLITE_INDEX_CONSTRAINT_LE
			|| op == SQLITE_INDEX_CONSTRAINT_EQ
			|| op == SQLITE_INDEX_CONSTRAINT_GE
			|| op == SQLITE_INDEX_CONSTRAINT_GT
		)) {
			curIndex += 1;
			pIdxInfo->aConstraintUsage[i].argvIndex = curIndex;
		}
	}

	if (curIndex == 0) {
		pIdxInfo->estimatedCost = (double) pVtab->table->hash_table->nbuckets;
	} else {
		pIdxInfo->estimatedCost = (double) 1;
		pIdxInfo->idxStr = sqlite3_malloc(curIndex);
		pIdxInfo->needToFreeIdxStr = true;

		for (int32_t i = 0; i < pIdxInfo->nConstraint; i++) {
			uint32_t argvIndex = pIdxInfo->aConstraintUsage[i].argvIndex;
			if (argvIndex != 0) {
				pIdxInfo->idxStr[argvIndex-1] = pIdxInfo->aConstraint[i].op;
			}
		}
		pIdxInfo->idxStr[curIndex] = 0; // terminator
	}
	return SQLITE_OK;
}

int fdbUpdate(
  sqlite3_vtab *tab,
  int argc,
  sqlite3_value **argv,
  sqlite_int64 *pRowid
) {
	//printf("Update!\n");
	fdb_vtab *pVtab = (fdb_vtab*)tab;

	for (int32_t i = 0; i < argc; i++) {
		//printf(" | ");
		print_sqlite3_value(argv[i]);
	}
	//printf("\n");

	if (argc > 1 && sqlite3_value_type(argv[0]) != SQLITE_NULL) {
		// UPDATE
		int64_t rowid = sqlite3_value_int64(argv[0]);
		if (rowid > UINT32_MAX) return SQLITE_RANGE;
		uint32_t nbuckets = pVtab->table->hash_table->nbuckets;
		uint32_t bucketIndex = rowid & (nbuckets - 1);
		uint32_t rowIndex = (uint32_t) rowid >> (ctz(nbuckets));
		Bucket* bucket = pVtab->table->hash_table->buckets[bucketIndex];
		for (uint32_t i = 0; i < rowIndex; i++) {
			bucket = bucket->next;
		}
		Row* row = bucket->row;
		//printf("got row %i\n", (uint32_t) row);
		for (int32_t i = 2; i < argc; i++) {
			if (sqlite3_value_nochange(argv[i])) {
				continue;
			}
			//printf(" | %s: ", pVtab->table->desc->columns[i-2].name);
			print_sqlite3_value(argv[i]);
			uint32_t sqlite3_type = sqlite3_value_type(argv[i]);

			switch (row->values[i-2].data_type) {
				case FDB_I32: {
					if (sqlite3_type != SQLITE_INTEGER) {
						return SQLITE_CONSTRAINT_DATATYPE;
					}
					int64_t value = sqlite3_value_int64(argv[i]);
					if (value < INT32_MIN || value > INT32_MAX) {
						return SQLITE_RANGE;
					}
					row->values[i-2].value.i32 = (int32_t) value;
					break; }

				case FDB_U32: {
					if (sqlite3_type != SQLITE_INTEGER) {
						return SQLITE_CONSTRAINT_DATATYPE;
					}
					int64_t value = sqlite3_value_int64(argv[i]);
					if (value < 0 || value > UINT32_MAX) {
						return SQLITE_RANGE;
					}
					row->values[i-2].value.u32 = (uint32_t) value;
					break; }

				case FDB_REAL: {
					if (sqlite3_type != SQLITE_FLOAT) {
						return SQLITE_CONSTRAINT_DATATYPE;
					}
					double value = sqlite3_value_double(argv[i]);
					row->values[i-2].value.real = (float) value;
					break; }

				case FDB_NVARCHAR:
				case FDB_TEXT: {
					if (sqlite3_type != SQLITE_TEXT) {
						return SQLITE_CONSTRAINT_DATATYPE;
					}
					const char* value = (const char*) sqlite3_value_text(argv[i]);
					if (strlen(value) > strlen(row->values[i-2].value.text)) {
						return SQLITE_TOOBIG;
					}
					strcpy(row->values[i-2].value.text, value);
					break; }

				case FDB_BOOLEAN: {
					if (sqlite3_type != SQLITE_INTEGER) {
						return SQLITE_CONSTRAINT_DATATYPE;
					}
					int64_t value = sqlite3_value_int64(argv[i]);
					if (value != 0 && value != 1) {
						return SQLITE_RANGE;
					}
					row->values[i-2].value.boolean = (bool) value;
					break; }

				case FDB_I64: {
					if (sqlite3_type != SQLITE_INTEGER) {
						return SQLITE_CONSTRAINT_DATATYPE;
					}
					int64_t value = sqlite3_value_int64(argv[i]);
					*row->values[i-2].value.i64p = value;
					break; }

				case FDB_U64: {
					if (sqlite3_type != SQLITE_INTEGER) {
						return SQLITE_CONSTRAINT_DATATYPE;
					}
					int64_t value = sqlite3_value_int64(argv[i]);
					*row->values[i-2].value.u64p = value;
					break; }

				default:
					//printf("Unexpected FDB data type");
			}
		}
		//printf("\n");
		return SQLITE_OK;
	}
	return SQLITE_ERROR;
}

/*
** This following structure defines all the methods for the
** virtual table.
*/
static sqlite3_module fdbModule = {
	/* iVersion		*/ 0,
	/* xCreate		 */ 0,
	/* xConnect		*/ fdbConnect,
	/* xBestIndex	*/ fdbBestIndex,
	/* xDisconnect */ fdbDisconnect,
	/* xDestroy		*/ 0,
	/* xOpen			 */ fdbOpen,
	/* xClose			*/ fdbClose,
	/* xFilter		 */ fdbFilter,
	/* xNext			 */ fdbNext,
	/* xEof				*/ fdbEof,
	/* xColumn		 */ fdbColumn,
	/* xRowid			*/ fdbRowid,
	/* xUpdate		 */ fdbUpdate,
	/* xBegin			*/ 0,
	/* xSync			 */ 0,
	/* xCommit		 */ 0,
	/* xRollback	 */ 0,
	/* xFindMethod */ 0,
	/* xRename		 */ 0,
	/* xSavepoint	*/ 0,
	/* xRelease		*/ 0,
	/* xRollbackTo */ 0,
	/* xShadowName */ 0
};
