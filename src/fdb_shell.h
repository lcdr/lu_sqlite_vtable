#ifndef FDB_SHELL_H
#define FDB_SHELL_H
// to shut the compiler up
int sqlite3_fdb_init(void* db, void **pzErrMsg,	void *pApi);
#define SQLITE_SHELL_EXTFUNCS FDB
/*
** Define some macros to allow this extension to be built into the shell
** conveniently, in conjunction with use of SQLITE_SHELL_EXTFUNCS. This
** allows shell.c, as distributed, to have this extension built in.
*/
#define FDB_INIT(db) sqlite3_fdb_init(db, 0, 0)
#define FDB_EXPOSE(db, pzErr) /* Not needed, ..._init() does this. */
#endif
