# lu_sqlite_vtable

SQLite vtable extension for loading fdb files and associated utils

With the code in this repo it is possible to load a LU .fdb file into sqlite and run SQL against it, including limited `UPDATE` support.

There is no `DELETE` or `INSERT` support yet, and `UPDATE` for strings is limited to strings of length less or equal to the original string.

There is also no support for string indices yet, but this shouldn't matter much since there aren't many string-indexed tables and they are fairly small.

## Live LU client memory DB viewing and editing

Windows: Use `cdclient_shell.vcxproj` to build a LU mod DLL which you can load using [mod_loader](https://github.com/lcdr/mod_loader) or other loaders. Note: when using mod_loader rename the file to `mod.dll` first and follow the instructions in the mod_loader repo.

## SQLite extension for use with an existing SQLite shell or GUI

Note: For LU client compatibility the binaries are built in 32 bit mode, you may need to install additional packages for building and a 32-bit version of sqlite3/SQLiteStudio for loading.

Windows: Use `fdb.vcxproj`
Linux: Use `make_sqlite_extension.sh`

to build an SQLite extension dll.

Load into a `sqlite3` command line shell using `.load fdb`. SQLiteStudio also has [support](https://github.com/pawelsalawa/sqlitestudio/wiki/User_Manual#sqlite-extensions).
