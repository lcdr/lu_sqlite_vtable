gcc -shared -g -m32 -fPIC -DSQLITE_CORE -DSQLITE_CUSTOM_INCLUDE=../fdb_shell.h src/main.c src/sqlite3/shell.c src/sqlite3/sqlite3.c -lpthread -ldl -lm -o cdclient_shell.so
