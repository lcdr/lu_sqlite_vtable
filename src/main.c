#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#include <libloaderapi.h>
#endif

#include "fdb_vtab.c"
#include <stdlib.h>

Fdb* get_fdb_from_file(const char* path) {
	FILE* fdbfile = fopen(path, "rb");
	if (fdbfile == NULL) {
		return NULL;
	}
	fseek(fdbfile, 0, SEEK_END);
	long fsize = ftell(fdbfile);
	fseek(fdbfile, 0, SEEK_SET);

	unsigned char* buf = malloc(fsize + 1);
	if (buf == NULL) {
		return NULL;
	}

	size_t nread = fread(buf, 1, fsize, fdbfile);
	fclose(fdbfile);
	if (nread != fsize) {
		return NULL;
	}

	Fdb* fdb = (Fdb*) buf;
	fix_pointers(fdb);
	return fdb;
}

Fdb* get_fdb_from_legouniverse_exe() {
	#ifdef _WIN32
		const unsigned int FDB_PTR_ADDR = 0x014897DC;
		unsigned int base = (unsigned int) GetModuleHandle(NULL);
		Fdb* fdb = ***(Fdb****) (base + FDB_PTR_ADDR);
		return fdb;
	#else
		return NULL;
	#endif
}

#ifdef _WIN32
__declspec(dllexport)
#endif
int sqlite3_fdb_init(
	sqlite3 *db,
	char **pzErrMsg,
	const sqlite3_api_routines *pApi
){
	SQLITE_EXTENSION_INIT2(pApi);

	Fdb* fdb = get_fdb_from_legouniverse_exe();
	if (fdb == NULL) {
		return SQLITE_ERROR;
	}

	int rc = SQLITE_OK;

	for (unsigned int i = 0; i < fdb->ntables; i++) {
		rc = sqlite3_create_module(db, fdb->tables[i].desc->name, &fdbModule, fdb);
		if (rc != SQLITE_OK) {
			return rc;
		}
	}
	return rc;
}

#ifdef CDCLIENT_SHELL
#ifdef _WIN32
DWORD WINAPI ShellThread(void* data) {
	printf("cdclient shell loaded. In order to avoid interruptions, the client's own output will not be printed here, see the logfile for that.\n");
	while (true) {
		const unsigned int argc = 2;
		#if SQLITE_SHELL_IS_UTF8
		char* argv[2] = {"sqlite3", "-table"};
		main(argc, argv);
		#else
		int wmain(int argc, wchar_t** argv);
		wchar_t* argv[2] = {L"sqlite3", L"-table"};
		wmain(argc, argv);
		#endif
		printf("Respawning cdclient shell...\n");
	}
	return 0;
}

BOOL WINAPI DllMain(HINSTANCE hInst, DWORD reason, LPVOID v) {
	if (reason == DLL_PROCESS_ATTACH) {
		// disable legouniverse.exe stdout to avoid spam, ours will still work for some reason
		SetStdHandle(STD_OUTPUT_HANDLE, NULL);
		HANDLE thread = CreateThread(NULL, 0, ShellThread, NULL, 0, NULL);
	}
	return TRUE;
}
#endif
#endif
