#include <inttypes.h>
#include <libproc.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/proc_info.h>
#include <unistd.h>

/* Qualification-only read of one task-captured process. */
int main(int argc, char **argv) {
    if (argc != 2) return 2;
    char *end = NULL;
    long value = strtol(argv[1], &end, 10);
    if (*end || value < 1 || value > INT32_MAX) return 2;

    int pid = (int)value;
    struct proc_bsdinfo before = {0}, after = {0};
    uint64_t threads[512], after_threads[512];
    struct proc_threadinfo info[512] = {0};
    if (proc_pidinfo(pid, PROC_PIDTBSDINFO, 0, &before, sizeof(before)) != sizeof(before) ||
        before.pbi_uid != getuid() || before.pbi_pid != (uint32_t)pid) return 3;

    int size = proc_pidinfo(pid, PROC_PIDLISTTHREADS, 0, threads, sizeof(threads));
    if (size <= 0 || size >= (int)sizeof(threads) || size % sizeof(threads[0])) return 4;
    size_t count = (size_t)size / sizeof(threads[0]);
    for (size_t i = 0; i < count; ++i) {
        /* LISTTHREADS supplies opaque handles consumed by THREADINFO. */
        if (proc_pidinfo(pid, PROC_PIDTHREADINFO, threads[i], &info[i], sizeof(info[i])) != sizeof(info[i]) ||
            !memchr(info[i].pth_name, '\0', sizeof(info[i].pth_name))) return 5;
    }
    if (proc_pidinfo(pid, PROC_PIDLISTTHREADS, 0, after_threads, sizeof(after_threads)) != size ||
        memcmp(threads, after_threads, (size_t)size) != 0) return 6;
    if (proc_pidinfo(pid, PROC_PIDTBSDINFO, 0, &after, sizeof(after)) != sizeof(after) ||
        before.pbi_pid != after.pbi_pid || before.pbi_uid != after.pbi_uid ||
        before.pbi_start_tvsec != after.pbi_start_tvsec ||
        before.pbi_start_tvusec != after.pbi_start_tvusec) return 7;

    printf("%d\t%" PRIu64 "\t%" PRIu64 "\n", pid, before.pbi_start_tvsec, before.pbi_start_tvusec);
    for (size_t i = 0; i < count; ++i) {
        printf("%" PRIx64 "\t", threads[i]);
        for (const unsigned char *name = (const unsigned char *)info[i].pth_name; *name; ++name)
            printf("%02x", (unsigned int)*name);
        putchar('\n');
    }
    return ferror(stdout) ? 8 : 0;
}
