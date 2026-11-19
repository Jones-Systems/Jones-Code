#include <inttypes.h>
#include <libproc.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/proc_info.h>
#include <unistd.h>

enum {
    thread_details_unavailable = 5,
    thread_set_changed = 6,
};

/* XNU's PROC_PIDLISTTHREADIDS (sys/proc_info_private.h) returns kernel IDs.
 * PROC_PIDLISTTHREADS returns pthread handles, which can be zero.
 * This qualification helper falls back to sample if the flavor is unavailable. */
static const int pid_list_thread_ids = 28;

static int read_thread_snapshot(int pid, uint64_t threads[512], struct proc_threadinfo info[512],
                                size_t *count) {
    uint64_t after_threads[512];
    int size = proc_pidinfo(pid, pid_list_thread_ids, 0, threads, sizeof(after_threads));
    if (size <= 0 || size >= (int)sizeof(after_threads) || size % sizeof(threads[0])) return 4;
    size_t thread_count = (size_t)size / sizeof(threads[0]);
    for (size_t i = 0; i < thread_count; ++i) {
        if (threads[i] == 0 ||
            proc_pidinfo(pid, PROC_PIDTHREADID64INFO, threads[i], &info[i], sizeof(info[i])) != sizeof(info[i]) ||
            !memchr(info[i].pth_name, '\0', sizeof(info[i].pth_name))) return thread_details_unavailable;
    }
    if (proc_pidinfo(pid, pid_list_thread_ids, 0, after_threads, sizeof(after_threads)) != size ||
        memcmp(threads, after_threads, (size_t)size) != 0) return thread_set_changed;
    *count = thread_count;
    return 0;
}

/* Qualification-only read of one task-captured process. */
int main(int argc, char **argv) {
    if (argc != 2) return 2;
    char *end = NULL;
    long value = strtol(argv[1], &end, 10);
    if (*end || value < 1 || value > INT32_MAX) return 2;

    int pid = (int)value;
    struct proc_bsdinfo before = {0}, after = {0};
    uint64_t threads[512];
    struct proc_threadinfo info[512] = {0};
    if (proc_pidinfo(pid, PROC_PIDTBSDINFO, 0, &before, sizeof(before)) != sizeof(before) ||
        before.pbi_uid != getuid() || before.pbi_pid != (uint32_t)pid) return 3;

    size_t count = 0;
    int result = read_thread_snapshot(pid, threads, info, &count);
    // Thread creation or exit can invalidate a bracket. Try one new bracket before falling back.
    if (result == thread_details_unavailable || result == thread_set_changed) {
        result = read_thread_snapshot(pid, threads, info, &count);
    }
    if (result != 0) return result;
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
