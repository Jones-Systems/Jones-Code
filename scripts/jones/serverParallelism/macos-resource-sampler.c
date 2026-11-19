#include <inttypes.h>
#include <libproc.h>
#include <mach/mach_time.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/proc_info.h>
#include <unistd.h>

static int read_process(int pid, uid_t uid, mach_timebase_info_data_t timebase) {
    struct proc_taskallinfo snapshot = {0};
    struct proc_bsdinfo after = {0};
    if (proc_pidinfo(pid, PROC_PIDTASKALLINFO, 0, &snapshot, sizeof(snapshot)) != sizeof(snapshot))
        return 1;

    const struct proc_bsdinfo *identity = &snapshot.pbsd;
    const struct proc_taskinfo *task = &snapshot.ptinfo;
    if (identity->pbi_uid != uid || identity->pbi_pid != (uint32_t)pid ||
        !identity->pbi_start_tvsec || identity->pbi_start_tvusec >= 1000000 ||
        !task->pti_resident_size || task->pti_threadnum < 1 ||
        task->pti_total_user > UINT64_MAX - task->pti_total_system)
        return 1;

    if (proc_pidinfo(pid, PROC_PIDTBSDINFO, 0, &after, sizeof(after)) != sizeof(after) ||
        identity->pbi_pid != after.pbi_pid || identity->pbi_uid != after.pbi_uid ||
        identity->pbi_start_tvsec != after.pbi_start_tvsec ||
        identity->pbi_start_tvusec != after.pbi_start_tvusec)
        return 1;

    // PROC_PIDTASKALLINFO reports CPU in Mach time units, including on arm64.
    const long double ns = (long double)(task->pti_total_user + task->pti_total_system) *
        timebase.numer / timebase.denom;
    printf(" %d:%.0Lf:%" PRIu64 ":%d:%" PRIu64 ":%" PRIu64, pid, ns,
        task->pti_resident_size, task->pti_threadnum, identity->pbi_start_tvsec,
        identity->pbi_start_tvusec);
    return 0;
}

int main(void) {
    enum { MAX_PIDS = 4 }; // Qualification parent and up to three captured workers.
    char request[64];
    const uid_t uid = getuid();
    const pid_t parent = getppid();
    const pid_t self = getpid();
    mach_timebase_info_data_t timebase = {0};
    if (parent <= 1 || mach_timebase_info(&timebase) != KERN_SUCCESS ||
        !timebase.numer || !timebase.denom || puts("ready") == EOF || fflush(stdout) == EOF)
        return 1;

    // EOF also closes the helper if its owning qualification process exits.
    while (fgets(request, sizeof(request), stdin)) {
        const size_t length = strlen(request);
        if (length < 2 || request[length - 1] != '\n' || getppid() != parent) return 2;
        int pids[MAX_PIDS];
        size_t count = 0;
        char *cursor = request;
        for (;;) {
            if (count == MAX_PIDS || *cursor < '1' || *cursor > '9') return 2;
            char *end = NULL;
            long pid = strtol(cursor, &end, 10);
            if (pid < 1 || pid > INT32_MAX || pid == self) return 2;
            for (size_t i = 0; i < count; i++)
                if (pids[i] == pid) return 2;
            pids[count++] = (int)pid;
            if (*end == '\n') break;
            if (*end != ' ') return 2;
            cursor = end + 1;
        }
        if (pids[0] != parent) return 2;
        fputs("rows", stdout);
        for (size_t i = 0; i < count; i++)
            if (read_process(pids[i], uid, timebase)) printf(" %d:unavailable", pids[i]);
        // Record the observer separately from the workload process budget.
        if (read_process(self, uid, timebase)) printf(" %d:unavailable", self);
        if (putchar('\n') == EOF || fflush(stdout) == EOF) return 1;
    }
    return ferror(stdin) ? 1 : 0;
}
