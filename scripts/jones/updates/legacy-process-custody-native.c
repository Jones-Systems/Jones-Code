#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <signal.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#ifdef __APPLE__
#include <libproc.h>
#include <mach/message.h>
#include <spawn.h>

/* Private ABI from apple-oss-distributions/xnu, bsd/sys/proc_info_private.h.
   SDKs omit this flavor. Exact byte count and field offsets are qualification gates. */
struct jones_uniqidentifierinfo {
  uint8_t executable_uuid[16];
  uint64_t unique_id;
  uint64_t parent_unique_id;
  int32_t idversion;
  int32_t original_parent_idversion;
  uint64_t reserved2;
  uint64_t reserved3;
};
_Static_assert(sizeof(struct jones_uniqidentifierinfo) == 56, "private ABI size");
_Static_assert(offsetof(struct jones_uniqidentifierinfo, idversion) == 32, "private ABI offset");
_Static_assert(sizeof(audit_token_t) == 32, "audit token ABI size");

typedef int (*signal_token_fn)(audit_token_t *, int);
static volatile sig_atomic_t cancelled;
static pid_t owned_child = -1;
static int actor_input = -1;
static int actor_output = -1;

static void cancel(int number) { cancelled = number; }

static double seconds(void) {
  struct timespec value;
  if (clock_gettime(CLOCK_MONOTONIC, &value) != 0) return 0;
  return value.tv_sec + value.tv_nsec / 1000000000.0;
}

static int wait_owned(int *status, int flags, int cleanup) {
  double deadline = seconds() + 5;
  while (seconds() < deadline) {
    if (cancelled && !cleanup) return -1;
    pid_t result = waitpid(owned_child, status, flags | WNOHANG);
    if (result == owned_child) {
      if (WIFEXITED(*status) || WIFSIGNALED(*status)) owned_child = -1;
      return 0;
    }
    if (result < 0 && errno != EINTR) return -1;
    (void)poll(NULL, 0, 10);
  }
  return -1;
}

static int cleanup(void) {
  int status;
  if (actor_input >= 0) close(actor_input);
  if (actor_output >= 0) close(actor_output);
  actor_input = actor_output = -1;
  if (owned_child <= 0) return 0;
  /* PID-only signaling is restricted to cleanup of our unreaped direct child.
     It is never a fallback for the audit-token qualification operation. */
  if (kill(owned_child, SIGKILL) != 0 && errno != ESRCH) return -1;
  return wait_owned(&status, 0, 1);
}

static int finish(const char *status, const char *reason, int code) {
  if (cleanup() != 0) {
    puts("{\"event\":\"result\",\"status\":\"failed\",\"reason\":\"cleanup-unproved\",\"cleanupProved\":false}");
    return 2;
  }
  printf("{\"event\":\"result\",\"status\":\"%s\",\"reason\":\"%s\",\"cleanupProved\":true}\n", status, reason);
  return fflush(stdout) == 0 ? code : 1;
}

static int byte(int descriptor, char *value, int bounded) {
  double deadline = seconds() + 5;
  for (;;) {
    if (cancelled || (bounded && seconds() >= deadline)) return -1;
    struct pollfd input = {descriptor, POLLIN, 0};
    int available = poll(&input, 1, 50);
    if (available < 0 && errno == EINTR) continue;
    if (available < 0) return -1;
    if (available == 0) continue;
    ssize_t count = read(descriptor, value, 1);
    if (count < 0 && errno == EINTR) continue;
    return count == 1 ? 0 : -1;
  }
}

static int actor(int argc, char **argv) {
  if (argc != 5) return 10;
  int input = atoi(argv[2]), output = atoi(argv[3]);
  char ready = strcmp(argv[4], "after-exec") == 0 ? 'X' : 'R';
  if (write(output, &ready, 1) != 1) return 11;
  char command;
  while (byte(input, &command, 0) == 0) {
    if (command == 'P') {
      if (write(output, "A", 1) != 1) return 12;
    } else if (command == 'E') {
      char *arguments[] = {argv[0], "--actor", argv[2], argv[3], "after-exec", NULL};
      execv(argv[0], arguments);
      return 13;
    } else return 14;
  }
  return 0;
}

static int exchange(char command, char expected) {
  char reply;
  return write(actor_input, &command, 1) == 1 && byte(actor_output, &reply, 1) == 0 && reply == expected ? 0 : -1;
}

static int capture(int flavor, struct jones_uniqidentifierinfo *info, audit_token_t *token) {
  memset(info, 0, sizeof(*info));
  int count = proc_pidinfo(owned_child, flavor, 0, info, sizeof(*info));
  if (count != sizeof(*info) || info->unique_id == 0 || info->idversion == 0) return -1;
  memset(token, 0, sizeof(*token));
  /* XNU proc_find_audit_token validates PID and pidversion. These two values
     originate from our captured child and kernel readback, not a process search. */
  token->val[5] = (uint32_t)owned_child;
  token->val[7] = (uint32_t)info->idversion;
  return 0;
}
#endif

int main(int argc, char **argv) {
#ifndef __APPLE__
  (void)argc;
  (void)argv;
  puts("{\"event\":\"result\",\"status\":\"unsupported\",\"reason\":\"darwin-required\",\"cleanupProved\":true}");
  return 77;
#else
  if (argc >= 2 && strcmp(argv[1], "--actor") == 0) return actor(argc, argv);
  if (argc != 2 || (strcmp(argv[1], "run") && strcmp(argv[1], "missing-symbol") && strcmp(argv[1], "missing-flavor")))
    return finish("failed", "invalid-fixture-scenario", 1);
  if (getuid() == 0 || geteuid() == 0) return finish("unsupported", "rootless-proof-required", 77);
  struct sigaction action;
  memset(&action, 0, sizeof(action));
  action.sa_handler = cancel;
  sigemptyset(&action.sa_mask);
  if (sigaction(SIGTERM, &action, NULL) || sigaction(SIGINT, &action, NULL) || sigaction(SIGHUP, &action, NULL))
    return finish("failed", "cleanup-handlers-unavailable", 1);
  signal(SIGPIPE, SIG_IGN);
  const char *symbol = strcmp(argv[1], "missing-symbol") == 0 ? "jones_fixture_absent_signal_symbol" : "proc_signal_with_audittoken";
  signal_token_fn signal_token = (signal_token_fn)dlsym(RTLD_DEFAULT, symbol);
  if (signal_token == NULL) return finish("unsupported", "direct-symbol-unavailable", 77);
  int command_pipe[2], result_pipe[2];
  if (pipe(command_pipe) != 0) return finish("failed", "command-pipe-failed", 1);
  if (pipe(result_pipe) != 0) {
    close(command_pipe[0]); close(command_pipe[1]);
    return finish("failed", "result-pipe-failed", 1);
  }
  char input[24], output[24];
  snprintf(input, sizeof(input), "%d", command_pipe[0]);
  snprintf(output, sizeof(output), "%d", result_pipe[1]);
  char *arguments[] = {argv[0], "--actor", input, output, "before-exec", NULL};
  char *environment[] = {"PATH=/usr/bin:/bin", NULL};
  posix_spawn_file_actions_t actions;
  int error = posix_spawn_file_actions_init(&actions);
  int actions_initialized = !error;
  if (!error) error = posix_spawn_file_actions_addclose(&actions, command_pipe[1]);
  if (!error) error = posix_spawn_file_actions_addclose(&actions, result_pipe[0]);
  if (!error) error = posix_spawn(&owned_child, argv[0], &actions, NULL, arguments, environment);
  if (actions_initialized) posix_spawn_file_actions_destroy(&actions);
  close(command_pipe[0]); close(result_pipe[1]);
  actor_input = command_pipe[1]; actor_output = result_pipe[0];
  if (error) { owned_child = -1; return finish("failed", "actor-spawn-failed", 1); }
  char ready;
  if (byte(actor_output, &ready, 1) || ready != 'R') return finish("failed", "actor-not-ready", 1);
  struct jones_uniqidentifierinfo before, after;
  audit_token_t original, current;
  int flavor = strcmp(argv[1], "missing-flavor") == 0 ? 0x7fffffff : 17;
  if (capture(flavor, &before, &original)) return finish("unsupported", "kernel-identity-schema-unavailable", 77);
  printf("{\"event\":\"ready\",\"pid\":%d,\"pidversion\":%u,\"uniqueId\":\"%llu\"}\n",
         owned_child, original.val[7], (unsigned long long)before.unique_id);
  if (fflush(stdout)) return finish("failed", "ready-output-failed", 1);
  char start;
  if (byte(STDIN_FILENO, &start, 1) || start != 'R') return finish("failed", "fixture-cancelled-or-control-closed", 1);
  audit_token_t wrong = original;
  wrong.val[7] ^= 1;
  int wrong_result = signal_token(&wrong, SIGTERM);
  if (wrong_result != ESRCH || exchange('P', 'A')) return finish("failed", "wrong-token-was-not-refused-with-survivor", 1);
  int stopped = signal_token(&original, SIGSTOP);
  if (stopped == EPERM || stopped == ENOSYS || stopped == ENOTSUP)
    return finish("unsupported", "direct-signal-not-permitted", 77);
  int status;
  if (stopped || wait_owned(&status, WUNTRACED, 0) || !WIFSTOPPED(status) || WSTOPSIG(status) != SIGSTOP)
    return finish("failed", "exact-stop-unproved", 1);
  if (signal_token(&original, SIGCONT) || exchange('P', 'A')) return finish("failed", "exact-continue-unproved", 1);
  if (exchange('E', 'X') || capture(17, &after, &current)) return finish("failed", "exec-identity-readback-failed", 1);
  if (current.val[5] != original.val[5] || current.val[7] == original.val[7])
    return finish("unsupported", "same-pid-exec-version-change-unproved", 77);
  int stale_result = signal_token(&original, SIGTERM);
  if (stale_result != ESRCH || exchange('P', 'A')) return finish("failed", "stale-token-was-not-refused-with-survivor", 1);
  if (signal_token(&current, SIGTERM) || wait_owned(&status, 0, 0) || !WIFSIGNALED(status) || WTERMSIG(status) != SIGTERM)
    return finish("failed", "exact-terminate-unproved", 1);
  printf("{\"event\":\"proof\",\"wrongTokenErrno\":%d,\"staleTokenErrno\":%d,\"samePid\":%u,"
         "\"beforePidversion\":%u,\"afterPidversion\":%u,\"beforeUniqueId\":\"%llu\",\"afterUniqueId\":\"%llu\","
         "\"stopObserved\":true,\"continueSurvivor\":true,\"wrongTokenSurvivor\":true,\"staleTokenSurvivor\":true,\"termReaped\":true}\n",
         wrong_result, stale_result, original.val[5], original.val[7], current.val[7],
         (unsigned long long)before.unique_id, (unsigned long long)after.unique_id);
  return finish("passed", "fixture-owned-audit-token-signaling", 0);
#endif
}
