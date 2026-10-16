#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <signal.h>
#include <spawn.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

static volatile sig_atomic_t cancelled = 0;
static pid_t owned_child = -1;
static pid_t unexpected_child = -1;

static void cancel(int signal_number) { cancelled = signal_number; }

static double seconds(void) {
  struct timespec now;
  if (clock_gettime(CLOCK_MONOTONIC, &now) != 0) return 0;
  return now.tv_sec + now.tv_nsec / 1000000000.0;
}

/* An unreaped direct child cannot have its PID reused. Never accept a discovered PID. */
static int reap_child(pid_t *child, int terminate, int *status) {
  if (child == NULL || *child <= 0) return EINVAL;
  if (terminate && kill(*child, SIGKILL) != 0 && errno != ESRCH) return -1;
  double deadline = seconds() + 5;
  for (;;) {
    pid_t result = waitpid(*child, status, WNOHANG);
    if (result == *child) {
      *child = -1;
      return 0;
    }
    if (result < 0 && errno != EINTR) return -1;
    if (seconds() >= deadline) return -1;
    if (cancelled && !terminate) return -1;
    (void)poll(NULL, 0, 10);
  }
}

static int fail(const char *message) {
  int status;
  fprintf(stderr, "%s\n", message);
  int cleanup_failed = 0;
  if (owned_child > 0 && reap_child(&owned_child, 1, &status) != 0) cleanup_failed = 1;
  if (unexpected_child > 0 && reap_child(&unexpected_child, 1, &status) != 0) cleanup_failed = 1;
  if (cleanup_failed) {
    fprintf(stderr, "Captured child cleanup failed.\n");
    return 2;
  }
  return 1;
}

int main(int argc, char **argv) {
#ifndef __APPLE__
  (void)argc;
  (void)argv;
  puts("{\"status\":\"unsupported\",\"reason\":\"darwin-required\"}");
  return 77;
#else
  if (argc == 3 && strcmp(argv[1], "--writer") == 0) {
    int fd = open(argv[2], O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW, 0600);
    if (fd < 0) return 3;
    int result = write(fd, "native-write\n", 13) == 13 && fsync(fd) == 0 ? 0 : 4;
    if (close(fd) != 0) result = 4;
    return result;
  }
  if (argc != 3) return fail("Expected fixture executable and marker paths.");
  struct sigaction action;
  memset(&action, 0, sizeof(action));
  action.sa_handler = cancel;
  sigemptyset(&action.sa_mask);
  if (sigaction(SIGTERM, &action, NULL) || sigaction(SIGINT, &action, NULL) ||
      sigaction(SIGHUP, &action, NULL)) return fail("Cannot install cleanup handlers.");
  signal(SIGPIPE, SIG_IGN);
  posix_spawnattr_t attributes;
  if (posix_spawnattr_init(&attributes)) return fail("Cannot create spawn attributes.");
  if (posix_spawnattr_setflags(&attributes, POSIX_SPAWN_START_SUSPENDED)) {
    posix_spawnattr_destroy(&attributes);
    return fail("Suspended spawn is unavailable.");
  }
  char *arguments[] = {argv[1], "--writer", argv[2], NULL};
  char *environment[] = {"PATH=/usr/bin:/bin", NULL};
  int error = posix_spawn(&owned_child, argv[1], NULL, &attributes, arguments, environment);
  posix_spawnattr_destroy(&attributes);
  if (error) {
    owned_child = -1;
    return fail("Suspended spawn failed.");
  }
  printf("{\"event\":\"suspended\",\"pid\":%d}\n", owned_child);
  if (fflush(stdout) != 0) return fail("Cannot publish captured child.");
  struct pollfd input = {STDIN_FILENO, POLLIN, 0};
  char command[32];
  size_t length = 0;
  double deadline = seconds() + 15;
  while (length < sizeof(command) - 1) {
    if (cancelled) return fail("Native fixture cancelled.");
    if (seconds() >= deadline) return fail("Native fixture command timed out.");
    int available = poll(&input, 1, 50);
    if (available < 0 && errno == EINTR) continue;
    if (available < 0) return fail("Native fixture input failed.");
    if (!available) continue;
    ssize_t count = read(STDIN_FILENO, command + length, 1);
    if (count <= 0) return fail("Native fixture input closed.");
    if (command[length++] == '\n') break;
  }
  command[length] = 0;
  error = posix_spawn(&unexpected_child, argv[1], NULL, NULL, arguments, environment);
  if (!error) {
    /* Both PIDs came from this controller; retain and reap each if denial fails. */
    return fail("Fresh launch unexpectedly passed the execute gate.");
  }
  unexpected_child = -1;
  if (error != EACCES) return fail("Fresh launch did not fail with EACCES.");
  int status = 0;
  int refused = strcmp(command, "missing-custody\n") == 0;
  if (strcmp(command, "resume\n") == 0) {
    if (kill(owned_child, SIGCONT) || reap_child(&owned_child, 0, &status))
      return fail("Cannot resume and reap captured child.");
    if (!WIFEXITED(status) || WEXITSTATUS(status)) return fail("Writer failed after resumption.");
  } else if (strcmp(command, "drain\n") == 0 || refused) {
    /* The admission operation has no authority when its custody handle is absent.
       The controller retains separate cleanup custody even in that negative case. */
    if (refused && reap_child(NULL, 1, &status) != EINVAL)
      return fail("Drain admitted an absent custody handle.");
    if (reap_child(&owned_child, 1, &status)) return fail("Cannot drain captured child.");
    if (!WIFSIGNALED(status) || WTERMSIG(status) != SIGKILL)
      return fail("Captured child did not terminate before user space.");
  } else {
    return fail("Unknown native fixture command.");
  }
  printf("{\"event\":\"complete\",\"freshLaunchErrno\":%d,\"reaped\":true,\"custodyRefused\":%s}\n",
         error, refused ? "true" : "false");
  return fflush(stdout) == 0 ? 0 : 1;
#endif
}
