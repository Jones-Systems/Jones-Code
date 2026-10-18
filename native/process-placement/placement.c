#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <limits.h>
#include <linux/magic.h>
#include <linux/capability.h>
#include <sched.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <sys/prctl.h>
#include <sys/stat.h>
#include <sys/vfs.h>
#include <sys/syscall.h>
#include <unistd.h>

static void fail(const char *message) {
  fprintf(stderr, "t3 process placement: %s\n", message);
  exit(125);
}
struct sha256 { uint32_t state[8]; uint64_t bytes; unsigned char block[64]; size_t used; };
static uint32_t rotate(uint32_t value, unsigned count) { return (value >> count) | (value << (32 - count)); }
static void sha_block(struct sha256 *hash) {
  static const uint32_t k[64] = {
    0x428a2f98,0x71374491,0xb5c0fbcf,0xe9b5dba5,0x3956c25b,0x59f111f1,0x923f82a4,0xab1c5ed5,
    0xd807aa98,0x12835b01,0x243185be,0x550c7dc3,0x72be5d74,0x80deb1fe,0x9bdc06a7,0xc19bf174,
    0xe49b69c1,0xefbe4786,0x0fc19dc6,0x240ca1cc,0x2de92c6f,0x4a7484aa,0x5cb0a9dc,0x76f988da,
    0x983e5152,0xa831c66d,0xb00327c8,0xbf597fc7,0xc6e00bf3,0xd5a79147,0x06ca6351,0x14292967,
    0x27b70a85,0x2e1b2138,0x4d2c6dfc,0x53380d13,0x650a7354,0x766a0abb,0x81c2c92e,0x92722c85,
    0xa2bfe8a1,0xa81a664b,0xc24b8b70,0xc76c51a3,0xd192e819,0xd6990624,0xf40e3585,0x106aa070,
    0x19a4c116,0x1e376c08,0x2748774c,0x34b0bcb5,0x391c0cb3,0x4ed8aa4a,0x5b9cca4f,0x682e6ff3,
    0x748f82ee,0x78a5636f,0x84c87814,0x8cc70208,0x90befffa,0xa4506ceb,0xbef9a3f7,0xc67178f2
  };
  uint32_t w[64];
  for (int i = 0; i < 16; i++) w[i] = (uint32_t)hash->block[4*i] << 24 | (uint32_t)hash->block[4*i+1] << 16 | (uint32_t)hash->block[4*i+2] << 8 | hash->block[4*i+3];
  for (int i = 16; i < 64; i++) {
    uint32_t a = w[i-15], b = w[i-2];
    w[i] = w[i-16] + (rotate(a,7) ^ rotate(a,18) ^ (a >> 3)) + w[i-7] + (rotate(b,17) ^ rotate(b,19) ^ (b >> 10));
  }
  uint32_t a=hash->state[0], b=hash->state[1], c=hash->state[2], d=hash->state[3], e=hash->state[4], f=hash->state[5], g=hash->state[6], h=hash->state[7];
  for (int i = 0; i < 64; i++) {
    uint32_t first=h+(rotate(e,6)^rotate(e,11)^rotate(e,25))+((e&f)^(~e&g))+k[i]+w[i];
    uint32_t second=(rotate(a,2)^rotate(a,13)^rotate(a,22))+((a&b)^(a&c)^(b&c));
    h=g;g=f;f=e;e=d+first;d=c;c=b;b=a;a=first+second;
  }
  hash->state[0]+=a;hash->state[1]+=b;hash->state[2]+=c;hash->state[3]+=d;hash->state[4]+=e;hash->state[5]+=f;hash->state[6]+=g;hash->state[7]+=h;
}
static void sha_byte(struct sha256 *hash, unsigned char value) {
  hash->block[hash->used++] = value;
  if (hash->used == 64) { sha_block(hash); hash->used = 0; }
}
static void verify_helper(const char *path, const char *expected, uintmax_t device, uintmax_t inode) {
  char actual[PATH_MAX];
  ssize_t size = readlink("/proc/self/exe", actual, sizeof(actual)-1);
  if (size < 0 || size >= (ssize_t)sizeof(actual)-1) fail("helper path unavailable");
  actual[size] = 0;
  if (strcmp(actual, path) || strlen(expected) != 64) fail("helper identity changed");
  int fd = open("/proc/self/exe", O_RDONLY | O_CLOEXEC);
  struct stat stat;
  if (fd < 0 || fstat(fd, &stat) || !S_ISREG(stat.st_mode) || (uintmax_t)stat.st_dev != device ||
      (uintmax_t)stat.st_ino != inode || (stat.st_mode & 0022) || stat.st_uid != getuid()) fail("helper identity changed");
  struct sha256 hash = { .state = {0x6a09e667,0xbb67ae85,0x3c6ef372,0xa54ff53a,0x510e527f,0x9b05688c,0x1f83d9ab,0x5be0cd19} };
  unsigned char buffer[4096]; ssize_t count;
  while ((count = read(fd, buffer, sizeof(buffer))) > 0) {
    hash.bytes += (uint64_t)count;
    for (ssize_t i = 0; i < count; i++) sha_byte(&hash, buffer[i]);
  }
  if (count < 0) fail("helper hash unavailable");
  close(fd);
  uint64_t bits = hash.bytes * 8;
  sha_byte(&hash, 0x80);
  while (hash.used != 56) sha_byte(&hash, 0);
  for (int i = 7; i >= 0; i--) sha_byte(&hash, (unsigned char)(bits >> (8*i)));
  char digest[65];
  for (int i = 0; i < 8; i++) snprintf(digest+8*i, 9, "%08" PRIx32, hash.state[i]);
  if (strcmp(digest, expected)) fail("helper hash changed");
}
static uintmax_t number(const char *input) {
  char *end;
  errno = 0;
  if (!*input || strlen(input) > 20) fail("invalid identity");
  for (const char *digit = input; *digit; digit++) if (*digit < '0' || *digit > '9') fail("invalid identity");
  uintmax_t value = strtoumax(input, &end, 10);
  if (errno || *end) fail("invalid identity");
  return value;
}
static int open_directory(const char *path) {
  if (path[0] != '/' || strlen(path) >= PATH_MAX) fail("invalid cgroup path");
  int fd = open("/", O_PATH | O_DIRECTORY | O_CLOEXEC);
  if (fd < 0) fail("cannot open filesystem root");
  char copy[PATH_MAX];
  strcpy(copy, path + 1);
  char *save = NULL;
  for (char *part = strtok_r(copy, "/", &save); part; part = strtok_r(NULL, "/", &save)) {
    if (!strcmp(part, ".") || !strcmp(part, "..")) fail("invalid cgroup path");
    int next = openat(fd, part, O_PATH | O_NOFOLLOW | O_DIRECTORY | O_CLOEXEC);
    close(fd);
    if (next < 0) fail("cgroup unavailable or symlinked");
    fd = next;
  }
  return fd;
}
static void identity(int fd, uintmax_t device, uintmax_t inode) {
  struct stat stat;
  struct statfs fs;
  if (fstat(fd, &stat) || fstatfs(fd, &fs) || fs.f_type != CGROUP2_SUPER_MAGIC ||
      (uintmax_t)stat.st_dev != device || (uintmax_t)stat.st_ino != inode) fail("stale cgroup identity");
}
struct placement_sched_attr {
  unsigned int size;
  unsigned int policy;
  unsigned long long flags;
  int nice;
  unsigned int priority;
  unsigned long long runtime, deadline, period;
};
static void verify_server_priority(pid_t pid) {
  struct placement_sched_attr attr = { .size = sizeof(attr) };
  if (syscall(SYS_sched_getattr, pid, &attr, sizeof(attr), 0) || attr.policy != SCHED_OTHER ||
      attr.nice != -15 || !(attr.flags & 1ULL)) fail("server priority or reset-on-fork unavailable");
}
int main(int argc, char **argv) {
  if (argc < 11 || strcmp(argv[1], "--helper-path") || strcmp(argv[3], "--helper-sha256") ||
      strcmp(argv[5], "--helper-device") || strcmp(argv[7], "--helper-inode")) fail("invalid helper invocation");
  verify_helper(argv[2], argv[4], number(argv[6]), number(argv[8]));
  argc -= 8; argv += 8;
  if (argc == 3 && !strcmp(argv[1], "--verify-server-priority")) {
    uintmax_t pid = number(argv[2]);
    if (pid == 0 || pid > INT_MAX || (pid_t)pid != getppid()) fail("only the invoking parent may be verified");
    verify_server_priority((pid_t)pid);
    return 0;
  }
  int server_priority = argc >= 13 && !strcmp(argv[9], "--server-priority") && !strcmp(argv[10], "-15");
  int delimiter = server_priority ? 11 : 9;
  if (argc < 11 || strcmp(argv[1], "--role") || strcmp(argv[3], "--cgroup") ||
      strcmp(argv[5], "--device") || strcmp(argv[7], "--inode") || strcmp(argv[delimiter], "--") ||
      (strcmp(argv[2], "control") && strcmp(argv[2], "workload"))) fail("invalid invocation");
  if (server_priority && strcmp(argv[2], "control")) fail("server priority requires control role");
  const char *path = argv[4];
  if (strncmp(path, "/sys/fs/cgroup/", 15)) fail("cgroup must be beneath /sys/fs/cgroup");
  uintmax_t device = number(argv[6]), inode = number(argv[8]);
  int directory = open_directory(path);
  identity(directory, device, inode);
  int cpu = openat(directory, "cpu.max", O_RDONLY | O_NOFOLLOW | O_CLOEXEC);
  if (cpu < 0) fail("CPU controller unavailable");
  close(cpu);
  int procs = openat(directory, "cgroup.procs", O_WRONLY | O_NOFOLLOW | O_CLOEXEC);
  if (procs < 0) fail("delegation unavailable");
  char pid[32];
  int size = snprintf(pid, sizeof(pid), "%ld\n", (long)getpid());
  if (write(procs, pid, (size_t)size) != size) fail("placement rejected");
  close(procs);
  FILE *membership = fopen("/proc/self/cgroup", "re");
  if (!membership) fail("cannot verify membership");
  char *line = NULL; size_t capacity = 0; int verified = 0;
  while (getline(&line, &capacity, membership) >= 0) {
    if (strncmp(line, "0::", 3)) continue;
    line[strcspn(line, "\n")] = 0;
    char current[PATH_MAX];
    if (snprintf(current, sizeof(current), "/sys/fs/cgroup%s", line + 3) >= (int)sizeof(current)) fail("membership too long");
    int actual = open_directory(current);
    identity(actual, device, inode);
    close(actual); verified = 1;
  }
  free(line); fclose(membership);
  if (!verified) fail("membership mismatch");
  identity(directory, device, inode);
  close(directory);
  if (!strcmp(argv[2], "workload")) {
    struct sched_param params = { .sched_priority = 0 };
    if (sched_setscheduler(0, SCHED_OTHER, &params) || setpriority(PRIO_PROCESS, 0, 0)) fail("cannot reset workload scheduling");
  }
  if (server_priority) {
    struct placement_sched_attr attr = { .size = sizeof(attr), .policy = SCHED_OTHER, .flags = 1ULL, .nice = -15 };
    if (syscall(SYS_sched_setattr, 0, &attr, 0)) fail("server priority capability unavailable");
    verify_server_priority(0);
  }
  struct __user_cap_header_struct capability_header = { .version = _LINUX_CAPABILITY_VERSION_3, .pid = 0 };
  struct __user_cap_data_struct capability_data[2] = { { 0 }, { 0 } };
  if (prctl(PR_CAP_AMBIENT, PR_CAP_AMBIENT_CLEAR_ALL, 0, 0, 0) ||
      syscall(SYS_capset, &capability_header, capability_data)) fail("cannot drop helper capabilities");
  execvp(argv[delimiter + 1], &argv[delimiter + 1]);
  fail("payload execution failed");
}
