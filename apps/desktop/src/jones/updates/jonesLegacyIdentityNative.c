#include <errno.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#ifdef __APPLE__
#include <fcntl.h>
#include <limits.h>
#include <libproc.h>
#include <stddef.h>
#include <sys/proc_info.h>
#include <sys/attr.h>
#include <sys/mount.h>
#include <sys/stat.h>
#include <sys/sysctl.h>
#include <uuid/uuid.h>

#define MAX_OBJECTS 64
#define MAX_PATH_BYTES 32768
#define MAX_DEPTH 64

/* getattrlist buffers use four-byte packing, including the length prefix.
   Only attributes present in the returned and supported masks are consumed. */
struct __attribute__((packed, aligned(4))) object_attributes {
  uint32_t length;
  attribute_set_t returned;
  uint64_t file_id;
};
struct __attribute__((packed, aligned(4))) volume_attributes {
  uint32_t length;
  attribute_set_t returned;
  vol_capabilities_attr_t capabilities;
  uuid_t uuid;
  vol_attributes_attr_t supported;
};
_Static_assert(sizeof(struct object_attributes) == 32, "object attribute ABI");
_Static_assert(sizeof(struct volume_attributes) == 112, "volume attribute ABI");

struct observation {
  int fd;
  int directory;
  const char *path;
  struct stat info;
  struct statfs mount;
  uint64_t file_id;
  uuid_string_t volume_uuid;
};

static const char *reason = "native-observation-failed";
static int failure_errno;
static int unsupported;

static int refuse(const char *value, int error, int unavailable) {
  reason = value;
  failure_errno = error;
  unsupported = unavailable;
  return -1;
}

static int utf8(const unsigned char *text) {
  while (*text) {
    unsigned char first = *text++;
    if (first < 0x80) continue;
    int count;
    uint32_t code;
    if (first >= 0xc2 && first <= 0xdf) { count = 1; code = first & 0x1f; }
    else if (first >= 0xe0 && first <= 0xef) { count = 2; code = first & 0x0f; }
    else if (first >= 0xf0 && first <= 0xf4) { count = 3; code = first & 0x07; }
    else return 0;
    for (int index = 0; index < count; index++) {
      if (*text < 0x80 || *text > 0xbf) return 0;
      code = (code << 6) | (*text++ & 0x3f);
    }
    if ((count == 2 && code < 0x800) || (count == 3 && code < 0x10000) ||
        (code >= 0xd800 && code <= 0xdfff) || code > 0x10ffff) return 0;
  }
  return 1;
}

static int canonical_path(const char *path) {
  size_t length = strlen(path);
  if (length == 0 || length >= PATH_MAX || path[0] != '/' || !utf8((const unsigned char *)path))
    return refuse("invalid-path", 0, 0);
  if (length == 1) return 0;
  size_t start = 1;
  int depth = 0;
  for (size_t index = 1; index <= length; index++) {
    if (path[index] != '/' && path[index] != '\0') continue;
    size_t size = index - start;
    if (size == 0 || size > NAME_MAX || (size == 1 && path[start] == '.') ||
        (size == 2 && path[start] == '.' && path[start + 1] == '.') || ++depth > MAX_DEPTH)
      return refuse("invalid-path", 0, 0);
    start = index + 1;
  }
  return 0;
}

/* Ancestors are opened without following links, not claimed to be exclusively
   owned. Endpoint ownership and complete path readback are separate checks. */
static int open_path(const char *path, int directory) {
  int fd = open("/", O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
  if (fd < 0) return refuse("path-open-failed", errno, 0);
  if (path[1] == '\0') return fd;
  char copy[PATH_MAX];
  memcpy(copy, path, strlen(path) + 1);
  char *part = copy + 1;
  for (;;) {
    char *separator = strchr(part, '/');
    if (separator != NULL) *separator = '\0';
    int flags = O_EVTONLY | O_NOFOLLOW | O_CLOEXEC | O_NONBLOCK;
    if (separator != NULL || directory) flags |= O_DIRECTORY;
    int next = openat(fd, part, flags);
    int saved = errno;
    close(fd);
    if (next < 0) return refuse("path-open-failed", saved, 0);
    fd = next;
    if (separator == NULL) return fd;
    part = separator + 1;
  }
}

static int same_stat(const struct stat *left, const struct stat *right) {
  return left->st_dev == right->st_dev && left->st_ino == right->st_ino &&
    left->st_uid == right->st_uid && left->st_gid == right->st_gid &&
    left->st_mode == right->st_mode && left->st_nlink == right->st_nlink &&
    left->st_flags == right->st_flags && left->st_size == right->st_size &&
    left->st_mtimespec.tv_sec == right->st_mtimespec.tv_sec &&
    left->st_mtimespec.tv_nsec == right->st_mtimespec.tv_nsec &&
    left->st_ctimespec.tv_sec == right->st_ctimespec.tv_sec &&
    left->st_ctimespec.tv_nsec == right->st_ctimespec.tv_nsec &&
    left->st_birthtimespec.tv_sec == right->st_birthtimespec.tv_sec &&
    left->st_birthtimespec.tv_nsec == right->st_birthtimespec.tv_nsec;
}

static int boot_uuid(uuid_string_t output) {
  size_t size = sizeof(uuid_string_t);
  memset(output, 0, size);
  if (sysctlbyname("kern.bootsessionuuid", output, &size, NULL, 0) != 0)
    return refuse("boot-uuid-unavailable", errno, 1);
  uuid_t parsed;
  if (size != sizeof(uuid_string_t) || output[size - 1] != '\0' || strlen(output) != 36 ||
      uuid_parse(output, parsed) != 0 || uuid_is_null(parsed))
    return refuse("boot-uuid-schema-unavailable", 0, 1);
  return 0;
}

static int read_object(struct observation *object) {
  if (fstat(object->fd, &object->info) != 0) return refuse("fd-stat-failed", errno, 0);
  if ((object->directory ? !S_ISDIR(object->info.st_mode) : !S_ISREG(object->info.st_mode)) ||
      (!object->directory && object->info.st_nlink != 1) ||
      object->info.st_uid != getuid() || (object->info.st_mode & 0022) != 0)
    return refuse("endpoint-type-links-or-ownership", 0, 0);
  char actual[PATH_MAX];
  if (fcntl(object->fd, F_GETPATH, actual) != 0) return refuse("fd-path-unavailable", errno, 1);
  if (strcmp(actual, object->path) != 0) return refuse("fd-path-alias", 0, 0);
  int check = open_path(object->path, object->directory);
  if (check < 0) return -1;
  struct stat check_info;
  int result = fstat(check, &check_info);
  int saved = errno;
  close(check);
  if (result != 0) return refuse("path-stat-failed", saved, 0);
  if (!same_stat(&object->info, &check_info)) return refuse("fd-path-binding-changed", 0, 0);
  if (fstatfs(object->fd, &object->mount) != 0) return refuse("mount-stat-failed", errno, 0);
  if (strcmp(object->mount.f_fstypename, "apfs") != 0) return refuse("apfs-required", 0, 1);
  if (!(object->mount.f_flags & MNT_LOCAL) ||
      (object->mount.f_flags & (MNT_NOEXEC | MNT_IGNORE_OWNERSHIP)))
    return refuse("mount-policy-refused", 0, 0);
  struct attrlist requested = {0};
  requested.bitmapcount = ATTR_BIT_MAP_COUNT;
  requested.commonattr = ATTR_CMN_RETURNED_ATTRS | ATTR_CMN_FILEID;
  struct object_attributes attributes = {0};
  if (fgetattrlist(object->fd, &requested, &attributes, sizeof(attributes), FSOPT_REPORT_FULLSIZE) != 0)
    return refuse("object-attributes-unavailable", errno, 1);
  if (attributes.length != sizeof(attributes) || !(attributes.returned.commonattr & ATTR_CMN_FILEID) ||
      attributes.file_id == 0 || attributes.file_id != object->info.st_ino)
    return refuse("object-attribute-schema-unavailable", 0, 1);
  object->file_id = attributes.file_id;
  if (canonical_path(object->mount.f_mntonname) != 0) return -1;
  int root = open_path(object->mount.f_mntonname, 1);
  if (root < 0) return -1;
  struct statfs root_mount;
  result = fstatfs(root, &root_mount);
  saved = errno;
  if (result != 0 || memcmp(&root_mount.f_fsid, &object->mount.f_fsid, sizeof(fsid_t)) != 0) {
    close(root);
    return refuse("volume-root-binding-changed", result ? saved : 0, 0);
  }
  memset(&requested, 0, sizeof(requested));
  requested.bitmapcount = ATTR_BIT_MAP_COUNT;
  requested.commonattr = ATTR_CMN_RETURNED_ATTRS;
  requested.volattr = ATTR_VOL_INFO | ATTR_VOL_CAPABILITIES | ATTR_VOL_UUID | ATTR_VOL_ATTRIBUTES;
  struct volume_attributes volume = {0};
  result = fgetattrlist(root, &requested, &volume, sizeof(volume), FSOPT_REPORT_FULLSIZE);
  saved = errno;
  close(root);
  if (result != 0) return refuse("volume-attributes-unavailable", saved, 1);
  const uint32_t required = VOL_CAP_FMT_PERSISTENTOBJECTIDS | VOL_CAP_FMT_64BIT_OBJECT_IDS;
  const uint32_t volume_mask = ATTR_VOL_CAPABILITIES | ATTR_VOL_UUID | ATTR_VOL_ATTRIBUTES;
  if (volume.length != sizeof(volume) ||
      (volume.returned.volattr & volume_mask) != volume_mask ||
      (volume.capabilities.valid[VOL_CAPABILITIES_FORMAT] & required) != required ||
      (volume.capabilities.capabilities[VOL_CAPABILITIES_FORMAT] & required) != required ||
      !(volume.supported.validattr.commonattr & ATTR_CMN_FILEID) ||
      !(volume.supported.nativeattr.commonattr & ATTR_CMN_FILEID) ||
      !(volume.supported.validattr.volattr & ATTR_VOL_UUID) ||
      !(volume.supported.nativeattr.volattr & ATTR_VOL_UUID) || uuid_is_null(volume.uuid))
    return refuse("persistent-identity-capabilities-unavailable", 0, 1);
  uuid_unparse_lower(volume.uuid, object->volume_uuid);
  struct stat after;
  if (fstat(object->fd, &after) != 0) return refuse("fd-reread-failed", errno, 0);
  if (!same_stat(&object->info, &after)) return refuse("object-changed", 0, 0);
  return 0;
}

static void json_string(const char *value) {
  putchar('"');
  for (const unsigned char *cursor = (const unsigned char *)value; *cursor; cursor++) {
    if (*cursor == '"' || *cursor == '\\') printf("\\%c", *cursor);
    else if (*cursor < 0x20) printf("\\u%04x", *cursor);
    else putchar(*cursor);
  }
  putchar('"');
}

static void nanoseconds(struct timespec value) {
  /* Darwin timestamps outside this range cannot be encoded without overflow. */
  printf("\"%" PRId64 "\"", (int64_t)value.tv_sec * INT64_C(1000000000) + value.tv_nsec);
}

static int timestamp_supported(struct timespec value) {
  return value.tv_sec >= 0 && value.tv_sec <= (INT64_MAX - 999999999) / 1000000000 &&
    value.tv_nsec >= 0 && value.tv_nsec < 1000000000;
}

static void print_object(const struct observation *object, int index) {
  printf("{\"index\":%d,\"path\":", index);
  json_string(object->path);
  printf(",\"kind\":\"%s\",\"persistent\":{\"volumeUuid\":\"%s\",\"fileId\":\"%" PRIu64 "\"},"
         "\"stat\":{\"device\":\"%" PRIu64 "\",\"inode\":\"%" PRIu64 "\",\"uid\":%u,\"gid\":%u,"
         "\"nlink\":\"%" PRIu64 "\",\"mode\":%u,\"flags\":%u,\"size\":\"%" PRId64 "\",\"mtimeNs\":",
         object->directory ? "directory" : "file", object->volume_uuid, object->file_id,
         (uint64_t)object->info.st_dev, (uint64_t)object->info.st_ino,
         object->info.st_uid, object->info.st_gid, (uint64_t)object->info.st_nlink,
         object->info.st_mode & 07777, object->info.st_flags, (int64_t)object->info.st_size);
  nanoseconds(object->info.st_mtimespec);
  printf(",\"ctimeNs\":"); nanoseconds(object->info.st_ctimespec);
  printf(",\"birthtimeNs\":"); nanoseconds(object->info.st_birthtimespec);
  printf("},\"mount\":{\"filesystem\":\"apfs\",\"flags\":%u,\"mountPoint\":", object->mount.f_flags);
  json_string(object->mount.f_mntonname);
  printf(",\"local\":true,\"ownershipHonored\":true,\"executable\":true,"
         "\"persistentObjectIds\":true,\"objectIds64Bit\":true},\"current\":true}");
}

/* xnu-11215.81.4 and xnu-12377.81.4 bsd/sys/proc_info_private.h, flavor 17.
   Public SDKs omit this declaration. Offset 36 was reserved in 11215 and is
   deliberately unused; exact byte counts and consumed offsets fail closed. */
struct owner_unique_info {
  unsigned char uuid[16];
  uint64_t unique_id, parent_unique_id;
  int32_t version, original_parent_version;
  uint64_t reserved[2];
};
_Static_assert(sizeof(struct owner_unique_info) == 56, "process identity ABI");
_Static_assert(offsetof(struct owner_unique_info, unique_id) == 16, "unique ID ABI");
_Static_assert(offsetof(struct owner_unique_info, parent_unique_id) == 24, "parent unique ID ABI");
_Static_assert(offsetof(struct owner_unique_info, version) == 32, "PID version ABI");
_Static_assert(sizeof(struct proc_fdinfo) == 8, "FD list ABI");
#define OWNER_UNIQUE_FLAVOR 17
#define MAX_PROCESS_FDS 256
#define MAX_FAMILY_MEMBERS 4
#define MAX_EXCLUDED_FDS 64

struct process_identity {
  pid_t pid, parent_pid;
  uint64_t unique_id, parent_unique_id;
  uint32_t version, uid;
};
struct family_member {
  const char *name;
  struct observation object;
};
struct fd_snapshot {
  struct proc_fdinfo fds[MAX_PROCESS_FDS];
  struct vnode_fdinfo vnodes[MAX_PROCESS_FDS];
  int count;
};

static int decimal(const char *text, uint64_t maximum, uint64_t *value) {
  if (!*text || (text[0] == '0' && text[1])) return refuse("invalid-process-arguments", 0, 0);
  for (const char *p = text; *p; p++)
    if (*p < '0' || *p > '9') return refuse("invalid-process-arguments", 0, 0);
  char *end;
  errno = 0;
  unsigned long long parsed = strtoull(text, &end, 10);
  if (errno || *end || parsed > maximum) return refuse("invalid-process-arguments", 0, 0);
  *value = (uint64_t)parsed;
  return 0;
}

static int same_process(const struct process_identity *a, const struct process_identity *b) {
  return a->pid == b->pid && a->parent_pid == b->parent_pid && a->unique_id == b->unique_id &&
    a->parent_unique_id == b->parent_unique_id && a->version == b->version && a->uid == b->uid;
}

static int read_process(pid_t pid, struct process_identity *result) {
  struct owner_unique_info first = {0}, last = {0};
  struct proc_bsdinfo bsd = {0};
  if (proc_pidinfo(pid, OWNER_UNIQUE_FLAVOR, 0, &first, sizeof(first)) != (int)sizeof(first) ||
      proc_pidinfo(pid, PROC_PIDTBSDINFO, 0, &bsd, sizeof(bsd)) != (int)sizeof(bsd) ||
      proc_pidinfo(pid, OWNER_UNIQUE_FLAVOR, 0, &last, sizeof(last)) != (int)sizeof(last))
    return refuse("process-identity-unavailable", errno, 0);
  if (!first.unique_id || first.unique_id != last.unique_id || first.version != last.version ||
      first.parent_unique_id != last.parent_unique_id || bsd.pbi_pid != (uint32_t)pid ||
      bsd.pbi_uid != getuid() || bsd.pbi_ruid != getuid() || bsd.pbi_svuid != getuid())
    return refuse("process-identity-changed-or-owner", 0, 0);
  *result = (struct process_identity){pid, (pid_t)bsd.pbi_ppid, first.unique_id,
    first.parent_unique_id, (uint32_t)first.version, bsd.pbi_uid};
  return 0;
}

static int fd_order(const void *a, const void *b) {
  const struct proc_fdinfo *left = a, *right = b;
  return (left->proc_fd > right->proc_fd) - (left->proc_fd < right->proc_fd);
}

static int read_fds(pid_t pid, struct proc_fdinfo *fds) {
  const int capacity = MAX_PROCESS_FDS * (int)sizeof(*fds);
  int bytes = proc_pidinfo(pid, PROC_PIDLISTFDS, 0, fds, capacity);
  if (bytes <= 0 || bytes >= capacity || bytes % (int)sizeof(*fds))
    return refuse("fd-list-incomplete", errno, 0);
  int count = bytes / (int)sizeof(*fds);
  qsort(fds, (size_t)count, sizeof(*fds), fd_order);
  for (int i = 0; i < count; i++)
    if (fds[i].proc_fd < 0 || (i && fds[i].proc_fd == fds[i - 1].proc_fd))
      return refuse("fd-list-invalid", 0, 0);
  return count;
}

static int read_snapshot(pid_t pid, struct fd_snapshot *snapshot) {
  snapshot->count = read_fds(pid, snapshot->fds);
  if (snapshot->count < 0) return -1;
  for (int i = 0; i < snapshot->count; i++) {
    if (snapshot->fds[i].proc_fdtype != PROX_FDTYPE_VNODE) continue;
    struct vnode_fdinfo *value = &snapshot->vnodes[i];
    memset(value, 0, sizeof(*value));
    if (proc_pidfdinfo(pid, snapshot->fds[i].proc_fd, PROC_PIDFDVNODEINFO,
                       value, sizeof(*value)) != (int)sizeof(*value))
      return refuse("vnode-observation-unavailable", errno, 0);
  }
  return 0;
}

static int same_snapshot(const struct fd_snapshot *a, const struct fd_snapshot *b) {
  if (a->count != b->count || memcmp(a->fds, b->fds, (size_t)a->count * sizeof(*a->fds))) return 0;
  for (int i = 0; i < a->count; i++)
    if (a->fds[i].proc_fdtype == PROX_FDTYPE_VNODE &&
        memcmp(&a->vnodes[i], &b->vnodes[i], sizeof(*a->vnodes))) return 0;
  return 1;
}

static int member_matches(const struct family_member *member, const struct vnode_fdinfo *value) {
  const struct vinfo_stat *s = &value->pvi.vi_stat;
  const struct stat *expected = &member->object.info;
  if (s->vst_dev != (uint32_t)expected->st_dev || s->vst_ino != member->object.file_id ||
      memcmp(&value->pvi.vi_fsid, &member->object.mount.f_fsid, sizeof(fsid_t))) return 0;
  if (!(s->vst_uid == expected->st_uid && s->vst_gid == expected->st_gid &&
    s->vst_mode == expected->st_mode && s->vst_nlink == expected->st_nlink &&
    s->vst_size == expected->st_size && s->vst_flags == expected->st_flags &&
    s->vst_birthtime == expected->st_birthtimespec.tv_sec &&
    s->vst_birthtimensec == expected->st_birthtimespec.tv_nsec))
    return refuse("family-vnode-mismatch", 0, 0);
  return 1;
}

static const char *fd_access(const struct vnode_fdinfo *value) {
  if (value->pfi.fi_openflags & O_EVTONLY) return "event-only";
  /* Kernel fg_flag uses FREAD/FWRITE (1/2), not user O_RDONLY/O_RDWR. */
  switch (value->pfi.fi_openflags & 3) {
    case 1: return "read-only";
    case 2: return "write-only";
    case 3: return "read-write";
    default: return "none";
  }
}

static const char *fd_exclusion(int owner, int fd, const struct vnode_fdinfo *value,
                                const int *excluded, int count) {
  if (owner) return "owner";
  for (int i = 0; i < count; i++) if (fd == excluded[i]) return "pre-open-inherited";
  if (value->pfi.fi_openflags & O_EVTONLY) return "event-only";
  if (value->pfi.fi_status & PROC_FP_SHARED) return "shared";
  if (!(value->pfi.fi_openflags & 1)) return "not-readable";
  return NULL;
}

static void print_process(const struct process_identity *p, const char *role, int count) {
  printf("{\"role\":\"%s\",\"pid\":%d,\"parentPid\":%d,\"uniqueId\":\"%" PRIu64
         "\",\"parentUniqueId\":\"%" PRIu64 "\",\"pidVersion\":%u,\"uid\":%u,\"fdCount\":%d}",
         role, p->pid, p->parent_pid, p->unique_id, p->parent_unique_id, p->version, p->uid, count);
}

/* The caller binds its captured lifecycle, not serialized adoption authority.
   Family witnesses are inherited by this helper only, never opened in the
   worker as surrogate SQLite handles. Exclusions are source-fixed descriptors
   recorded before spawning the worker, not a worker-selected post-open list.
   Discovery covers supplied present objects only; omitted/unlinked sidecars,
   descriptor origin, SQLite close and child reap remain unproved. */
static int observe_worker(int argc, char **argv) {
  int capture = !strcmp(argv[1], "capture-worker");
  const char *operation = capture ? "capture-worker" : "observe-worker";
  struct family_member members[MAX_FAMILY_MEMBERS] = {0};
  struct process_identity owner, worker, after;
  struct fd_snapshot owner_fds = {0}, worker_fds = {0}, fresh = {0};
  int excluded[MAX_EXCLUDED_FDS], excluded_count = 0;
  int checked = 0, count = 0, seen = 0;
  uint64_t inputs[9] = {0};
  const uint64_t maxima[9] = {UINT64_MAX, UINT32_MAX, UINT64_MAX, UINT32_MAX,
    INT_MAX, UINT64_MAX, UINT32_MAX, UINT64_MAX, UINT32_MAX};
  const char *names[] = {"--database", "--wal", "--shm", "--journal"};
  const char *suffixes[] = {"", "-wal", "-shm", "-journal"};
  uuid_string_t boot, after_boot;
  pid_t parent = getppid();
  if (getuid() == 0 || getuid() != geteuid() || parent <= 1) {
    refuse("rootless-owner-required", 0, 0); goto failed_worker;
  }
  int first = capture ? 3 : 11;
  if (argc < first + 3 || argc > first + 2 * MAX_EXCLUDED_FDS + 3 * MAX_FAMILY_MEMBERS) {
    refuse("invalid-process-arguments", 0, 0); goto failed_worker;
  }
  if (capture) {
    if (decimal(argv[2], INT_MAX, &inputs[4])) goto failed_worker;
  } else {
    for (int i = 0; i < 9; i++) if (decimal(argv[i + 2], maxima[i], &inputs[i])) goto failed_worker;
  }
  if (inputs[4] <= 1 || inputs[4] == (uint64_t)parent || inputs[4] == (uint64_t)getpid() ||
      (!capture && (!inputs[0] || !inputs[1] || inputs[3] != getuid() || !inputs[5] ||
                    !inputs[6] || inputs[7] != inputs[0] || inputs[8] != getuid()))) {
    refuse("invalid-process-binding", 0, 0); goto failed_worker;
  }
  if (read_process(parent, &owner)) goto failed_worker;
  if (!capture && (owner.unique_id != inputs[0] || owner.version != inputs[1] ||
      owner.parent_unique_id != inputs[2] || owner.uid != inputs[3])) {
    refuse("owner-identity-mismatch", 0, 0); goto failed_worker;
  }
  if (read_process((pid_t)inputs[4], &worker)) goto failed_worker;
  if (worker.parent_unique_id != owner.unique_id || worker.parent_pid != parent ||
      (!capture && (worker.unique_id != inputs[5] || worker.version != inputs[6] || worker.uid != inputs[8]))) {
    refuse("worker-identity-mismatch", 0, 0); goto failed_worker;
  }
  int arg = first;
  while (arg < argc && !strcmp(argv[arg], "--exclude-worker-fd")) {
    uint64_t value;
    if (arg + 1 >= argc || excluded_count == MAX_EXCLUDED_FDS ||
        decimal(argv[arg + 1], INT_MAX, &value)) {
      refuse("invalid-excluded-fd", 0, 0); goto failed_worker;
    }
    for (int i = 0; i < excluded_count; i++)
      if (excluded[i] == (int)value) { refuse("duplicate-excluded-fd", 0, 0); goto failed_worker; }
    excluded[excluded_count++] = (int)value;
    arg += 2;
  }
  if (arg == argc || (argc - arg) % 3 || (argc - arg) / 3 > MAX_FAMILY_MEMBERS) {
    refuse("invalid-family-arguments", 0, 0); goto failed_worker;
  }
  count = (argc - arg) / 3;
  size_t path_bytes = 0;
  for (int i = 0; i < count; i++) {
    struct family_member *member = &members[i];
    int kind = -1;
    for (int j = 0; j < MAX_FAMILY_MEMBERS; j++)
      if (!strcmp(argv[arg + i * 3], names[j])) kind = j;
    if (kind < 0 || (seen & (1 << kind)) || (i == 0 && kind != 0)) {
      refuse("invalid-family-role", 0, 0); goto failed_worker;
    }
    seen |= 1 << kind;
    uint64_t local;
    if (decimal(argv[arg + i * 3 + 1], INT_MAX, &local)) goto failed_worker;
    if (local < 3 || fcntl((int)local, F_GETFD) < 0) {
      refuse("invalid-inherited-fd", errno, 0); goto failed_worker;
    }
    for (int j = 0; j < i; j++)
      if (members[j].object.fd == (int)local) { refuse("duplicate-family-fd", 0, 0); goto failed_worker; }
    member->name = names[kind] + 2;
    member->object.fd = (int)local;
    member->object.path = argv[arg + i * 3 + 2];
    checked++;
    path_bytes += strlen(member->object.path);
    if (path_bytes > MAX_PATH_BYTES || canonical_path(member->object.path)) goto failed_worker;
    if (i) {
      size_t base = strlen(members[0].object.path), suffix = strlen(suffixes[kind]);
      if (strlen(member->object.path) != base + suffix ||
          strncmp(member->object.path, members[0].object.path, base) ||
          strcmp(member->object.path + base, suffixes[kind])) {
        refuse("invalid-family-path", 0, 0); goto failed_worker;
      }
    }
  }
  if (boot_uuid(boot)) goto failed_worker;
  for (int i = 0; i < count; i++) {
    struct observation *object = &members[i].object;
    if (read_object(object) || !timestamp_supported(object->info.st_mtimespec) ||
        !timestamp_supported(object->info.st_ctimespec) ||
        !timestamp_supported(object->info.st_birthtimespec)) goto failed_worker;
    for (int j = 0; j < i; j++)
      if (object->file_id == members[j].object.file_id &&
          !strcmp(object->volume_uuid, members[j].object.volume_uuid)) {
        refuse("aliased-family-object", 0, 0); goto failed_worker;
      }
  }
  if (read_snapshot(parent, &owner_fds) || read_snapshot(worker.pid, &worker_fds)) goto failed_worker;
  if (read_snapshot(parent, &fresh)) goto failed_worker;
  if (!same_snapshot(&fresh, &owner_fds)) { refuse("owner-fd-snapshot-changed", 0, 0); goto failed_worker; }
  if (read_snapshot(worker.pid, &fresh)) goto failed_worker;
  if (!same_snapshot(&fresh, &worker_fds)) { refuse("worker-fd-snapshot-changed", 0, 0); goto failed_worker; }
  for (int i = 0; i < count; i++) {
    struct observation object = members[i].object;
    if (read_object(&object)) goto failed_worker;
    if (!same_stat(&object.info, &members[i].object.info) ||
        strcmp(object.volume_uuid, members[i].object.volume_uuid) ||
        object.file_id != members[i].object.file_id) {
      refuse("family-object-changed", 0, 0); goto failed_worker;
    }
  }
  if (read_process(worker.pid, &after) || !same_process(&worker, &after) ||
      read_process(parent, &after) || !same_process(&owner, &after) || getppid() != parent ||
      boot_uuid(after_boot) || strcmp(boot, after_boot)) {
    refuse("process-bracket-changed", 0, 0); goto failed_worker;
  }
  for (int process = 0; process < 2; process++) {
    const struct fd_snapshot *snapshot = process ? &worker_fds : &owner_fds;
    for (int i = 0; i < snapshot->count; i++)
      if (snapshot->fds[i].proc_fdtype == PROX_FDTYPE_VNODE)
        for (int j = 0; j < count; j++)
          if (member_matches(&members[j], &snapshot->vnodes[i]) < 0) goto failed_worker;
  }
  printf("{\"schema\":1,\"operation\":\"%s\",\"status\":\"observed\",\"bootSessionUuid\":\"%s\",\"processes\":[", operation, boot);
  print_process(&owner, "owner", owner_fds.count); putchar(','); print_process(&worker, "worker", worker_fds.count);
  printf("],\"excludedWorkerFds\":[");
  for (int i = 0; i < excluded_count; i++) { if (i) putchar(','); printf("%d", excluded[i]); }
  printf("],\"family\":[");
  for (int i = 0; i < count; i++) {
    if (i) putchar(',');
    printf("{\"role\":\"%s\",\"object\":", members[i].name);
    print_object(&members[i].object, i); putchar('}');
  }
  printf("],\"matches\":[");
  int printed = 0;
  for (int process = 0; process < 2; process++) {
    const struct fd_snapshot *snapshot = process ? &worker_fds : &owner_fds;
    for (int i = 0; i < snapshot->count; i++) {
      if (snapshot->fds[i].proc_fdtype != PROX_FDTYPE_VNODE) continue;
      for (int j = 0; j < count; j++) {
        const struct vnode_fdinfo *value = &snapshot->vnodes[i];
        if (!member_matches(&members[j], value)) continue;
        int fd = snapshot->fds[i].proc_fd;
        const char *exclusion = fd_exclusion(!process, fd, value, excluded, excluded_count);
        if (printed++) putchar(',');
        printf("{\"process\":\"%s\",\"familyRole\":\"%s\",\"fd\":%d,\"access\":\"%s\",\"shared\":%s,"
               "\"eligibleOpenObservation\":%s,\"exclusion\":",
          process ? "worker" : "owner", members[j].name, fd, fd_access(value),
          (value->pfi.fi_status & PROC_FP_SHARED) ? "true" : "false", exclusion ? "false" : "true");
        if (exclusion) json_string(exclusion); else printf("null");
        putchar('}');
      }
    }
  }
  for (int i = 0; i < checked; i++) close(members[i].object.fd);
  puts("],\"suppliedFamilyOnly\":true,\"familyMembershipComplete\":false,\"observationOnly\":true,"
       "\"sqliteOpenProved\":false,\"databaseSyncCloseProved\":false,\"workerReaped\":false,"
       "\"stateAdmissionAllowed\":false,\"launchMembershipComplete\":false,\"processCustody\":\"unobserved\",\"activationAllowed\":false}");
  return fflush(stdout) == 0 ? 0 : 1;
failed_worker:
  for (int i = 0; i < checked; i++) close(members[i].object.fd);
  printf("{\"schema\":1,\"operation\":\"%s\",\"status\":\"refused\",\"reason\":\"%s\",\"errno\":%d,"
         "\"processes\":[],\"excludedWorkerFds\":[],\"family\":[],\"matches\":[],"
         "\"suppliedFamilyOnly\":true,\"familyMembershipComplete\":false,\"observationOnly\":true,\"sqliteOpenProved\":false,"
         "\"databaseSyncCloseProved\":false,\"workerReaped\":false,\"stateAdmissionAllowed\":false,"
         "\"launchMembershipComplete\":false,\"processCustody\":\"unobserved\",\"activationAllowed\":false}\n", operation, reason, failure_errno);
  return 1;
}
#endif

int main(int argc, char **argv) {
#ifndef __APPLE__
  (void)argc; (void)argv;
  puts("{\"schema\":1,\"operation\":\"unsupported\",\"status\":\"unsupported\",\"reason\":\"darwin-required\",\"objects\":[],\"observationOnly\":true,\"activationAllowed\":false}");
  return 77;
#else
  if (argc > 1 && (!strcmp(argv[1], "observe-worker") || !strcmp(argv[1], "capture-worker"))) return observe_worker(argc, argv);
  struct observation objects[MAX_OBJECTS] = {0};
  int count = 0, checked = 0, failed_index = -1;
  const char *operation = argc > 1 && strcmp(argv[1], "boot") == 0 ? "boot" : "inspect";
  uuid_string_t boot, after_boot;
  if (getuid() == 0 || getuid() != geteuid()) { refuse("rootless-owner-required", 0, 0); goto failed; }
  if (argc == 2 && strcmp(argv[1], "boot") == 0) {
    if (boot_uuid(boot) != 0) goto failed;
  } else {
    if (argc < 5 || strcmp(argv[1], "inspect") || (argc - 2) % 3 || (argc - 2) / 3 > MAX_OBJECTS) {
      refuse("invalid-arguments", 0, 0); goto failed;
    }
    count = (argc - 2) / 3;
    size_t path_bytes = 0;
    /* Validate every inherited descriptor before opening internal descriptors,
       so a missing input FD cannot accidentally bind to our subsequent open. */
    for (int index = 0; index < count; index++) {
      failed_index = index;
      const char *kind = argv[2 + 3 * index], *number = argv[3 + 3 * index];
      char *end = NULL;
      errno = 0;
      long fd = strtol(number, &end, 10);
      if ((strcmp(kind, "--file") && strcmp(kind, "--directory")) || number[0] < '0' || number[0] > '9' ||
          *end != '\0' || errno || fd < 3 || fd > INT_MAX || fcntl((int)fd, F_GETFD) < 0) {
        refuse("invalid-inherited-fd", errno, 0); goto failed;
      }
      for (int prior = 0; prior < index; prior++) {
        if (objects[prior].fd == fd) { refuse("duplicate-inherited-fd", 0, 0); goto failed; }
      }
      objects[index].fd = (int)fd;
      objects[index].directory = strcmp(kind, "--directory") == 0;
      objects[index].path = argv[4 + 3 * index];
      checked++;
      path_bytes += strlen(objects[index].path);
      if (path_bytes > MAX_PATH_BYTES || canonical_path(objects[index].path) != 0) {
        refuse("invalid-path-bound", 0, 0); goto failed;
      }
    }
    failed_index = -1;
    if (boot_uuid(boot) != 0) goto failed;
    for (int index = 0; index < count; index++) {
      failed_index = index;
      if (read_object(&objects[index]) != 0) goto failed;
      if (!timestamp_supported(objects[index].info.st_mtimespec) ||
          !timestamp_supported(objects[index].info.st_ctimespec) ||
          !timestamp_supported(objects[index].info.st_birthtimespec)) {
        refuse("timestamp-schema-unavailable", 0, 1); goto failed;
      }
    }
    for (int index = 0; index < count; index++) {
      failed_index = index;
      struct observation fresh = objects[index];
      if (read_object(&fresh) != 0) goto failed;
      if (!same_stat(&fresh.info, &objects[index].info) || fresh.file_id != objects[index].file_id ||
          strcmp(fresh.volume_uuid, objects[index].volume_uuid) ||
          memcmp(&fresh.mount.f_fsid, &objects[index].mount.f_fsid, sizeof(fsid_t)) ||
          fresh.mount.f_flags != objects[index].mount.f_flags ||
          strcmp(fresh.mount.f_mntonname, objects[index].mount.f_mntonname)) {
        refuse("observation-changed", 0, 0); goto failed;
      }
    }
    failed_index = -1;
    if (boot_uuid(after_boot) != 0) goto failed;
    if (strcmp(boot, after_boot)) { refuse("boot-identity-changed", 0, 0); goto failed; }
  }
  for (int index = 0; index < checked; index++) close(objects[index].fd);
  printf("{\"schema\":1,\"operation\":\"%s\",\"status\":\"observed\",\"bootSessionUuid\":\"%s\","
         "\"ancestorPolicy\":\"nofollow-currentness-only\",\"objects\":[", operation, boot);
  for (int index = 0; index < count; index++) {
    if (index) putchar(',');
    print_object(&objects[index], index);
  }
  puts("],\"observationOnly\":true,\"activationAllowed\":false}");
  return fflush(stdout) == 0 ? 0 : 1;
failed:
  for (int index = 0; index < checked; index++) close(objects[index].fd);
  printf("{\"schema\":1,\"operation\":\"%s\",\"status\":\"%s\",\"reason\":\"%s\","
         "\"errno\":%d,\"failingIndex\":%d,\"objects\":[],\"observationOnly\":true,\"activationAllowed\":false}\n",
         operation, unsupported ? "unsupported" : "refused", reason, failure_errno, failed_index);
  return unsupported ? 77 : 1;
#endif
}
