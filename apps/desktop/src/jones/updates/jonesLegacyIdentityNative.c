#include <errno.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#ifdef __APPLE__
#include <fcntl.h>
#include <limits.h>
#include <stddef.h>
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
#endif

int main(int argc, char **argv) {
#ifndef __APPLE__
  (void)argc; (void)argv;
  puts("{\"schema\":1,\"operation\":\"unsupported\",\"status\":\"unsupported\",\"reason\":\"darwin-required\",\"objects\":[],\"observationOnly\":true,\"activationAllowed\":false}");
  return 77;
#else
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
