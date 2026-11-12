/* Test-only SDK oracle. No addresses or identities are written to an output stream. */
#include <libproc.h>
#include <sys/proc_info.h>
#include <sys/socket.h>
#include <sys/ioctl.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdint.h>
#include <stddef.h>
#include <string.h>
#include <Availability.h>
#if !defined(__APPLE__) || !defined(__aarch64__)
#error This oracle is restricted to Darwin arm64.
#endif
#if __MAC_OS_X_VERSION_MAX_ALLOWED != 150200
#error The candidate layout requires the macOS 15.2 SDK.
#endif

/* Private ABI source, distinct from the SDK crash-info structure:
 * apple-oss-distributions/xnu commit 043036a2b3718f7f0be807e2870f8f47d3fa0796,
 * bsd/sys/proc_info_private.h:45-55,146-148 (xnu-12377.81.4).
 * This source correspondence is not a runtime qualification. */
struct cold_unique {
  uint8_t uuid[16]; uint64_t id, parent; int32_t version, original_parent_version;
  uint64_t reserve2, reserve3;
};
_Static_assert(sizeof(struct cold_unique) == 56, "private XNU source layout");

int cold_oracle_layout(uint32_t *out, int length) {
  const uint32_t values[] = {
    sizeof(struct proc_bsdinfo), sizeof(struct socket_fdinfo), sizeof(struct vnode_fdinfo), sizeof(struct proc_fdinfo),
    offsetof(struct proc_bsdinfo, pbi_pid), offsetof(struct proc_bsdinfo, pbi_ppid), offsetof(struct proc_bsdinfo, pbi_uid),
    offsetof(struct proc_bsdinfo, pbi_start_tvsec), offsetof(struct proc_bsdinfo, pbi_start_tvusec),
    offsetof(struct socket_fdinfo, pfi.fi_status), offsetof(struct socket_fdinfo, psi.soi_so),
    offsetof(struct socket_fdinfo, psi.soi_type), offsetof(struct socket_fdinfo, psi.soi_family),
    offsetof(struct socket_fdinfo, psi.soi_kind), offsetof(struct socket_fdinfo, psi.soi_proto.pri_un.unsi_conn_so),
    offsetof(struct vnode_fdinfo, pvi.vi_stat.vst_dev), offsetof(struct vnode_fdinfo, pvi.vi_stat.vst_mode),
    offsetof(struct vnode_fdinfo, pvi.vi_stat.vst_ino), F_GETFD, FD_CLOEXEC, FIOCLEX, PROC_FP_CLEXEC,
    PROC_PIDTBSDINFO, PROC_PIDFDSOCKETINFO, PROC_PIDFDVNODEINFO, PROC_PIDLISTFDS, PROX_FDTYPE_SOCKET,
    sizeof(struct cold_unique), offsetof(struct cold_unique, id), offsetof(struct cold_unique, parent), offsetof(struct cold_unique, version)
  };
  if (length != (int)sizeof(values)) return -1;
  memcpy(out, values, sizeof(values)); return 0;
}

/* Fixed selectors identify only self/parent and inherited fixture descriptors 3..5. */
int cold_oracle_capture(int kind, void *out, int length) {
  int size = kind < 2 ? sizeof(struct proc_bsdinfo) : kind < 4 ? sizeof(struct socket_fdinfo) :
    kind == 4 ? sizeof(struct vnode_fdinfo) : kind < 7 ? sizeof(struct cold_unique) : kind < 9 ? sizeof(struct socket_fdinfo) : 0;
  if (kind < 0 || !size || length != size) return -1;
  memset(out, 0, length);
  if (kind < 2) return proc_pidinfo(kind ? getppid() : getpid(), PROC_PIDTBSDINFO, 0, out, length);
  if (kind < 4) return proc_pidfdinfo(getpid(), kind == 2 ? 4 : 5, PROC_PIDFDSOCKETINFO, out, length);
  if (kind == 4) return proc_pidfdinfo(getpid(), 3, PROC_PIDFDVNODEINFO, out, length);
  if (kind >= 7) return proc_pidfdinfo(getpid(), kind == 7 ? 3 : -1, PROC_PIDFDSOCKETINFO, out, length);
  return proc_pidinfo(kind == 5 ? getpid() : getppid(), 17, 0, out, length);
}

/* Compare compact JS-decoded values with a fresh independent native read. */
int cold_oracle_compare(int kind, const void *input, int length) {
  uint64_t supplied[5] = {0}, expected[5] = {0}; int count;
  if (kind < 0 || kind > 6) return 0;
  if (kind < 2) {
    struct proc_bsdinfo value;
    if (cold_oracle_capture(kind, &value, sizeof(value)) != (int)sizeof(value)) return 0;
    count = 5; expected[0] = value.pbi_pid; expected[1] = value.pbi_ppid; expected[2] = value.pbi_uid;
    expected[3] = value.pbi_start_tvsec; expected[4] = value.pbi_start_tvusec;
  } else if (kind < 4) {
    struct socket_fdinfo value;
    if (cold_oracle_capture(kind, &value, sizeof(value)) != (int)sizeof(value)) return 0;
    count = 5; expected[0] = value.psi.soi_so; expected[1] = value.psi.soi_proto.pri_un.unsi_conn_so;
    expected[2] = value.psi.soi_type == SOCK_STREAM && value.psi.soi_family == AF_UNIX && value.psi.soi_kind == SOCKINFO_UN;
    expected[3] = value.psi.soi_so != 0 && value.psi.soi_proto.pri_un.unsi_conn_so != 0;
    expected[4] = (value.pfi.fi_status & PROC_FP_CLEXEC) != 0;
  } else if (kind == 4) {
    struct vnode_fdinfo value;
    if (cold_oracle_capture(kind, &value, sizeof(value)) != (int)sizeof(value)) return 0;
    count = 3; expected[0] = (uint32_t)value.pvi.vi_stat.vst_dev; expected[1] = value.pvi.vi_stat.vst_ino;
    expected[2] = (value.pvi.vi_stat.vst_mode & 0170000) == 0100000;
  } else {
    struct cold_unique value;
    if (cold_oracle_capture(kind, &value, sizeof(value)) != (int)sizeof(value)) return 0;
    count = 3; expected[0] = value.id; expected[1] = value.parent; expected[2] = value.version;
  }
  if (length != count * 8) return 0;
  memcpy(supplied, input, length);
  int match = !memcmp(supplied, expected, length);
  memset(supplied, 0, sizeof(supplied)); memset(expected, 0, sizeof(expected));
  return match;
}
