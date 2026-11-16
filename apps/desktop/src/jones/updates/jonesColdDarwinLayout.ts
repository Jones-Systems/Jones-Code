/** Candidate SDK 15.2 arm64 layout. Runtime observations are correlation, never admission. */
export const coldDarwinLayout = Object.freeze({
  sdk: "macosx15.2",
  architecture: "arm64",
  byteOrder: "little",
  bsd: 136,
  socket: 792,
  vnode: 176,
  fd: 8,
  privateUnique: 56,
  privateUniqueFlavor: 17,
  privateUniqueQualified: false,
  getFd: 1,
  closeOnExec: 1,
  ioctlCloseOnExec: 0x20006601,
  procCloseOnExec: 2,
  bsdFlavor: 3,
  socketFlavor: 3,
  vnodeFlavor: 1,
  listFlavor: 1,
  socketFdType: 2,
} as const);

export function coldDarwinTarget(platform: string, architecture: string, kernel: string): boolean {
  // This is an experiment target, not an OS-product or packaged-runtime qualification.
  return platform === "darwin" && architecture === "arm64" && kernel === "25.3.0";
}

function view(bytes: Uint8Array, returned: number, size: number): DataView {
  if (bytes.byteLength !== size || returned !== size) {
    throw new Error("Cold Darwin observation has an unsupported length.");
  }
  return new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
}

export function decodeColdDarwinBsd(bytes: Uint8Array, returned: number) {
  const data = view(bytes, returned, coldDarwinLayout.bsd);
  return {
    pid: data.getUint32(12, true),
    parentPid: data.getUint32(16, true),
    uid: data.getUint32(20, true),
    startSeconds: data.getBigUint64(120, true),
    startMicroseconds: data.getBigUint64(128, true),
  };
}

export function decodeColdDarwinPrivateUnique(bytes: Uint8Array, returned: number) {
  // Flavor 17 is private XNU ABI; matching this candidate shape does not qualify it.
  const data = view(bytes, returned, coldDarwinLayout.privateUnique);
  return {
    uniqueId: data.getBigUint64(16, true),
    parentUniqueId: data.getBigUint64(24, true),
    pidVersion: data.getInt32(32, true),
  };
}

export function decodeColdDarwinSocket(bytes: Uint8Array, returned: number) {
  const data = view(bytes, returned, coldDarwinLayout.socket);
  const stream = data.getInt32(176, true) === 1;
  const unix = data.getInt32(184, true) === 1 && data.getInt32(256, true) === 3;
  const endpoint = data.getBigUint64(160, true);
  const peer = data.getBigUint64(264, true);
  return {
    streamUnix: stream && unix,
    nonzero: endpoint !== 0n && peer !== 0n,
    closeOnExec: (data.getUint32(4, true) & coldDarwinLayout.procCloseOnExec) !== 0,
    endpoint,
    peer,
  };
}

export function decodeColdDarwinVnode(bytes: Uint8Array, returned: number) {
  const data = view(bytes, returned, coldDarwinLayout.vnode);
  return {
    device: data.getUint32(24, true),
    inode: data.getBigUint64(32, true),
    regular: (data.getUint16(28, true) & 0xf000) === 0x8000,
  };
}

export function decodeColdDarwinFds(bytes: Uint8Array, returned: number) {
  if (
    bytes.byteLength !== 8192 ||
    !Number.isInteger(returned) ||
    returned <= 0 ||
    returned >= bytes.byteLength ||
    returned % coldDarwinLayout.fd !== 0
  ) {
    throw new Error("Cold Darwin descriptor scan is incomplete.");
  }
  const data = new DataView(bytes.buffer, bytes.byteOffset, returned);
  const result: Array<{ fd: number; socket: boolean }> = [];
  const seen = new Set<number>();
  for (let offset = 0; offset < returned; offset += coldDarwinLayout.fd) {
    const fd = data.getUint32(offset, true);
    if (fd > 0x7fffffff || seen.has(fd)) throw new Error("Cold Darwin descriptor scan is invalid.");
    seen.add(fd);
    result.push({ fd, socket: data.getUint32(offset + 4, true) === coldDarwinLayout.socketFdType });
  }
  return result;
}

export function coldDarwinReciprocal(
  left: ReturnType<typeof decodeColdDarwinSocket>,
  right: ReturnType<typeof decodeColdDarwinSocket>,
): boolean {
  return (
    left.streamUnix &&
    right.streamUnix &&
    left.nonzero &&
    right.nonzero &&
    left.endpoint === right.peer &&
    right.endpoint === left.peer
  );
}
