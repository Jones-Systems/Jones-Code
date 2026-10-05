declare module "*.json.fixture?raw" {
  const contents: string;
  export default contents;
}

declare module "node:crypto" {
  interface Hash {
    update(value: string): Hash;
    digest(encoding: "hex"): string;
  }
  export function createHash(algorithm: "sha256"): Hash;
}
