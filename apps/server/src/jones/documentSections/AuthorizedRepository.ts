/** Server-only handle. Construct only after workspace/access checks; never decode
 * it from a request, a reference, a document, or a continuity record. */
export class AuthorizedRepository {
  readonly #root: string;
  private constructor(root: string) {
    this.#root = root;
  }

  static fromAuthorizedWorkspace(root: string): AuthorizedRepository {
    return new AuthorizedRepository(root);
  }

  static rootOf(repository: AuthorizedRepository): string {
    return repository.#root;
  }
}
