set -euo pipefail
# Bootstrap is copied into checkout steps because repository source is not available yet.
# anonymous-checkout.node-test.mjs binds every copy to this exact script.
test "$GITHUB_REPOSITORY" = Jones-Systems/Jones-Code
[[ "$JONES_CHECKOUT_SHA" =~ ^[0-9a-f]{40}$ ]]
case "$JONES_CHECKOUT_DEPTH" in 0|1|2) ;; *) exit 2 ;; esac
case "$JONES_CHECKOUT_CONE" in true|false) ;; *) exit 2 ;; esac
test ! -L "$GITHUB_WORKSPACE"
test "$(pwd -P)" = "$GITHUB_WORKSPACE"
shopt -s nullglob dotglob
checkout_entries=("$GITHUB_WORKSPACE"/*)
test "${#checkout_entries[@]}" -eq 0
unset GIT_CONFIG_PARAMETERS GIT_CONFIG GIT_DIR GIT_WORK_TREE GIT_INDEX_FILE GIT_COMMON_DIR GIT_OBJECT_DIRECTORY GIT_ALTERNATE_OBJECT_DIRECTORIES GIT_REPLACE_REF_BASE GIT_SHALLOW_FILE
export GIT_CONFIG_NOSYSTEM=1 GIT_CONFIG_SYSTEM=/dev/null GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_COUNT=0
export GIT_TERMINAL_PROMPT=0 GCM_INTERACTIVE=never GIT_ASKPASS=/bin/false SSH_ASKPASS=/bin/false
git init .
git remote add origin https://github.com/Jones-Systems/Jones-Code.git
if test "$JONES_CHECKOUT_CONE" = true; then
  git sparse-checkout init --cone
  printf '%s\n' "$JONES_CHECKOUT_SPARSE" | git sparse-checkout set --cone --stdin
else
  git sparse-checkout init --no-cone
  printf '%s\n' "$JONES_CHECKOUT_SPARSE" | git sparse-checkout set --no-cone --stdin
fi
if test "$JONES_CHECKOUT_DEPTH" = 0; then
  git -c protocol.version=2 -c credential.helper= -c core.askPass=/bin/false fetch --tags origin '+refs/heads/*:refs/remotes/origin/*' "$JONES_CHECKOUT_SHA"
else
  git -c protocol.version=2 -c credential.helper= -c core.askPass=/bin/false fetch --no-tags --depth="$JONES_CHECKOUT_DEPTH" origin "$JONES_CHECKOUT_SHA"
fi
git checkout --detach "$JONES_CHECKOUT_SHA"
test "$(git rev-parse HEAD)" = "$JONES_CHECKOUT_SHA"
