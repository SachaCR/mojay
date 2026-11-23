#!/usr/bin/env bash
# Prepares a release in the working tree, without committing anything: writes
# the new version everywhere it appears, and turns the "Unreleased" section of
# CHANGELOG.md into the section of that version. Run by the Release workflow
# (.github/workflows/release.yml); safe to run locally to see what it changes.
#
# Usage: prepare-release.sh <patch|minor|major|none> <date YYYY-MM-DD>
#
# "none" changes nothing: it releases the version already written, for
# example after a run that pushed its tag but failed to publish.
#
# Prints the version of the release on its last line.

set -euo pipefail

readonly REPOSITORY_URL="https://github.com/SachaCR/mojay"
# The crates and the npm package are released together, with one version.
readonly CRATE_MANIFESTS=(Cargo.toml cli/Cargo.toml bindings/node/Cargo.toml)

fail() {
  echo "::error::$*" >&2
  exit 1
}

# The version of the `mojay` crate, the reference for all the others.
current_version() {
  sed -n 's/^version = "\(.*\)"$/\1/p' Cargo.toml | head -n 1
}

next_version() {
  local version=$1 release_type=$2 major minor patch
  IFS=. read -r major minor patch <<<"$version"
  case "$release_type" in
    major) echo "$((major + 1)).0.0" ;;
    minor) echo "$major.$((minor + 1)).0" ;;
    patch) echo "$major.$minor.$((patch + 1))" ;;
    none) echo "$version" ;;
    *) fail "Unknown release type '$release_type' (patch, minor, major or none)" ;;
  esac
}

set_version() {
  local old=$1 new=$2 manifest
  for manifest in "${CRATE_MANIFESTS[@]}"; do
    # The first `version =` line is the package's own.
    OLD=$old NEW=$new perl -i -pe \
      's/^version = "\Q$ENV{OLD}\E"$/version = "$ENV{NEW}"/ if !$done && /^version = / && ($done = 1)' \
      "$manifest"
  done
  # The CLI depends on the library of the same release.
  OLD=$old NEW=$new perl -i -pe \
    's/^(mojay = \{ path = "\.\.", version = ")\Q$ENV{OLD}\E"/$1$ENV{NEW}"/' cli/Cargo.toml

  (cd bindings/node && npm version "$new" --no-git-tag-version >/dev/null)
  # napi-rs writes the package version into its loader, to check the binaries
  # it loads. `napi build` regenerates it, and the workflow checks the two agree.
  OLD=$old NEW=$new perl -i -pe 's/(?<![\d.])\Q$ENV{OLD}\E(?![\d.])/$ENV{NEW}/g' bindings/node/index.js

  cargo update --workspace --quiet
}

# Renames "## [Unreleased]" to the new version, opens an empty "Unreleased"
# above it, and adds the link of the new version. Refuses an empty section:
# a release without a note of what changed is a mistake.
release_changelog() {
  local version=$1 date=$2
  grep -q '^## \[Unreleased\]$' CHANGELOG.md || fail "CHANGELOG.md has no '## [Unreleased]' section"
  local notes
  notes=$(awk '/^## \[Unreleased\]$/ { inside = 1; next } /^## \[/ { inside = 0 } inside && NF' CHANGELOG.md)
  [ -n "$notes" ] || fail "The '## [Unreleased]' section of CHANGELOG.md is empty: note what this release changes"

  VERSION=$version DATE=$date URL=$REPOSITORY_URL perl -i -pe '
    s/^## \[Unreleased\]$/## [Unreleased]\n\n## [$ENV{VERSION}] - $ENV{DATE}/;
    s/^(\[\d+\.\d+\.\d+\]: )/[$ENV{VERSION}]: $ENV{URL}\/releases\/tag\/v$ENV{VERSION}\n$1/ if /^\[\d+\.\d+\.\d+\]: / && !$linked++;
  ' CHANGELOG.md
}

main() {
  [ $# -eq 2 ] || fail "Usage: prepare-release.sh <patch|minor|major|none> <date YYYY-MM-DD>"
  local release_type=$1 date=$2 old new
  old=$(current_version)
  [ -n "$old" ] || fail "No version found in Cargo.toml"
  new=$(next_version "$old" "$release_type")

  if [ "$new" != "$old" ]; then
    # The changelog first: it is the step that refuses, before anything changed.
    release_changelog "$new" "$date"
    set_version "$old" "$new"
  fi
  echo "$new"
}

main "$@"
