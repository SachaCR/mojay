# Releasing Mojay

A release publishes the crates `mojay` and `mojay-cli` to crates.io, the
`@mojay-dev/node` package to npm (with prebuilt binaries for macOS and Linux),
and a GitHub release, all with the same version. It starts from a button:
the **Release** workflow (`.github/workflows/release.yml`) does the rest.

A published version can never be replaced or reused, on crates.io or on npm.

## One-time setup

1. **crates.io**: sign in with GitHub on https://crates.io, verify your email,
   then create an API token (Account Settings → API Tokens) with the scopes
   `publish-new` and `publish-update`. Save it as the repository secret
   `CARGO_REGISTRY_TOKEN` (GitHub → Settings → Secrets and variables → Actions).
2. **npm**: create an account on https://www.npmjs.com with two-factor
   authentication, and the organization `mojay-dev` (free plan): the package
   is `@mojay-dev/node`, since npm refuses the plain name `mojay` as too close
   to an existing package. The workflow publishes it with trusted publishing,
   without any token: on npmjs.com, in the settings of `@mojay-dev/node`, add a
   Trusted Publisher of type GitHub Actions with the user `SachaCR`, the
   repository `mojay` and the workflow `release.yml` (no environment).

   npm can only add a trusted publisher to a package that exists: the first
   version (0.1.0) was published by hand, see below.

crates.io supports trusted publishing too (crate settings → Trusted
Publishing). The crates job would then need `id-token: write` and the
`rust-lang/crates-io-auth-action` step instead of `CARGO_REGISTRY_TOKEN`.

## As you work

Note each visible change in `CHANGELOG.md`, under `## [Unreleased]`, in the
same commit as the change. The release turns that section into the section of
the new version, and its text becomes the notes of the GitHub release. A
release with an empty "Unreleased" section is refused.

## Releasing

1. Make sure the CI is green on `main`.
2. Actions → Release → Run workflow, from the branch `main`, and choose:
   - the **release type**: `patch` (0.1.0 → 0.1.1), `minor` (0.1.0 → 0.2.0)
     or `major`. Until 1.0, a change of API or disk format is a `minor`.
   - **dry run**, to rehearse: every step runs, nothing is pushed or
     published.
3. Follow the run. The workflow:
   1. writes the new version everywhere (`.github/scripts/prepare-release.sh`:
      the three Cargo manifests, `Cargo.lock`, `package.json`,
      `package-lock.json` and the napi-rs loader `index.js`), and turns
      "Unreleased" into `## [<version>] - <date>` in the changelog;
   2. runs the tests on Linux and macOS, checks the crates package, and
      builds and tests the four Node.js binaries;
   3. only then commits `Release <version>` on `main`, tags it
      `v<version>` and pushes both;
   4. publishes the crates and the npm package, then the GitHub release.

To see locally what step 1 changes, run the script on a clean tree, then
discard its changes:

```bash
.github/scripts/prepare-release.sh patch "$(date +%F)" && git diff --stat
```

## When a run fails

- **Before the tag** (tests, builds): nothing was pushed or published. Fix the
  cause on `main` and run the release again.
- **The push is refused**: `main` moved during the run, so the release would
  not contain what was tested. Run it again.
- **After the tag** (a publish failed): re-run the failed jobs, or run the
  workflow again with the release type `none`, which releases the version
  already on `main`. Every publish step skips what is already published.

## How 0.1.0 reached npm

The crates and `@mojay-dev/node` are both published since 0.1.0. The npm
package of 0.1.0 was published by hand (`npm publish --access public` from
`bindings/node`), with the four binaries of a rehearsal run on `main`: the
`v0.1.0` tag still had two racy Node tests, while the library and binding code
were the same. Later versions go through the workflow.
