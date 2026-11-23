# Releasing Mojay

A release publishes, from one tag, the crates `mojay` and `mojay-cli` to
crates.io, the `mojay` package to npm (with prebuilt binaries for macOS and
Linux), and a GitHub release. `.github/workflows/release.yml` does all of it
when a tag `v<version>` is pushed.

A published version can never be replaced or reused, on crates.io or on npm.

## One-time setup

1. **crates.io**: sign in with GitHub on https://crates.io, verify your email,
   then create an API token (Account Settings → API Tokens) with the scopes
   `publish-new` and `publish-update`. Save it as the repository secret
   `CARGO_REGISTRY_TOKEN` (GitHub → Settings → Secrets and variables → Actions).
2. **npm**: create an account on https://www.npmjs.com with two-factor
   authentication, then a granular access token with read and write access to
   packages (it must be allowed to publish a new package for the first
   release). Save it as the repository secret `NPM_TOKEN`.

After the first release, both registries can use trusted publishing from
GitHub Actions instead of long-lived tokens: crates.io (crate settings →
Trusted Publishing) and npm (package settings → Trusted Publisher). The
workflow would then need `id-token: write` on the crates job and no token.

## Steps

1. Make sure the CI is green on `main`. To check the release itself without
   publishing anything, run the **Release** workflow by hand (Actions →
   Release → Run workflow): it builds and tests every binary and does a dry
   run of both publishes.
2. Set the new version everywhere, for example `0.2.0`:
   - `version` in `Cargo.toml` and `cli/Cargo.toml`, and the `version` of the
     `mojay` dependency in `cli/Cargo.toml`;
   - the npm package: from `bindings/node`, `npm version 0.2.0 --no-git-tag-version`
     (updates `package.json` and `package-lock.json`).
3. In `CHANGELOG.md`, turn the section into `## [0.2.0] - <date>` and add its
   link at the bottom. The release notes are taken from this section.
4. Run the checks of `AGENT.md` (section 5), then commit:
   `Release 0.2.0`.
5. Tag and push:

   ```bash
   git tag -a v0.2.0 -m "Mojay 0.2.0"
   ```

   ```bash
   git push origin main v0.2.0
   ```

6. Follow the **Release** workflow in the Actions tab. It checks that the tag,
   the crates and the npm package have the same version and that the changelog
   has its section, builds and tests the Node.js binaries on every platform,
   then publishes. If a step fails after something was published, fix the
   cause and re-run the failed jobs: versions already published are skipped.

## After the first release

The website and the READMEs mention how to use Mojay "before the first
release" (from the Git repository): remove those notes once `cargo add mojay`
and `npm install mojay` work.
