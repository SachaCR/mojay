// Builds the native module for the architecture of the Node running this script.
//
// By default `napi build` targets Rust's architecture, which can differ from
// Node's (for example an x64 Node under Rosetta on an arm64 Mac): the module
// would then not be found at load time.
//
// Usage: node scripts/build.mjs [--release]

import { execFileSync, spawnSync } from 'node:child_process'

const TARGETS = {
  'darwin-arm64': 'aarch64-apple-darwin',
  'darwin-x64': 'x86_64-apple-darwin',
  'linux-x64': 'x86_64-unknown-linux-gnu',
  'linux-arm64': 'aarch64-unknown-linux-gnu',
}

const platform = `${process.platform}-${process.arch}`
const target = TARGETS[platform]
if (!target) {
  console.error(`Unsupported platform: ${platform} (Mojay targets macOS and Linux).`)
  process.exit(1)
}

const installed = execFileSync('rustup', ['target', 'list', '--installed'], { encoding: 'utf8' })
if (!installed.split('\n').includes(target)) {
  console.error(`This Node is ${platform}: it needs the Rust target ${target}.`)
  console.error(`Install it with: rustup target add ${target}`)
  process.exit(1)
}

const args = ['napi', 'build', '--platform', '--target', target, ...process.argv.slice(2)]
const result = spawnSync('npx', args, { stdio: 'inherit' })
process.exit(result.status ?? 1)
