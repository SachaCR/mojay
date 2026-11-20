# mojay.dev

The Mojay website: homepage, documentation and blog, built with
[Docusaurus](https://docusaurus.io/). It is deployed to GitHub Pages by
`.github/workflows/website.yml` on every push to `main` that touches this
folder.

```bash
pnpm install
```

```bash
pnpm start
```

`pnpm start` serves the site with live reload on http://localhost:3000.

```bash
pnpm build
```

`pnpm build` writes the static site to `build/` and fails on any broken link.
`pnpm serve` then serves that build, and `pnpm typecheck` checks the
TypeScript.

## Where things are

| Path | Content |
| --- | --- |
| `docs/` | The documentation. The sidebar follows the folders (`_category_.json`, `sidebar_position`). |
| `docs/how-it-works/` | Mojay's internals, adapted from `docs/conception.md` (the reference, in French). |
| `blog/` | Blog posts, one folder per post; authors and tags in `authors.yml` and `tags.yml`. |
| `src/pages/index.tsx` | The homepage. |
| `src/components/` | The install pill and the animated log of the hero. |
| `src/css/custom.css` | Colors and fonts. |
| `static/` | Logo, social card, `CNAME` (mojay.dev), `robots.txt`. |

When Mojay's behavior or disk format changes, update the matching page here,
in the same commit as `docs/conception.md`.
