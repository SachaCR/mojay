import { themes as prismThemes } from "prism-react-renderer";
import type { Config } from "@docusaurus/types";
import type * as Preset from "@docusaurus/preset-classic";

// This runs in Node.js: no browser APIs or JSX here.

/** The repository, in one place: links, "edit this page", the footer. */
const GITHUB_REPO = "SachaCR/mojay";
const GITHUB_URL = `https://github.com/${GITHUB_REPO}`;
const EDIT_URL = `${GITHUB_URL}/tree/main/website/`;

const config: Config = {
  title: "Mojay",
  tagline: "SQLite for Kafka",
  favicon: "img/logo.svg",

  future: {
    v4: true,
  },

  url: "https://mojay.dev",
  baseUrl: "/",
  trailingSlash: false,

  organizationName: GITHUB_REPO.split("/")[0],
  projectName: GITHUB_REPO.split("/")[1],

  onBrokenLinks: "throw",

  i18n: {
    defaultLocale: "en",
    locales: ["en"],
  },

  headTags: [
    {
      tagName: "link",
      attributes: { rel: "preconnect", href: "https://fonts.googleapis.com" },
    },
    {
      tagName: "link",
      attributes: {
        rel: "preconnect",
        href: "https://fonts.gstatic.com",
        crossorigin: "anonymous",
      },
    },
  ],
  stylesheets: [
    "https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600&family=Space+Grotesk:wght@500;600;700&display=swap",
  ],

  presets: [
    [
      "classic",
      {
        docs: {
          sidebarPath: "./sidebars.ts",
          editUrl: EDIT_URL,
        },
        blog: {
          showReadingTime: true,
          feedOptions: {
            type: ["rss", "atom"],
            xslt: true,
          },
          editUrl: EDIT_URL,
          onInlineTags: "throw",
          onInlineAuthors: "throw",
          onUntruncatedBlogPosts: "throw",
        },
        theme: {
          customCss: "./src/css/custom.css",
        },
      } satisfies Preset.Options,
    ],
  ],

  themeConfig: {
    image: "img/social-card.png",
    colorMode: {
      respectPrefersColorScheme: true,
    },
    navbar: {
      title: "Mojay",
      logo: {
        alt: "Mojay logo",
        src: "img/logo.svg",
      },
      items: [
        {
          type: "docSidebar",
          sidebarId: "docs",
          position: "left",
          label: "Docs",
        },
        { to: "/blog", label: "Blog", position: "left" },
        {
          href: "https://docs.rs/mojay",
          label: "API",
          position: "left",
        },
        {
          href: GITHUB_URL,
          label: "GitHub",
          position: "right",
        },
      ],
    },
    footer: {
      style: "dark",
      links: [
        {
          title: "Docs",
          items: [
            { label: "Introduction", to: "/docs/intro" },
            { label: "Getting started", to: "/docs/getting-started/rust" },
            { label: "How it works", to: "/docs/how-it-works/on-disk-layout" },
            { label: "Benchmarks", to: "/docs/benchmarks" },
          ],
        },
        {
          title: "Packages",
          items: [
            { label: "crates.io", href: "https://crates.io/crates/mojay" },
            { label: "npm", href: "https://www.npmjs.com/package/@mojay-dev/node" },
            { label: "API reference (docs.rs)", href: "https://docs.rs/mojay" },
          ],
        },
        {
          title: "More",
          items: [
            { label: "Blog", to: "/blog" },
            { label: "GitHub", href: GITHUB_URL },
          ],
        },
      ],
      copyright: `Mojay is dual-licensed under MIT or Apache-2.0. Built with Docusaurus.`,
    },
    prism: {
      theme: prismThemes.oneLight,
      darkTheme: prismThemes.oneDark,
      additionalLanguages: ["rust", "toml", "bash"],
    },
    mermaid: {
      theme: { light: "neutral", dark: "dark" },
    },
  } satisfies Preset.ThemeConfig,

  markdown: {
    mermaid: true,
  },
  themes: ["@docusaurus/theme-mermaid"],
};

export default config;
