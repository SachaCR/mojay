/**
 * The install pill from the homepage hero: one tab per package manager, the
 * command, a copy button, and a link to the package's registry page.
 *
 * Adapted from Ontologic's pill. Styling is the component's; placement is the
 * caller's, passed as `className`.
 */

import type { ReactNode } from "react";
import { useState } from "react";
import clsx from "clsx";
import Link from "@docusaurus/Link";

import styles from "./styles.module.css";

export type InstallOption = {
  /** The tab label, e.g. "Rust". */
  label: string;
  /** The command as the reader should copy it, verbatim. */
  command: string;
  /** The package's page on its registry. */
  registryUrl: string;
  /** The registry's name, for the link's title. */
  registryName: string;
};

function CopyButton({ text }: { text: string }) {
  const [copied, setCopied] = useState(false);

  function handleCopy() {
    navigator.clipboard.writeText(text).then(
      () => {
        setCopied(true);
        setTimeout(() => setCopied(false), 2000);
      },
      // Clipboard access refused (permissions, embedded browser): the
      // command stays visible and selectable, so there is nothing to undo.
      () => {},
    );
  }

  return (
    <button
      type="button"
      className={styles.copyButton}
      onClick={handleCopy}
      aria-label={copied ? "Copied" : "Copy to clipboard"}
      title="Copy to clipboard"
    >
      {copied ? (
        <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
          <polyline points="20 6 9 17 4 12" />
        </svg>
      ) : (
        <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
          <rect x="9" y="9" width="13" height="13" rx="2" ry="2" />
          <path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1" />
        </svg>
      )}
    </button>
  );
}

function PackageIcon() {
  return (
    <svg width="17" height="17" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
      <path d="M21 16V8a2 2 0 0 0-1-1.73l-7-4a2 2 0 0 0-2 0l-7 4A2 2 0 0 0 3 8v8a2 2 0 0 0 1 1.73l7 4a2 2 0 0 0 2 0l7-4A2 2 0 0 0 21 16z" />
      <polyline points="3.27 6.96 12 12.01 20.73 6.96" />
      <line x1="12" y1="22.08" x2="12" y2="12" />
    </svg>
  );
}

type Props = {
  options: InstallOption[];
  /** Placement only: margins and width belong to the caller. */
  className?: string;
};

export default function InstallCommand({ options, className }: Props): ReactNode {
  const [selected, setSelected] = useState(0);
  const option = options[selected];

  return (
    <div className={clsx(styles.wrapper, className)}>
      <div className={styles.tabs} role="tablist" aria-label="Install with">
        {options.map((o, index) => (
          <button
            key={o.label}
            type="button"
            role="tab"
            aria-selected={index === selected}
            className={clsx(styles.tab, index === selected && styles.tabActive)}
            onClick={() => setSelected(index)}
          >
            {o.label}
          </button>
        ))}
      </div>
      <div className={styles.install} role="tabpanel">
        <span className={styles.prompt} aria-hidden="true">$</span>
        <code>{option.command}</code>
        <CopyButton text={option.command} />
        <Link
          to={option.registryUrl}
          className={styles.registryLink}
          title={`View on ${option.registryName}`}
          aria-label={`View on ${option.registryName}`}
        >
          <PackageIcon />
        </Link>
      </div>
    </div>
  );
}
