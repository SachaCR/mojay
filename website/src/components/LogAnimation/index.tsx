/**
 * The hero's picture of a partition: records are appended to the active
 * segment, become readable once flushed (the read limit moves), segments are
 * sealed when full, and the oldest one goes away with retention.
 *
 * Two phases per tick: append (written, not yet readable), then flush
 * (readable). It starts from a still frame with a few segments already
 * written, which is all that is shown with `prefers-reduced-motion`.
 */

import type { ReactNode } from "react";
import { useEffect, useState } from "react";
import clsx from "clsx";

import styles from "./styles.module.css";

const SEGMENT_SIZE = 6;
const VISIBLE_SEGMENTS = 3;
const TICK_MS = 550;
/** The still frame: a few records into the third segment, all flushed. */
const STILL_NEXT_OFFSET = 15;

type State = { next: number; flushed: boolean };

function useLogState(): State {
  const [state, setState] = useState<State>({ next: STILL_NEXT_OFFSET, flushed: true });

  useEffect(() => {
    if (window.matchMedia("(prefers-reduced-motion: reduce)").matches) {
      return undefined;
    }
    const timer = window.setInterval(() => {
      setState(({ next, flushed }) =>
        flushed ? { next: next + 1, flushed: false } : { next, flushed: true },
      );
    }, TICK_MS);
    return () => window.clearInterval(timer);
  }, []);

  return state;
}

export default function LogAnimation(): ReactNode {
  const { next, flushed } = useLogState();
  const readLimit = flushed ? next : next - 1;
  const active = Math.floor(next / SEGMENT_SIZE);
  const first = Math.max(0, active - (VISIBLE_SEGMENTS - 1));
  const segments = Array.from({ length: VISIBLE_SEGMENTS }, (_, i) => first + i);

  return (
    <figure
      className={styles.panel}
      role="img"
      aria-label="A partition log: records are appended to the active segment and become readable once flushed to disk."
    >
      <div className={styles.header}>
        <span className={styles.dots} aria-hidden="true">
          <i />
          <i />
          <i />
        </span>
        <span className={styles.path}>topics/orders/p0</span>
      </div>
      <div className={styles.segments}>
        {segments.map((segment) => {
          const base = segment * SEGMENT_SIZE;
          const isActive = segment === active;
          return (
            <div
              key={segment}
              className={clsx(
                styles.segment,
                isActive && styles.active,
                segment > active && styles.notYetCreated,
              )}
            >
              <div className={styles.segmentLabel}>
                {String(base).padStart(4, "0")}.log
                <span>{isActive ? "active" : segment < active ? "sealed" : ""}</span>
              </div>
              <div className={styles.cells}>
                {Array.from({ length: SEGMENT_SIZE }, (_, i) => {
                  const offset = base + i;
                  const written = offset < next;
                  const readable = offset < readLimit;
                  return (
                    <span
                      key={offset}
                      className={clsx(
                        styles.cell,
                        written && styles.written,
                        readable && styles.readable,
                        offset === readLimit && styles.limit,
                      )}
                    >
                      {written ? offset : ""}
                    </span>
                  );
                })}
              </div>
            </div>
          );
        })}
      </div>
      <figcaption className={styles.legend}>
        <span>
          <i className={clsx(styles.swatch, styles.readable)} /> readable
        </span>
        <span>
          <i className={clsx(styles.swatch, styles.written)} /> written, flushing
        </span>
        <span className={styles.readLimit}>
          read limit <strong>{readLimit}</strong>
        </span>
      </figcaption>
    </figure>
  );
}
