# Guardian Dashboard Override

This page override takes precedence over `../MASTER.md` for the production
Guardian console.

## Product direction

- Light-only operations dashboard requested by the product owner.
- Dense, scan-first layout for administrators; no decorative hero treatment.
- Flat white surfaces on `#F8FAFC`, blue information hierarchy, WCAG-adjusted
  deep-amber primary evaluation actions, and semantic green/red status colors.
- No gradients, glass effects, emoji navigation, or layout-shifting hover motion.

## Typography

Production CSP and offline packaging do not load third-party font resources.
Use local `Fira Sans`/`Fira Code` when installed, followed by the existing
system Chinese and sans/monospace fallbacks. Body text remains 16px; compact
data labels never go below 12px.

## Interaction and accessibility

- All controls have at least a 44px interaction target.
- Inline SVG icons use one outline style with `1.8` stroke width.
- Keyboard focus uses the primary blue ring and is never removed.
- Tables stay within an explicit horizontal-scroll wrapper on narrow screens.
- Motion is limited to 180–200ms opacity/color transitions and honors
  `prefers-reduced-motion`.
- Verify at 375, 768, 1024, and 1440 widths with no document-level overflow.
