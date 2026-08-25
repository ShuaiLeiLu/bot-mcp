# Guardian dashboard override

This page intentionally overrides the generated purple/pink AI palette. Guardian is an
infrastructure control plane, so it uses a restrained light operations palette:

- primary navy: `#12445A`;
- secondary teal: `#247684`;
- action/success teal: `#0F766E` / `#16715B`;
- canvas: `#F4F7F8`;
- card: `#FFFFFF`;
- foreground: `#102A3A`;
- border: `#D6E1E5`.

Page rules:

1. Data-dense dashboard spacing remains at density 8/10.
2. Direct scheduling state is always expressed with text plus a status dot; color alone is never
   the only signal.
3. Start, emergency stop, and manual recovery use explicit confirmation and disabled loading
   states.
4. Account identities are not rendered in recovery summaries.
5. Mobile uses a drawer at 56rem and never introduces page-level horizontal scrolling.
6. Motion is limited to 180–200ms state transitions and is disabled under reduced-motion.
7. Navigation icons use the existing consistent outline SVG language; no emoji icons.
