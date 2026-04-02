# Brand Assets

This directory is the canonical home for the shared IPTO logo assets used across the project.

Current direction:
- the old raster illustration used in `doc/system/logo/logo.png` should be considered legacy
- the SVG icon/lockup family here is the preferred visual identity
- the documentation front page now uses a simplified LaTeX/TikZ rendering aligned with the same concept

Canonical asset set:
- `ipto-icon.svg`: dark/default icon
- `ipto-icon-light.svg`: light-surface icon
- `ipto-icon-mono.svg`: monochrome icon
- `ipto-lockup.svg`: dark/default icon + wordmark lockup
- `ipto-lockup-light.svg`: light-surface icon + wordmark lockup
- `ipto-monogram-ip.svg`: compact monogram variant

For now, `apps/admin-web/static/` contains served copies of the same assets for the web application. The intended source of truth is this directory.

Canonical concepts:
- layered lines represent versioned repository state
- connected nodes represent graph/schema relationships
- the enclosing badge represents a bounded system artifact rather than a generic AI-style tech emblem
