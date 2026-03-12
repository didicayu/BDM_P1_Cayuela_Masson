# P1.2 Delivery LaTeX Report

This folder contains the LaTeX source for the P1.2 delivery document.

## Layout

- `src/p1_2_delivery.tex`: main LaTeX source
- `assets/`: images (place updated architecture diagram here)
- `build/`: generated artifacts (`.pdf`, `.aux`, `.log`, etc.)
- `Makefile`: build automation

## Build

From repository root:

```bash
cd docs/p1_2_delivery
make
```

Output PDF:

```bash
docs/p1_2_delivery/build/p1_2_delivery.pdf
```

## Clean

```bash
cd docs/p1_2_delivery
make clean
```

Or remove all build artifacts except `build/.gitkeep`:

```bash
cd docs/p1_2_delivery
make distclean
```

## Updated Diagram Placeholder

Place your latest architecture diagram at:

```bash
docs/p1_2_delivery/assets/current_implementation_diagram.png
```

The LaTeX document auto-includes this file if present. If it is missing, a visible placeholder box is rendered instead.
