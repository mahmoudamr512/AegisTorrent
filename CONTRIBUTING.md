# Contributing to AegisTorrent

Thank you for your interest in contributing. AegisTorrent is a learning-first systems project — contributions that deepen the implementation, improve correctness, or add test coverage are especially valued.

---

## Before you start

- Read the [Architecture](docs/architecture.md) doc to understand how modules fit together
- Check open issues for anything labeled `good-first-issue` or `help-wanted`
- For large changes, open an issue to discuss the approach before writing code

---

## Development setup

```bash
git clone https://github.com/mahmoudamr512/aegistorrent.git
cd aegistorrent
cargo build
cargo test
```

---

## Workflow

1. **Fork** the repository
2. **Branch** from `main`: `git checkout -b feat/your-feature-name`
3. **Code** — keep changes focused and scoped
4. **Test** — add unit or integration tests for new logic
5. **Lint** — run `cargo clippy` and `cargo fmt --check`
6. **Commit** using [conventional commits](https://www.conventionalcommits.org/)
7. **Push** and open a PR against `main`

---

## Commit format

```
feat(scheduler): implement rarest-first piece selection
fix(merkle): correct leaf hash order for odd-length trees
docs(protocol): add CANCEL message wire format
test(chunker): edge case for files smaller than piece size
```

Valid types: `feat`, `fix`, `docs`, `test`, `refactor`, `perf`, `chore`

---

## Code standards

- All code must pass `cargo clippy` with no warnings
- All public functions and types have `///` doc comments
- New modules need corresponding unit tests (`#[cfg(test)]` module)
- No `unsafe` without justification and review
- No external crate dependencies without discussion
- Format with `cargo fmt` before committing

---

## Issue labels

| Label | Meaning |
|---|---|
| `good-first-issue` | Small, well-scoped, great entry point |
| `help-wanted` | Medium effort, needs an owner |
| `research-needed` | Needs investigation before coding |
| `phase-1` through `phase-5` | Maps to roadmap phase |

---

Be direct, honest, and respectful. Technical disagreements are welcome — personal attacks are not.
