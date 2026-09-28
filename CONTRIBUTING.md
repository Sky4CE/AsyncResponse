# Contributing to AsyncResponse

Thank you for taking the time to contribute! Bug reports, feature requests, documentation
improvements, and code changes are all welcome.

By participating in this project, you agree to abide by our [Code of Conduct](CODE_OF_CONDUCT.md).
Report unacceptable behavior to `tyunisov@gmail.com`. Report security vulnerabilities privately as
described in the [security policy](SECURITY.md), not in a public issue.

---

## Reporting bugs and requesting features

Check the [existing issues](https://github.com/Sky4CE/AsyncResponse/issues) first, then open an
issue with the **Bug Report** or **Feature Request** template.

A useful bug report includes steps to reproduce, expected vs. actual behavior, the AsyncResponse
version, the channel / transport / durable-flow store in use, the .NET version and OS, and any
stack traces or logs. A feature request describes the problem, the proposed solution, and the
alternatives you considered — [docs/roadmap.md](docs/roadmap.md) shows what is already planned or
deliberately declined.

## Submitting pull requests

1. Fork `Sky4CE/AsyncResponse` and branch off `main` with a descriptive name (e.g.
   `fix/redis-timeout` or `feature/hangfire-transport`).
2. Make the change with tests — every bug fix carries a regression test that fails without the
   fix, and every feature is covered by unit or integration tests.
3. Build with no warnings and run the tests (see below).
4. Record public API changes and add a `CHANGELOG.md` entry (see below).
5. Open the PR against `main` and fill in the pull request template.

---

## Local development

Prerequisites: the .NET SDK pinned in [global.json](global.json) (10.0.100 or a later 10.0
feature band) plus the .NET 8 runtime, because packages and the unit suite target both
`net8.0` and `net10.0`. Integration tests also need a running Docker daemon.

```bash
git clone https://github.com/<your-username>/AsyncResponse.git
cd AsyncResponse
dotnet build AsyncResponse.slnx
```

Tests run on **Microsoft.Testing.Platform** (xUnit.net v3). With that runner, pass a project with
`--project` — a positional project path is not supported:

```bash
# Unit suite — no Docker needed:
dotnet test --project tests/AsyncResponse.Tests/AsyncResponse.Tests.csproj

# Integration tests that boot no containers (in-process sample plus the Native AOT
# publish gate; ASYNCRESPONSE_SKIP_AOT_GATE=1 skips the gate):
dotnet test --project tests/AsyncResponse.IntegrationTests/AsyncResponse.IntegrationTests.csproj --filter-trait "batch=none"

# One Docker-backed batch (Aspire starts the containers; images are pulled on first run):
dotnet test --project tests/AsyncResponse.IntegrationTests/AsyncResponse.IntegrationTests.csproj --filter-trait "batch=data"

# Everything, including every Docker-backed batch:
dotnet test
```

[docs/operations.md](docs/operations.md#building-and-testing) describes the full test layout,
the integration batches, and running against the Native AOT sample.

### Public API changes

Every shipped package tracks its public surface with `Microsoft.CodeAnalysis.PublicApiAnalyzers`.
A change that adds or removes a public type or member fails the build with `RS0016` / `RS0017`
until it is recorded in that package's `PublicAPI.Unshipped.txt` — use the IDE code fix ("Add to
public API") or `dotnet format analyzers`. This is the API-review gate, not a broken build.

### Changelog

[GitHub Releases](https://github.com/Sky4CE/AsyncResponse/releases) carry the published release
notes. `CHANGELOG.md` tracks work that has landed on `main` but not shipped: add user-visible
changes under `[Unreleased]`.

---

## Conventions

- **Style** — standard .NET conventions, enforced by `.editorconfig` in the build
  (`EnforceCodeStyleInBuild`); CI builds with warnings as errors. Keep formatting consistent with
  the surrounding code.
- **Comments** — keep existing comments and XML docs unless your change makes them outdated; then
  update them.
- **Strong naming** — all assemblies are signed with the checked-in `asyncresponse.snk`. The key
  is intentionally public (strong naming is identity, not security), so there is nothing to
  configure. `InternalsVisibleTo` entries carry the matching public key; test doubles over
  internal seams also befriend Moq's `DynamicProxyGenAssembly2`.
- **Trimming and Native AOT** — every shipped package sets `IsAotCompatible=true`. New
  serialization goes through the source-generated seam (`AsyncResponseJson` /
  `AsyncResponseJsonContext`), and new reflection needs an explicit annotation story; read
  [docs/aot.md](docs/aot.md) before adding either.
- **Documentation** — a change to options, behavior, or providers updates the relevant pages in
  `docs/` and the README. A new provider package also meets the checklist in
  [docs/roadmap.md](docs/roadmap.md#2-the-bar-for-a-new-package).
