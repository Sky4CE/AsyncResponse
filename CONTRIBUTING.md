# Contributing to AsyncResponse

First off, thank you for taking the time to contribute! Contributions of all kinds (bug reports, feature requests, documentation improvements, code changes) are highly welcome.

The following is a set of guidelines for contributing to AsyncResponse.

---

## Code of Conduct

By participating in this project, you agree to abide by our [Code of Conduct](CODE_OF_CONDUCT.md). Please report any unacceptable behavior to `tyunisov@gmail.com`.

---

## How to Contribute

### 1. Reporting Bugs

Before submitting a bug report, please check the [existing issues](https://github.com/Sky4CE/AsyncResponse/issues) to make sure it hasn't been reported yet.

When creating a bug report, please use our **Bug Report Template** and provide as much context as possible:
* A clear and descriptive title.
* Steps to reproduce the behavior.
* Expected vs. actual behavior.
* Environment details (.NET version, OS, OS version, transport/channel used).
* Stack traces, error logs, or code snippets reproducing the issue.

### 2. Requesting Features

If you want to suggest a new feature or improvement, please check the [existing issues](https://github.com/Sky4CE/AsyncResponse/issues) first. 

When requesting a feature, please use our **Feature Request Template** and describe:
* The problem you are trying to solve.
* The proposed solution.
* Alternative solutions or workarounds you've considered.

### 3. Submitting Pull Requests (PRs)

We welcome PRs for bug fixes, performance improvements, and new features. To make the process smooth:

1. **Fork the Repository**: Create a fork of `Sky4CE/AsyncResponse`.
2. **Create a Branch**: Branch off `main` with a descriptive name (e.g., `fix/redis-timeout-issue` or `feature/kafka-transport`).
3. **Write Code & Tests**: 
   * Write clean, readable code following standard C#/.NET guidelines.
   * Preserve all existing comments and docstrings unless they are outdated.
   * Write unit/integration tests for your changes.
4. **Format Your Code**: Ensure your files are formatted properly and contain no compiler warnings or lint errors.
5. **Run the Test Suite**: Ensure all existing and new tests pass locally before committing.
6. **Open the PR**: Push your branch to GitHub and open a Pull Request against our `main` branch. Use the provided Pull Request template and fill in all the details.

---

## Local Development Setup

To set up your local development environment:

1. Clone your fork of the repository:
   ```bash
   git clone https://github.com/<your-username>/AsyncResponse.git
   ```
2. Open the solution file `AsyncResponse.sln` using your favorite IDE (JetBrains Rider, Visual Studio, or VS Code).
3. To run all tests from the command line:
   ```bash
   dotnet test
   ```

---

## Assembly signing and AOT expectations

- All assemblies are strong-named with the checked-in `asyncresponse.snk`. The key is
  intentionally public (strong naming is identity, not a security boundary) — nothing to
  configure locally. `InternalsVisibleTo` entries carry the matching public key; test doubles
  over internal seams additionally befriend Moq's `DynamicProxyGenAssembly2`.
- Every shipped package builds with the trim/Native AOT analyzers enabled
  (`IsAotCompatible=true`) and CI treats warnings as errors. New serialization goes through the
  source-generated seam (`AsyncResponseJson` / `AsyncResponseJsonContext`); new reflection needs
  an explicit annotation story — see [docs/aot.md](docs/aot.md) before adding either.

## Coding Guidelines

* **Code Style**: We follow standard .NET coding conventions (PascalCase for public API, camelCase/prefix-underscore for private fields, etc.).
* **Aesthetics and Structure**: Keep formatting consistent with existing files.
* **Testing First**: Never submit code changes without accompanying automated unit or integration tests verifying them.
* **Documentation**: If your change modifies configuration options, behaviors, or introduces a new transport/channel, make sure to update the relevant documentation in the `docs/` folder.
