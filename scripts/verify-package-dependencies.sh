#!/usr/bin/env bash
# Fails when a packed AsyncResponse package depends on a SIBLING AsyncResponse package with
# anything other than an exact version range.
#
# The packages are one product split along provider lines: Core grants InternalsVisibleTo to every
# provider package and the providers compile against those internals, a surface no public-API
# baseline tracks. An open ">= x" range — what NuGet writes for a ProjectReference by default — lets
# a restore pair Core 0.2.0 with Transports.Redis 0.1.0 and the application dies with
# MissingMethodException at the first call across the seam. src/Directory.Build.targets rewrites
# those ranges to "[x]" through a PRIVATE NuGet pack target (_GetProjectReferenceVersions); this
# script is the check from outside, so an SDK that renames or drops that target is caught here
# rather than by a user at runtime.
#
# Usage: verify-package-dependencies.sh <directory-with-nupkg-files>
set -euo pipefail

directory=${1:-./artifacts}

if [ ! -d "$directory" ]; then
    echo "verify-package-dependencies: '$directory' is not a directory" >&2
    exit 2
fi

packages=()
while IFS= read -r package; do packages+=("$package"); done < <(find "$directory" -maxdepth 1 -name '*.nupkg' -not -name '*.symbols.nupkg' | sort)

if [ ${#packages[@]} -eq 0 ]; then
    echo "verify-package-dependencies: no .nupkg files in '$directory'" >&2
    exit 2
fi

failures=0
checked=0
for package in "${packages[@]}"; do
    name=$(basename "$package")
    # The .nuspec is the packed manifest; unzip -p writes it to stdout without extracting.
    nuspec=$(unzip -p "$package" '*.nuspec' 2>/dev/null || true)
    if [ -z "$nuspec" ]; then
        echo "FAIL $name: no .nuspec inside the package" >&2
        failures=$((failures + 1))
        continue
    fi

    # One <dependency .../> element per line, then keep the AsyncResponse.* ones.
    while IFS= read -r dependency; do
        id=$(printf '%s' "$dependency" | sed -n 's/.*id="\([^"]*\)".*/\1/p')
        version=$(printf '%s' "$dependency" | sed -n 's/.*version="\([^"]*\)".*/\1/p')
        case "$id" in
            AsyncResponse|AsyncResponse.*) ;;
            *) continue ;;
        esac

        checked=$((checked + 1))
        # Exact range: "[1.2.3]". Anything else — a bare version (">= 1.2.3"), "[1.0,2.0)",
        # "(1.0,)" — lets a mismatched sibling resolve.
        case "$version" in
            \[*\]) if printf '%s' "$version" | grep -q ','; then
                       echo "FAIL $name: depends on $id with range $version (a range, not one exact version)" >&2
                       failures=$((failures + 1))
                   fi ;;
            *) echo "FAIL $name: depends on $id with version $version (expected an exact range like [$version])" >&2
               failures=$((failures + 1)) ;;
        esac
    done < <(printf '%s' "$nuspec" | tr '>' '>\n' | grep '<dependency ' || true)
done

if [ "$failures" -gt 0 ]; then
    echo "verify-package-dependencies: $failures sibling dependency/dependencies are not pinned" >&2
    exit 1
fi

echo "verify-package-dependencies: ${#packages[@]} package(s), $checked sibling dependency/dependencies, all pinned to an exact version"
