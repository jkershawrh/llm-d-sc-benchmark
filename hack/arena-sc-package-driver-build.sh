#!/usr/bin/env bash
set -euo pipefail

# Produce the exact, minimal binary-build input for the dedicated Arena
# benchmark-driver image.  The output directory is immutable evidence: this
# script refuses to overwrite it.  Nothing is sent to or changed on a cluster.

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
FRAMEWORK_ROOT=$(cd "${SCRIPT_DIR}/.." && pwd)

OUTPUT_DIR=${1:-}
SC_SOURCE_ROOT=${2:-${SC_SOURCE_ROOT:-}}
if [[ -z "$OUTPUT_DIR" || "$OUTPUT_DIR" == -* || -z "$SC_SOURCE_ROOT" || $# -gt 2 ]]; then
  echo "usage: $0 OUTPUT_DIR CLASSIFIER_SOURCE_ROOT" >&2
  echo "CLASSIFIER_SOURCE_ROOT may instead be supplied as SC_SOURCE_ROOT" >&2
  exit 2
fi
if [[ "$OUTPUT_DIR" != /* ]]; then
  OUTPUT_DIR="${PWD}/${OUTPUT_DIR}"
fi
if [[ "$SC_SOURCE_ROOT" != /* ]]; then
  SC_SOURCE_ROOT="${PWD}/${SC_SOURCE_ROOT}"
fi
[[ "$OUTPUT_DIR" != / && "$OUTPUT_DIR" != "$FRAMEWORK_ROOT" \
   && "$OUTPUT_DIR" != "$SC_SOURCE_ROOT" ]] || {
  echo "refusing unsafe OUTPUT_DIR: ${OUTPUT_DIR}" >&2
  exit 2
}
[[ ! -e "$OUTPUT_DIR" ]] || {
  echo "OUTPUT_DIR already exists: ${OUTPUT_DIR}" >&2
  exit 2
}

for command in awk chmod cp date dirname find git gzip jq mkdir mktemp mv rg rm \
  shasum sort tar touch tr wc xargs; do
  command -v "$command" >/dev/null 2>&1 || {
    echo "required command not found: ${command}" >&2
    exit 2
  }
done

task_tmp_root=${TMPDIR:-/tmp}
work_dir=$(mktemp -d "${task_tmp_root%/}/llm-d-sc-driver-package.XXXXXX")
case "$work_dir" in
  "${task_tmp_root%/}"/llm-d-sc-driver-package.*) ;;
  *) echo "unexpected temporary directory: ${work_dir}" >&2; exit 2 ;;
esac
context_dir=${work_dir}/context
package_dir=${work_dir}/package
mkdir -p "$context_dir" "$package_dir"

cleanup() {
  cleanup_status=$?
  trap - EXIT INT TERM
  rm -rf "$work_dir"
  exit "$cleanup_status"
}
trap cleanup EXIT INT TERM

required_paths=(
  Cargo.toml
  Cargo.lock
  build.rs
  proto
  classifiers
  src
)
for path in "${required_paths[@]}"; do
  [[ -e "${SC_SOURCE_ROOT}/${path}" ]] || {
    echo "required build input is missing: ${SC_SOURCE_ROOT}/${path}" >&2
    exit 2
  }
done
[[ -f "${FRAMEWORK_ROOT}/Containerfile.benchmark-driver" ]] || {
  echo "benchmark-driver Containerfile is missing: ${FRAMEWORK_ROOT}/Containerfile.benchmark-driver" >&2
  exit 2
}

# COPYFILE_DISABLE prevents macOS AppleDouble/xattr records from entering the
# Linux build archive.  Only compiler inputs used by Containerfile.benchmark-
# driver are staged; docs, evidence, deploy files, .git, and target are absent.
COPYFILE_DISABLE=1 cp \
  "${SC_SOURCE_ROOT}/Cargo.toml" \
  "${SC_SOURCE_ROOT}/Cargo.lock" \
  "${FRAMEWORK_ROOT}/Containerfile.benchmark-driver" \
  "${SC_SOURCE_ROOT}/build.rs" \
  "$context_dir/"
COPYFILE_DISABLE=1 cp -R \
  "${SC_SOURCE_ROOT}/proto" \
  "${SC_SOURCE_ROOT}/classifiers" \
  "${SC_SOURCE_ROOT}/src" \
  "$context_dir/"

if find "$context_dir" -type l -print -quit | rg -q .; then
  echo "symlinks are not permitted in the driver build context" >&2
  exit 2
fi

files_manifest=${package_dir}/build-context-files.sha256
(
  cd "$context_dir"
  find . -type f -print0 \
    | LC_ALL=C sort -z \
    | xargs -0 shasum -a 256
) >"$files_manifest"
file_count=$(wc -l <"$files_manifest" | tr -d ' ')
[[ "$file_count" =~ ^[1-9][0-9]*$ ]] || {
  echo "driver build context is empty" >&2
  exit 2
}

context_manifest_sha256=$(shasum -a 256 "$files_manifest" | awk '{print $1}')
probe_sha256=$(shasum -a 256 "${SC_SOURCE_ROOT}/src/bin/sustained-corpus-probe.rs" | awk '{print $1}')
containerfile_sha256=$(shasum -a 256 "${FRAMEWORK_ROOT}/Containerfile.benchmark-driver" | awk '{print $1}')
git_head=$(git -C "$SC_SOURCE_ROOT" rev-parse HEAD)
framework_git_head=$(git -C "$FRAMEWORK_ROOT" rev-parse HEAD)
git -C "$SC_SOURCE_ROOT" status --porcelain=v1 >"${package_dir}/git-status.txt"
printf '%s\n' "$git_head" >"${package_dir}/git-head.txt"
printf '%s\n' "$framework_git_head" >"${package_dir}/framework-git-head.txt"

archive_name=llm-d-sc-benchmark-driver-context.tar.gz
archive_path=${package_dir}/${archive_name}
archive_tar=${work_dir}/llm-d-sc-benchmark-driver-context.tar
archive_file_list=${work_dir}/archive-files.list

# Normalize metadata and archive order so identical inputs yield identical
# archive bytes on the same tar implementation.  The content manifest remains
# the cross-implementation identity; the archive hash identifies exact bytes
# uploaded for this build.
find "$context_dir" -type d -exec chmod 0755 {} +
find "$context_dir" -type f -exec chmod 0644 {} +
find "$context_dir" -exec touch -h -t 200001010000.00 {} +
(
  cd "$context_dir"
  find . -type f -print0 | LC_ALL=C sort -z >"$archive_file_list"
  if tar --version 2>&1 | rg -q bsdtar; then
    COPYFILE_DISABLE=1 tar -c --null --uid 0 --gid 0 --uname root --gname root \
      --no-xattrs -T "$archive_file_list" -f "$archive_tar"
  else
    tar -c --null --owner=0 --group=0 --numeric-owner --no-xattrs \
      -T "$archive_file_list" -f "$archive_tar"
  fi
)
gzip -n -9 -c "$archive_tar" >"$archive_path"
archive_sha256=$(shasum -a 256 "$archive_path" | awk '{print $1}')
archive_bytes=$(wc -c <"$archive_path" | tr -d ' ')

verify_dir=${work_dir}/verify
mkdir -p "$verify_dir"
tar -xzf "$archive_path" -C "$verify_dir"
(
  cd "$verify_dir"
  shasum -a 256 -c "$files_manifest" >/dev/null
)

printf '%s\n' \
  "LLM_D_SC_DRIVER_GIT_HEAD=${git_head}" \
  "LLM_D_SC_DRIVER_CONTEXT_MANIFEST_SHA256=${context_manifest_sha256}" \
  "LLM_D_SC_DRIVER_ARCHIVE_SHA256=${archive_sha256}" \
  "LLM_D_SC_DRIVER_PROBE_SHA256=${probe_sha256}" \
  >"${package_dir}/build-args.env"

created_at=$(date -u +%FT%TZ)
dirty=false
[[ -s "${package_dir}/git-status.txt" ]] && dirty=true
jq -n \
  --arg created_at "$created_at" \
  --arg archive "$archive_name" \
  --arg archive_sha256 "$archive_sha256" \
  --arg context_manifest_sha256 "$context_manifest_sha256" \
  --arg probe_sha256 "$probe_sha256" \
  --arg containerfile_sha256 "$containerfile_sha256" \
  --arg git_head "$git_head" \
  --arg framework_git_head "$framework_git_head" \
  --argjson git_dirty "$dirty" \
  --argjson file_count "$file_count" \
  --argjson archive_bytes "$archive_bytes" \
  '{schema_version:1,kind:"llm-d-sc-benchmark-driver-build-input",
    created_at:$created_at,
    archive:{path:$archive,sha256:$archive_sha256,bytes:$archive_bytes},
    context:{file_count:$file_count,files_manifest:"build-context-files.sha256",
      files_manifest_sha256:$context_manifest_sha256,
      included:["Cargo.toml","Cargo.lock","Containerfile.benchmark-driver","build.rs","proto/","classifiers/","src/"],
      excluded:[".git/","target/","docs/","deploy/","hack/","specs/","tests/","artifacts/","training/","upstream-staging/","node_modules/"]},
    source:{git_head:$git_head,git_dirty:$git_dirty,
      git_status:"git-status.txt",probe_sha256:$probe_sha256,
      containerfile_sha256:$containerfile_sha256},
    framework:{git_head:$framework_git_head,git_head_file:"framework-git-head.txt"},
    build:{dockerfile_path:"Containerfile.benchmark-driver",
      output_must_be_driver_only:true,use_image_digest_only:true}}' \
  >"${package_dir}/build-input-provenance.json"

mkdir -p "$(dirname "$OUTPUT_DIR")"
mv "$package_dir" "$OUTPUT_DIR"

printf 'driver build package: %s\n' "$OUTPUT_DIR"
printf 'archive: %s/%s\n' "$OUTPUT_DIR" "$archive_name"
printf 'archive sha256: %s\n' "$archive_sha256"
printf 'context manifest sha256: %s\n' "$context_manifest_sha256"
printf 'probe source sha256: %s\n' "$probe_sha256"
