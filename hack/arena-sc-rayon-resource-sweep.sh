#!/usr/bin/env bash
set -euo pipefail

# Measure the semantic classifier's intra-request Rayon scaling independently
# of the horizontal-replica matrix.  The measured target is a temporary clone
# of the reference Deployment: the upstream image and pod configuration are
# retained, but the clone has a unique selector, no Service, one inference
# worker, and a fixed Guaranteed resource envelope.  The reference Deployment
# is read and attested but never patched or scaled.

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "${SCRIPT_DIR}/.." && pwd)

KUBECONFIG_PATH=${KUBECONFIG_PATH:-/tmp/llm-d-sc-arena-kubeconfig}
NAMESPACE=${NAMESPACE:-llm-d-sc-scaleout}
REFERENCE_DEPLOYMENT=${REFERENCE_DEPLOYMENT:-classifier-target}
TARGET_CONTAINER=${TARGET_CONTAINER:-llm-d-sc}
TARGET_NODE=${TARGET_NODE:-gnr2.fm2aihpcsed.com}
DRIVER_NODE=${DRIVER_NODE:-rhgnr1}
OTEL_DAEMONSET=${OTEL_DAEMONSET:-llm-d-sc-otel}

DRIVER_IMAGE=${DRIVER_IMAGE:?set the pinned benchmark-driver image digest}
TARGET_IMAGE=${TARGET_IMAGE:?set the expected pinned target image digest}
MODEL_SHA256=${MODEL_SHA256:?set MODEL_SHA256}
TOKENIZER_SHA256=${TOKENIZER_SHA256:-851ca67100d372ca3ae031a6abd168f53489eebfd7d89523f35c5c9b4d372c3c}
SEQUENCE_BASE=${SEQUENCE_BASE:?set a globally unused SEQUENCE_BASE}
SWEEP_SEED=${SWEEP_SEED:?set an integer SWEEP_SEED for reproducible blocked randomization}

SWEEP_RUN_ID=${SWEEP_RUN_ID:-rayon-$(date -u +%Y%m%d%H%M%S)}
SWEEP_DEPLOYMENT=${SWEEP_DEPLOYMENT:-sc-rayon-${SWEEP_RUN_ID}}
SWEEP_SELECTOR_KEY=benchmark.llm-d/rayon-sweep
SWEEP_SELECTOR_VALUE=$SWEEP_RUN_ID
SWEEP_SELECTOR=${SWEEP_SELECTOR_KEY}=${SWEEP_SELECTOR_VALUE}
LOCK_NAME=${LOCK_NAME:-sc-benchmark-matrix-lock}

# Five randomized blocks are the confirmatory default.  Every block contains
# the complete comparison, preventing time/order drift from becoming a thread
# count effect.
RAYON_VALUES=${RAYON_VALUES:-unset 1 2 4 8}
REPEATS=${REPEATS:-5}
DURATION_SECONDS=${DURATION_SECONDS:-180}
START_DELAY_SECONDS=${START_DELAY_SECONDS:-45}
QUIESCENCE_SECONDS=${QUIESCENCE_SECONDS:-30}
TELEMETRY_SETTLE_SECONDS=${TELEMETRY_SETTLE_SECONDS:-10}
MAX_ROWS_PER_ENDPOINT=${MAX_ROWS_PER_ENDPOINT:-50000}
TOKEN_COUNT=${TOKEN_COUNT:-64}
ROLLOUT_TIMEOUT_SECONDS=${ROLLOUT_TIMEOUT_SECONDS:-600}

# Arena has two SMT threads per physical core and CPU Manager's
# full-pcpus-only policy.  Sixteen logical CPUs therefore reserve eight
# complete physical cores, so RAYON_NUM_THREADS=8 is not forced onto SMT
# siblings.  GQ8 would reserve only four physical cores and confound RT8.
RESOURCE_CPU=${RESOURCE_CPU:-16}
RESOURCE_MEMORY=${RESOURCE_MEMORY:-4Gi}
MINIMUM_FULL_CORES=${MINIMUM_FULL_CORES:-8}

PLAN_ONLY=${PLAN_ONLY:-0}
DELETE_COMPLETED_JOBS=${DELETE_COMPLETED_JOBS:-1}
RESULT_ROOT=${RESULT_ROOT:-${REPO_ROOT}/results}
SWEEP_DIR=${SWEEP_DIR:-${RESULT_ROOT}/rayon/${SWEEP_RUN_ID}}
CELL_RESULT_ROOT=${CELL_RESULT_ROOT:-${SWEEP_DIR}/cells}
CELL_RUNNER=${CELL_RUNNER:-${SCRIPT_DIR}/arena-sc-inference-cell.sh}
METRICS_RUNNER=${METRICS_RUNNER:-${SCRIPT_DIR}/arena-sc-capture-thanos.sh}
TOPOLOGY_RUNNER=${TOPOLOGY_RUNNER:-${SCRIPT_DIR}/arena-sc-topology-preflight.py}

k=(oc --kubeconfig "$KUBECONFIG_PATH")
lock_acquired=0
sweep_deployment_created=0
sweep_complete=0
plan_only_complete=0
sweep_dir_owned=0
active_driver_run_id=""
current_cell=preflight
last_error=""

die() {
  last_error=$*
  echo "ERROR: ${last_error}" >&2
  if (( sweep_dir_owned == 1 )); then
    printf '%s\n' "$last_error" >"${SWEEP_DIR}/sweep-error.txt"
  fi
  exit 1
}

append_error() {
  if [[ -n "$last_error" ]]; then
    last_error="${last_error}; $1"
  else
    last_error=$1
  fi
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
}

assert_uint() {
  local name=$1 value=$2
  [[ "$value" =~ ^(0|[1-9][0-9]*)$ ]] \
    || die "${name} must be a base-10 unsigned integer without leading zeros; got ${value}"
}

assert_positive() {
  local name=$1 value=$2
  assert_uint "$name" "$value"
  (( value > 0 )) || die "${name} must be greater than zero"
}

normalize_list() {
  printf '%s\n' "${1//,/ }"
}

require_access() {
  local verb=$1 resource=$2 namespace=${3:-} allowed
  if [[ -n "$namespace" ]]; then
    allowed=$("${k[@]}" auth can-i "$verb" "$resource" -n "$namespace")
  else
    allowed=$("${k[@]}" auth can-i "$verb" "$resource")
  fi
  [[ "$allowed" == yes ]] \
    || die "current identity cannot ${verb} ${resource}${namespace:+ in ${namespace}}"
}

node_is_ready() {
  local node=$1
  "${k[@]}" get "node/${node}" -o json \
    | jq -e 'any(.status.conditions[]?; .type == "Ready" and .status == "True")' >/dev/null
}

target_pod_count() {
  "${k[@]}" get pods -n "$NAMESPACE" -l "$SWEEP_SELECTOR" -o json \
    | jq '.items | length'
}

wait_for_target_deletion() {
  if (( $(target_pod_count) == 0 )); then
    return
  fi
  "${k[@]}" wait --for=delete pod -n "$NAMESPACE" -l "$SWEEP_SELECTOR" \
    --timeout="${ROLLOUT_TIMEOUT_SECONDS}s" >/dev/null
}

delete_active_driver_jobs() {
  local failed=0
  if [[ -z "$active_driver_run_id" ]]; then
    return
  fi
  "${k[@]}" delete jobs -n "$NAMESPACE" \
    -l "benchmark.llm-d/run-id=${active_driver_run_id}" \
    --ignore-not-found --cascade=foreground --wait=true --timeout=120s >/dev/null \
    || failed=1
  "${k[@]}" delete pods -n "$NAMESPACE" \
    -l "benchmark.llm-d/run-id=${active_driver_run_id}" \
    --ignore-not-found --wait=true --timeout=120s >/dev/null \
    || failed=1
  (( failed == 0 )) || return 1
  active_driver_run_id=""
}

capture_reference_integrity() {
  local after=${SWEEP_DIR}/deployment-reference-after.json
  if ! "${k[@]}" get deployment/"$REFERENCE_DEPLOYMENT" -n "$NAMESPACE" -o json >"$after"; then
    return 1
  fi
  jq -n --slurpfile before "${SWEEP_DIR}/deployment-reference-before.json" \
    --slurpfile after "$after" '
      {uid_equal:($before[0].metadata.uid == $after[0].metadata.uid),
       resource_version_equal:
         ($before[0].metadata.resourceVersion == $after[0].metadata.resourceVersion),
       generation_equal:
         ($before[0].metadata.generation == $after[0].metadata.generation),
       spec_equal:($before[0].spec == $after[0].spec),
       pod_template_equal:
         ($before[0].spec.template == $after[0].spec.template),
       reference_deployment_mutated_by_sweep:false,
       gate_passed:
         ($before[0].metadata.uid == $after[0].metadata.uid
          and $before[0].metadata.generation == $after[0].metadata.generation
          and $before[0].spec == $after[0].spec)}' \
    >"${SWEEP_DIR}/deployment-reference-integrity.json"
  jq -e '.gate_passed' "${SWEEP_DIR}/deployment-reference-integrity.json" >/dev/null
}

cleanup() {
  local exit_code=$? cleanup_failed=0 final_status=aborted
  trap - EXIT
  trap '' INT TERM
  set +e

  if ! delete_active_driver_jobs; then
    cleanup_failed=1
    append_error "failed to delete active driver Jobs for ${active_driver_run_id}"
  fi

  if (( sweep_deployment_created == 1 )); then
    if ! "${k[@]}" delete deployment/"$SWEEP_DEPLOYMENT" -n "$NAMESPACE" \
      --cascade=foreground --wait=true --timeout=180s >/dev/null; then
      cleanup_failed=1
      append_error "failed to delete temporary sweep Deployment ${SWEEP_DEPLOYMENT}"
    fi
    if (( $(target_pod_count 2>/dev/null || echo 1) != 0 )); then
      cleanup_failed=1
      append_error "temporary sweep target Pods remain after cleanup"
    fi
  fi

  if (( lock_acquired == 1 )); then
    if ! capture_reference_integrity; then
      cleanup_failed=1
      append_error "reference Deployment integrity gate failed"
    fi
  fi

  if (( lock_acquired == 1 && cleanup_failed == 0 )); then
    if ! "${k[@]}" delete configmap "$LOCK_NAME" -n "$NAMESPACE" \
      --wait=true --timeout=60s >/dev/null; then
      cleanup_failed=1
      append_error "failed to release benchmark lock ${NAMESPACE}/${LOCK_NAME}"
    fi
  elif (( lock_acquired == 1 )); then
    append_error "benchmark lock retained for operator intervention"
  fi

  if (( cleanup_failed == 1 && exit_code == 0 )); then
    exit_code=1
  fi
  if (( exit_code == 0 && sweep_complete == 1 )); then
    final_status=completed
  elif (( exit_code == 0 && plan_only_complete == 1 )); then
    final_status=planned
  fi
  if (( sweep_dir_owned == 1 )); then
    jq -n --arg run_id "$SWEEP_RUN_ID" --arg status "$final_status" \
      --arg cell "$current_cell" --arg completed_at "$(date -u +%FT%TZ)" \
      --arg error "$last_error" --argjson exit_code "$exit_code" \
      '{schema_version:1,run_id:$run_id,status:$status,last_cell:$cell,
        completed_at:$completed_at,exit_code:$exit_code,
        error:(if $error == "" then null else $error end)}' \
      >"${SWEEP_DIR}/sweep-status.json"
  fi
  exit "$exit_code"
}

trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

for command in oc jq curl cksum sort awk sed git seq tail wc uniq date mkdir python3 shasum; do
  require_command "$command"
done
[[ -x "$CELL_RUNNER" ]] || die "cell runner is not executable: ${CELL_RUNNER}"
[[ -x "$METRICS_RUNNER" ]] || die "metrics runner is not executable: ${METRICS_RUNNER}"
[[ -f "$TOPOLOGY_RUNNER" ]] || die "topology runner not found: ${TOPOLOGY_RUNNER}"

[[ "$SWEEP_RUN_ID" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ ]] \
  || die "SWEEP_RUN_ID must be a DNS-safe lowercase label"
(( ${#SWEEP_RUN_ID} <= 28 )) || die "SWEEP_RUN_ID must be at most 28 characters"
[[ "$SWEEP_DEPLOYMENT" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ ]] \
  || die "SWEEP_DEPLOYMENT must be a DNS-safe lowercase label"
(( ${#SWEEP_DEPLOYMENT} <= 63 )) || die "SWEEP_DEPLOYMENT must be at most 63 characters"
[[ "$DRIVER_IMAGE" =~ @sha256:[0-9a-f]{64}$ ]] \
  || die "DRIVER_IMAGE must end in an immutable sha256 digest"
[[ "$TARGET_IMAGE" =~ ^sha256:[0-9a-f]{64}$ ]] \
  || die "TARGET_IMAGE must be sha256:<64 hex>"
[[ "$MODEL_SHA256" =~ ^[0-9a-f]{64}$ ]] || die "MODEL_SHA256 must be 64 lowercase hex"
[[ "$TOKENIZER_SHA256" =~ ^[0-9a-f]{64}$ ]] \
  || die "TOKENIZER_SHA256 must be 64 lowercase hex"
[[ "$RESOURCE_MEMORY" =~ ^[1-9][0-9]*(Mi|Gi)$ ]] \
  || die "RESOURCE_MEMORY must be a positive Mi or Gi quantity"

for pair in \
  "SEQUENCE_BASE:$SEQUENCE_BASE" "SWEEP_SEED:$SWEEP_SEED" "REPEATS:$REPEATS" \
  "DURATION_SECONDS:$DURATION_SECONDS" "START_DELAY_SECONDS:$START_DELAY_SECONDS" \
  "QUIESCENCE_SECONDS:$QUIESCENCE_SECONDS" \
  "TELEMETRY_SETTLE_SECONDS:$TELEMETRY_SETTLE_SECONDS" \
  "MAX_ROWS_PER_ENDPOINT:$MAX_ROWS_PER_ENDPOINT" "TOKEN_COUNT:$TOKEN_COUNT" \
  "ROLLOUT_TIMEOUT_SECONDS:$ROLLOUT_TIMEOUT_SECONDS" \
  "RESOURCE_CPU:$RESOURCE_CPU" "MINIMUM_FULL_CORES:$MINIMUM_FULL_CORES"; do
  assert_uint "${pair%%:*}" "${pair#*:}"
done
assert_positive REPEATS "$REPEATS"
assert_positive DURATION_SECONDS "$DURATION_SECONDS"
assert_positive MAX_ROWS_PER_ENDPOINT "$MAX_ROWS_PER_ENDPOINT"
assert_positive ROLLOUT_TIMEOUT_SECONDS "$ROLLOUT_TIMEOUT_SECONDS"
assert_positive RESOURCE_CPU "$RESOURCE_CPU"
assert_positive MINIMUM_FULL_CORES "$MINIMUM_FULL_CORES"
[[ "$TOKEN_COUNT" == 64 ]] \
  || die "TOKEN_COUNT must remain 64 so every cell uses the established exact64 corpus"
(( RESOURCE_CPU >= 16 && RESOURCE_CPU % 2 == 0 )) \
  || die "RESOURCE_CPU must be an even value of at least 16 on Arena; GQ8 only reserves four SMT cores"
(( MINIMUM_FULL_CORES >= 8 )) \
  || die "MINIMUM_FULL_CORES must remain at least 8 for the RAYON_NUM_THREADS=8 cell"
for flag in "$PLAN_ONLY" "$DELETE_COMPLETED_JOBS"; do
  [[ "$flag" == 0 || "$flag" == 1 ]] || die "boolean flags must be 0 or 1"
done

variant_count=0
for variant in $(normalize_list "$RAYON_VALUES"); do
  [[ "$variant" == unset || "$variant" =~ ^[1-9][0-9]*$ ]] \
    || die "RAYON_VALUES entries must be unset or positive integers; got ${variant}"
  variant_count=$((variant_count + 1))
done
[[ "$variant_count" == 5 ]] \
  || die "RAYON_VALUES must contain exactly unset,1,2,4,8"
for required in unset 1 2 4 8; do
  count=0
  for variant in $(normalize_list "$RAYON_VALUES"); do
    [[ "$variant" == "$required" ]] && count=$((count + 1))
  done
  [[ "$count" == 1 ]] || die "RAYON_VALUES must contain ${required} exactly once"
done

[[ ! -e "$SWEEP_DIR" ]] \
  || die "SWEEP_DIR already exists; choose a new SWEEP_RUN_ID: ${SWEEP_DIR}"
mkdir -p "$SWEEP_DIR" "$CELL_RESULT_ROOT"
sweep_dir_owned=1

schedule=${SWEEP_DIR}/sweep-plan.tsv
printf 'order\trepetition\trayon_threads\tresource_cpu\tminimum_full_cores\tsequence_base\trun_id\n' \
  >"$schedule"
order=0
sequence_span=$((MAX_ROWS_PER_ENDPOINT + 1))
for repetition in $(seq 1 "$REPEATS"); do
  keyed=${SWEEP_DIR}/.block-${repetition}.keyed.tsv
  : >"$keyed"
  for variant in $(normalize_list "$RAYON_VALUES"); do
    key=$(printf '%s' "${SWEEP_SEED}:${repetition}:${variant}" | cksum | awk '{print $1}')
    printf '%010u\t%s\n' "$key" "$variant" >>"$keyed"
  done
  LC_ALL=C sort -k1,1n -k2,2 "$keyed" >"${keyed}.sorted"
  while IFS=$'\t' read -r _ variant; do
    order=$((order + 1))
    if [[ "$variant" == unset ]]; then
      label=unset
    else
      label=t${variant}
    fi
    sequence=$((SEQUENCE_BASE + (order - 1) * sequence_span))
    cell_id=$(printf 'ry-%s-o%03d-%s' "$SWEEP_RUN_ID" "$order" "$label")
    (( ${#cell_id} <= 54 )) || die "generated cell RUN_ID is too long: ${cell_id}"
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
      "$order" "$repetition" "$variant" "$RESOURCE_CPU" \
      "$MINIMUM_FULL_CORES" "$sequence" "$cell_id" >>"$schedule"
  done <"${keyed}.sorted"
done

planned_cells=$((REPEATS * variant_count))
[[ $(($(wc -l <"$schedule") - 1)) == "$planned_cells" ]] \
  || die "internal error: sweep plan cell count mismatch"
[[ $(tail -n +2 "$schedule" | awk -F '\t' '{print $6}' | sort -n | uniq -d | wc -l | awk '{print $1}') == 0 ]] \
  || die "internal error: duplicate sequence bases"
last_reserved_sequence=$((SEQUENCE_BASE + planned_cells * sequence_span))
(( last_reserved_sequence > SEQUENCE_BASE && last_reserved_sequence < 4611686018427387904 )) \
  || die "sequence reservation exceeds the exact64 generator capacity"

jq -n --arg run_id "$SWEEP_RUN_ID" --arg seed "$SWEEP_SEED" \
  --arg variants "$RAYON_VALUES" --argjson repeats "$REPEATS" \
  --argjson duration "$DURATION_SECONDS" --argjson cpu "$RESOURCE_CPU" \
  --arg memory "$RESOURCE_MEMORY" --argjson full_cores "$MINIMUM_FULL_CORES" \
  --argjson sequence_base "$SEQUENCE_BASE" --argjson sequence_span "$sequence_span" \
  '{schema_version:1,run_id:$run_id,blocked_randomization_seed:$seed,
    variants:$variants,repeats:$repeats,duration_seconds:$duration,
    fixed_cell:{replicas:1,inference_workers:1,concurrency:1,connections:1,
      direct_pod_ip:true,exact_tokens:64},
    resources:{qos:"Guaranteed",requests:{cpu:$cpu,memory:$memory},
      limits:{cpu:$cpu,memory:$memory},minimum_complete_physical_cores:$full_cores},
    sequence:{base:$sequence_base,span_per_cell:$sequence_span},
    interpretation:{explicit_variants_unconstrained_by_thread_slots:true,
      unset_variant:"ambient-default oversubscription control, not an unconstrained host-wide run",
      gq8_rejected:"eight logical CPUs are four complete SMT cores on Arena and would confound RT8"}}' \
  >"${SWEEP_DIR}/sweep-plan.json"

if (( PLAN_ONLY == 1 )); then
  current_cell=plan-only
  plan_only_complete=1
  cat "$schedule"
  exit 0
fi

for spec in \
  "get:deployment/${REFERENCE_DEPLOYMENT}" "create:deployments.apps" \
  "get:deployments.apps" "patch:deployments.apps" "delete:deployments.apps" \
  "get:deployments.apps/scale" "update:deployments.apps/scale" \
  "get:pods" "list:pods" "watch:pods" "delete:pods" \
  "create:pods/exec" "get:pods/log" \
  "create:jobs.batch" "get:jobs.batch" "list:jobs.batch" "delete:jobs.batch" \
  "get:events" "list:events" \
  "get:endpointslices.discovery.k8s.io" "list:endpointslices.discovery.k8s.io" \
  "get:services" "list:services" \
  "create:configmaps" "get:configmaps" "delete:configmaps" \
  "get:daemonsets.apps"; do
  require_access "${spec%%:*}" "${spec#*:}" "$NAMESPACE"
done
require_access get nodes
require_access list nodes
require_access get routes.route.openshift.io openshift-monitoring

node_is_ready "$TARGET_NODE" || die "target node is not Ready"
node_is_ready "$DRIVER_NODE" || die "driver node is not Ready"
otel_json=$("${k[@]}" get daemonset/"$OTEL_DAEMONSET" -n "$NAMESPACE" -o json)
jq -e '(.status.desiredNumberScheduled // 0) > 0
  and .status.numberReady == .status.desiredNumberScheduled
  and (.status.numberUnavailable // 0) == 0' <<<"$otel_json" >/dev/null \
  || die "OTEL DaemonSet is not fully Ready"
jq . <<<"$otel_json" >"${SWEEP_DIR}/otel-daemonset-preflight.json"

if ! "${k[@]}" create configmap "$LOCK_NAME" -n "$NAMESPACE" \
  --from-literal="run-id=${SWEEP_RUN_ID}" \
  --from-literal="kind=rayon-resource-sweep" \
  --from-literal="created-at=$(date -u +%FT%TZ)" >/dev/null; then
  "${k[@]}" get configmap "$LOCK_NAME" -n "$NAMESPACE" -o yaml >&2 || true
  die "another benchmark owns ${NAMESPACE}/${LOCK_NAME}"
fi
lock_acquired=1

"${k[@]}" get deployment/"$REFERENCE_DEPLOYMENT" -n "$NAMESPACE" -o json \
  >"${SWEEP_DIR}/deployment-reference-before.json"
"${k[@]}" get nodes "$TARGET_NODE" "$DRIVER_NODE" -o json \
  >"${SWEEP_DIR}/nodes-before.json"
"${k[@]}" get services -n "$NAMESPACE" -o json >"${SWEEP_DIR}/services-before.json"
"${k[@]}" version -o json >"${SWEEP_DIR}/oc-version.json"
"${k[@]}" whoami >"${SWEEP_DIR}/cluster-identity.txt"
git -C "$REPO_ROOT" rev-parse HEAD >"${SWEEP_DIR}/git-head.txt"
git -C "$REPO_ROOT" status --porcelain=v1 >"${SWEEP_DIR}/git-status.txt"
shasum -a 256 "$0" "$CELL_RUNNER" "$METRICS_RUNNER" "$TOPOLOGY_RUNNER" \
  >"${SWEEP_DIR}/harness-sha256.txt"

jq -e --arg container "$TARGET_CONTAINER" '
  (.spec.template.spec.containers | length) == 1
  and .spec.template.spec.containers[0].name == $container' \
  "${SWEEP_DIR}/deployment-reference-before.json" >/dev/null \
  || die "reference Deployment must contain exactly one ${TARGET_CONTAINER} container"

jq --arg name "$SWEEP_DEPLOYMENT" --arg namespace "$NAMESPACE" \
  --arg selector_key "$SWEEP_SELECTOR_KEY" --arg selector_value "$SWEEP_SELECTOR_VALUE" \
  --arg target_node "$TARGET_NODE" --arg container "$TARGET_CONTAINER" \
  --arg cpu "$RESOURCE_CPU" --arg memory "$RESOURCE_MEMORY" '
  . as $source
  | {apiVersion:$source.apiVersion,kind:$source.kind,
     metadata:{name:$name,namespace:$namespace,
       labels:{"benchmark.llm-d/kind":"rayon-resource-sweep",
         "benchmark.llm-d/run-id":$selector_value},
       annotations:{"benchmark.llm-d/reference-deployment":$source.metadata.name,
         "benchmark.llm-d/reference-uid":$source.metadata.uid}},
     spec:$source.spec}
  | .spec.replicas=0
  | .spec.paused=false
  | .spec.selector={matchLabels:{($selector_key):$selector_value}}
  | .spec.template.metadata.labels={($selector_key):$selector_value,
      "benchmark.llm-d/kind":"rayon-resource-sweep"}
  | .spec.template.metadata.annotations=($source.spec.template.metadata.annotations // {})
  | .spec.template.spec.nodeSelector=
      (($source.spec.template.spec.nodeSelector // {}) + {"kubernetes.io/hostname":$target_node})
  | .spec.template.spec.containers |= map(
      if .name == $container then
        .env=((.env // [])
          | map(select(.name != "LLM_D_SC_INFERENCE_WORKERS"
                       and .name != "RAYON_NUM_THREADS"
                       and .name != "CANDLE_NUM_THREADS")))
          + [{name:"LLM_D_SC_INFERENCE_WORKERS",value:"1"}]
        | .resources={requests:{cpu:$cpu,memory:$memory},limits:{cpu:$cpu,memory:$memory}}
      else . end)' \
  "${SWEEP_DIR}/deployment-reference-before.json" \
  >"${SWEEP_DIR}/deployment-sweep-create.json"

"${k[@]}" create -f "${SWEEP_DIR}/deployment-sweep-create.json" >/dev/null
sweep_deployment_created=1
"${k[@]}" get deployment/"$SWEEP_DEPLOYMENT" -n "$NAMESPACE" -o json \
  >"${SWEEP_DIR}/deployment-sweep-created.json"

jq -n --arg run_id "$SWEEP_RUN_ID" --arg created_at "$(date -u +%FT%TZ)" \
  --arg namespace "$NAMESPACE" --arg reference_name "$REFERENCE_DEPLOYMENT" \
  --arg sweep_deployment "$SWEEP_DEPLOYMENT" --arg selector "$SWEEP_SELECTOR" \
  --arg target_node "$TARGET_NODE" --arg driver_node "$DRIVER_NODE" \
  --arg target_image "$TARGET_IMAGE" --arg driver_image "$DRIVER_IMAGE" \
  --arg model "$MODEL_SHA256" --arg tokenizer "$TOKENIZER_SHA256" \
  --argjson cpu "$RESOURCE_CPU" --arg memory "$RESOURCE_MEMORY" \
  --argjson minimum_full_cores "$MINIMUM_FULL_CORES" \
  --slurpfile reference_doc "${SWEEP_DIR}/deployment-reference-before.json" '
  {schema_version:1,run_id:$run_id,created_at:$created_at,namespace:$namespace,
   reference_deployment:{name:$reference_name,uid:$reference_doc[0].metadata.uid,
     generation:$reference_doc[0].metadata.generation,mutation_contract:"read-only"},
   sweep_deployment:$sweep_deployment,selector:$selector,
   target_node:$target_node,driver_node:$driver_node,
   topology:("cross-node-direct-"+$target_node+"-from-"+$driver_node),
   target_image:$target_image,driver_image:$driver_image,
   model_sha256:$model,tokenizer_sha256:$tokenizer,
   runtime:{inference_workers:1,candle_threads:"unset",rayon_variants:["unset",1,2,4,8]},
   resources:{qos:"Guaranteed",cpu_request:$cpu,cpu_limit:$cpu,
     memory_request:$memory,memory_limit:$memory,
     minimum_complete_physical_cores:$minimum_full_cores},
   isolation:{service_endpoints_forbidden:true,direct_pod_ip_only:true,
     fresh_temporary_pod_per_cell:true,reference_deployment_untouched:true}}' \
  >"${SWEEP_DIR}/sweep-provenance.json"

previous_pod_uid=""
tail -n +2 "$schedule" >"${SWEEP_DIR}/.execution-plan.tsv"
while IFS=$'\t' read -r cell_order repetition rayon_threads resource_cpu \
  minimum_full_cores sequence_base cell_id; do
  current_cell=$cell_id
  cell_dir=${CELL_RESULT_ROOT}/${cell_id}
  mkdir -p "$cell_dir"

  "${k[@]}" scale deployment/"$SWEEP_DEPLOYMENT" -n "$NAMESPACE" --replicas=0 >/dev/null
  wait_for_target_deletion

  current_deployment=$("${k[@]}" get deployment/"$SWEEP_DEPLOYMENT" -n "$NAMESPACE" -o json)
  containers_patch=$(jq -c --arg container "$TARGET_CONTAINER" \
    --arg rayon "$rayon_threads" --arg cpu "$RESOURCE_CPU" --arg memory "$RESOURCE_MEMORY" '
    [.spec.template.spec.containers[]
      | if .name == $container then
          .env=((.env // [])
            | map(select(.name != "LLM_D_SC_INFERENCE_WORKERS"
                         and .name != "RAYON_NUM_THREADS"
                         and .name != "CANDLE_NUM_THREADS")))
            + [{name:"LLM_D_SC_INFERENCE_WORKERS",value:"1"}]
            + (if $rayon == "unset" then []
               else [{name:"RAYON_NUM_THREADS",value:$rayon}] end)
          | .resources={requests:{cpu:$cpu,memory:$memory},limits:{cpu:$cpu,memory:$memory}}
        else . end]' <<<"$current_deployment")
  patch=$(jq -nc --argjson containers "$containers_patch" \
    '[{op:"replace",path:"/spec/template/spec/containers",value:$containers}]')
  "${k[@]}" patch deployment/"$SWEEP_DEPLOYMENT" -n "$NAMESPACE" \
    --type=json --patch "$patch" >/dev/null
  "${k[@]}" scale deployment/"$SWEEP_DEPLOYMENT" -n "$NAMESPACE" --replicas=1 >/dev/null
  "${k[@]}" rollout status deployment/"$SWEEP_DEPLOYMENT" -n "$NAMESPACE" \
    --timeout="${ROLLOUT_TIMEOUT_SECONDS}s" >/dev/null
  if (( QUIESCENCE_SECONDS > 0 )); then
    sleep "$QUIESCENCE_SECONDS"
  fi

  deployment_json=$("${k[@]}" get deployment/"$SWEEP_DEPLOYMENT" -n "$NAMESPACE" -o json)
  pods_json=$("${k[@]}" get pods -n "$NAMESPACE" -l "$SWEEP_SELECTOR" -o json)
  jq . <<<"$deployment_json" >"${cell_dir}/sweep-deployment-before.json"
  jq . <<<"$pods_json" >"${cell_dir}/sweep-targets-before.json"

  jq -e --arg container "$TARGET_CONTAINER" --arg rayon "$rayon_threads" \
    --arg cpu "$RESOURCE_CPU" --arg memory "$RESOURCE_MEMORY" '
    .spec.replicas == 1
    and any(.spec.template.spec.containers[]?;
      .name == $container
      and .resources.requests.cpu == $cpu and .resources.limits.cpu == $cpu
      and .resources.requests.memory == $memory and .resources.limits.memory == $memory
      and any(.env[]?;.name == "LLM_D_SC_INFERENCE_WORKERS" and .value == "1")
      and ([.env[]? | select(.name == "CANDLE_NUM_THREADS")] | length) == 0
      and (if $rayon == "unset"
           then ([.env[]? | select(.name == "RAYON_NUM_THREADS")] | length) == 0
           else any(.env[]?;.name == "RAYON_NUM_THREADS" and .value == $rayon) end))' \
    <<<"$deployment_json" >/dev/null || die "${cell_id}: Deployment runtime/resource gate failed"

  jq -e --arg node "$TARGET_NODE" --arg digest "$TARGET_IMAGE" \
    --arg container "$TARGET_CONTAINER" '
    (.items | length) == 1
    and .items[0].metadata.deletionTimestamp == null
    and .items[0].status.phase == "Running"
    and .items[0].spec.nodeName == $node
    and .items[0].status.qosClass == "Guaranteed"
    and any(.items[0].status.conditions[]?;.type == "Ready" and .status == "True")
    and ([.items[0].status.containerStatuses[]?.restartCount] | add // 0) == 0
    and any(.items[0].status.containerStatuses[]?;
      .name == $container and (.imageID | endswith($digest)))' \
    <<<"$pods_json" >/dev/null || die "${cell_id}: target health/QoS/image gate failed"

  pod_uid=$(jq -r '.items[0].metadata.uid' <<<"$pods_json")
  [[ -z "$previous_pod_uid" || "$pod_uid" != "$previous_pod_uid" ]] \
    || die "${cell_id}: target Pod was not fresh"
  previous_pod_uid=$pod_uid
  pod_name=$(jq -r '.items[0].metadata.name' <<<"$pods_json")

  "${k[@]}" get endpointslices.discovery.k8s.io -n "$NAMESPACE" -o json \
    >"${cell_dir}/endpointslices-preload.json"
  jq -e --arg uid "$pod_uid" '
    [.items[].endpoints[]?.targetRef.uid // empty] | index($uid) == null' \
    "${cell_dir}/endpointslices-preload.json" >/dev/null \
    || die "${cell_id}: temporary target appeared in a Service EndpointSlice"

  topology_status=0
  if python3 "$TOPOLOGY_RUNNER" live --kubeconfig "$KUBECONFIG_PATH" \
    --namespace "$NAMESPACE" --selector "$SWEEP_SELECTOR" --expected-pods 1 \
    --container "$TARGET_CONTAINER" --format json \
    >"${cell_dir}/topology-preflight.json" \
    2>"${cell_dir}/topology-preflight.stderr.txt"; then
    :
  else
    topology_status=$?
  fi
  (( topology_status == 0 )) \
    || die "${cell_id}: topology preflight rejected placement (exit ${topology_status})"

  jq --arg node "$TARGET_NODE" --argjson requested "$RESOURCE_CPU" \
    --argjson minimum "$MINIMUM_FULL_CORES" '
    def expand:
      split(",") | map(if contains("-") then
        (split("-") | range((.[0]|tonumber); ((.[1]|tonumber)+1)))
        else tonumber end);
    (.snapshot.pods[0].cpuset | expand) as $cpus
    | [.snapshot.nodes[$node].thread_sibling_groups[]
       | expand as $group
       | select(all($group[]; . as $cpu | ($cpus | index($cpu)) != null))
       | $group] as $complete_groups
    | {gate_passed:(.gate_passed and ($cpus|length) == $requested
        and ($complete_groups|length) >= $minimum),
       requested_logical_cpus:$requested,observed_logical_cpus:($cpus|length),
       observed_cpuset:.snapshot.pods[0].cpuset,
       complete_physical_core_sets:($complete_groups|length),
       minimum_complete_physical_core_sets:$minimum,
       sibling_sets:$complete_groups,
       qos_class:.snapshot.pods[0].qos_class}' \
    "${cell_dir}/topology-preflight.json" >"${cell_dir}/resource-topology-audit.json"
  jq -e '.gate_passed and .qos_class == "Guaranteed"' \
    "${cell_dir}/resource-topology-audit.json" >/dev/null \
    || die "${cell_id}: resource/topology envelope is insufficient"

  "${k[@]}" exec -n "$NAMESPACE" "$pod_name" -c "$TARGET_CONTAINER" -- sh -c '
    cat /proc/1/status
    printf "__TASKS__\n"
    for task in /proc/1/task/[0-9]*; do
      printf "%s " "${task##*/}"
      cat "$task/comm"
    done' >"${cell_dir}/proc-threads-before.txt"

  jq -n --arg run_id "$cell_id" --arg sweep_run_id "$SWEEP_RUN_ID" \
    --arg rayon "$rayon_threads" --arg topology "cross-node-direct-${TARGET_NODE}-from-${DRIVER_NODE}" \
    --argjson order "$cell_order" --argjson repetition "$repetition" \
    --argjson sequence_base "$sequence_base" --argjson cpu "$RESOURCE_CPU" \
    --arg memory "$RESOURCE_MEMORY" --argjson minimum_full_cores "$MINIMUM_FULL_CORES" \
    '{schema_version:1,run_id:$run_id,sweep_run_id:$sweep_run_id,
      order:$order,repetition:$repetition,rayon_threads:$rayon,
      replicas:1,inference_workers:1,concurrency:1,connections:1,
      sequence_base:$sequence_base,topology:$topology,
      resources:{qos:"Guaranteed",requests:{cpu:$cpu,memory:$memory},
        limits:{cpu:$cpu,memory:$memory},minimum_complete_physical_cores:$minimum_full_cores},
      interpretation:(if $rayon == "unset" then
        "ambient-default oversubscription control within fixed cpuset"
        else "explicit Rayon worker count with eight complete physical cores available; OS placement is not pinned" end)}' \
    >"${cell_dir}/sweep-cell.json"

  active_driver_run_id=$cell_id
  cell_runner_status=0
  if env KUBECONFIG_PATH="$KUBECONFIG_PATH" NAMESPACE="$NAMESPACE" \
    DEPLOYMENT="$SWEEP_DEPLOYMENT" TARGET_SELECTOR="$SWEEP_SELECTOR" \
    TARGET_NODE="$TARGET_NODE" DRIVER_NODE="$DRIVER_NODE" REPLICAS=1 \
    CONCURRENCY=1 CONNECTIONS=1 DURATION_SECONDS="$DURATION_SECONDS" \
    START_DELAY_SECONDS="$START_DELAY_SECONDS" MAX_ROWS_PER_ENDPOINT="$MAX_ROWS_PER_ENDPOINT" \
    SEQUENCE_BASE="$sequence_base" RUN_ID="$cell_id" DRIVER_IMAGE="$DRIVER_IMAGE" \
    TARGET_IMAGE="$TARGET_IMAGE" MODEL_SHA256="$MODEL_SHA256" \
    TOKENIZER_SHA256="$TOKENIZER_SHA256" TOKEN_COUNT="$TOKEN_COUNT" \
    RESULT_ROOT="$CELL_RESULT_ROOT" RESET_TARGETS=false \
    "$CELL_RUNNER" >"${cell_dir}/cell-runner-stdout.json" \
    2>"${cell_dir}/cell-runner-stderr.txt"; then
    :
  else
    cell_runner_status=$?
  fi
  printf '%s\n' "$cell_runner_status" >"${cell_dir}/cell-runner-exit-status.txt"
  (( cell_runner_status == 0 )) || die "${cell_id}: cell runner failed"

  "${k[@]}" exec -n "$NAMESPACE" "$pod_name" -c "$TARGET_CONTAINER" -- sh -c '
    cat /proc/1/status
    printf "__TASKS__\n"
    for task in /proc/1/task/[0-9]*; do
      printf "%s " "${task##*/}"
      cat "$task/comm"
    done' >"${cell_dir}/proc-threads-after.txt"

  if (( TELEMETRY_SETTLE_SECONDS > 0 )); then
    sleep "$TELEMETRY_SETTLE_SECONDS"
  fi
  KUBECONFIG_PATH="$KUBECONFIG_PATH" "$METRICS_RUNNER" "$cell_dir" \
    >"${cell_dir}/metrics-runner-stdout.json" \
    2>"${cell_dir}/metrics-runner-stderr.txt" \
    || die "${cell_id}: telemetry capture failed"

  jq -e --arg rayon "$rayon_threads" --arg cpu "$RESOURCE_CPU" \
    --arg memory "$RESOURCE_MEMORY" --arg uid "$pod_uid" '
    .initiated_within_plateau > 0
    and .completed_within_plateau == .ok_completed_within_plateau
    and .error_completed_within_plateau == 0
    and .initiated_within_plateau ==
      (.completed_within_plateau + .drained_after_plateau)
    and .latency_us.samples == .ok_completed_within_plateau
    and (.corpus_exhausted | not)
    and (.workers_late | not)
    and .health_event_violations == 0
    and (.statuses | keys == ["OK"])
    and .cell.inference_workers == "1"
    and .cell.runtime_threads.rayon == $rayon
    and .cell.runtime_threads.candle == "unset"
    and .cell.qos_class == "Guaranteed"
    and .cell.resources.requests.cpu == $cpu
    and .cell.resources.limits.cpu == $cpu
    and .cell.resources.requests.memory == $memory
    and .cell.resources.limits.memory == $memory
    and (.cgroup_cpu | length) == 1
    and .cgroup_cpu[0].pod_uid == $uid
    and .cgroup_cpu[0].cpuset_cpus_effective.start == .cgroup_cpu[0].cpuset_cpus_effective.end
    and .cgroup_cpu[0].cpu_max.start == .cgroup_cpu[0].cpu_max.end
    and (if $rayon == "unset" then .cgroup_cpu[0].nr_throttled_delta >= 0
         else .cgroup_cpu[0].nr_throttled_delta == 0 end)' \
    "${cell_dir}/summary.json" >/dev/null || die "${cell_id}: result/runtime gate failed"
  jq -e '(.pod_cpu_otel | length) == 1
    and (.pod_ready_min | length) == 1
    and (.node_ready_min | length) == 2
    and all(.pod_ready_min[];.min == 1)
    and all(.node_ready_min[];.min == 1)' \
    "${cell_dir}/metrics-summary.json" >/dev/null \
    || die "${cell_id}: authoritative telemetry is incomplete or unhealthy"
  jq -e --arg uid "$pod_uid" '.snapshot.pods[0].uid == $uid and .gate_passed' \
    "${cell_dir}/topology-preflight.json" >/dev/null \
    || die "${cell_id}: topology evidence does not match measured Pod"

  threads_before=$(awk '$1 == "Threads:" {print $2; exit}' "${cell_dir}/proc-threads-before.txt")
  threads_after=$(awk '$1 == "Threads:" {print $2; exit}' "${cell_dir}/proc-threads-after.txt")
  [[ "$threads_before" =~ ^[1-9][0-9]*$ && "$threads_after" =~ ^[1-9][0-9]*$ ]] \
    || die "${cell_id}: /proc thread-count evidence is malformed"

  shasum -a 256 "${cell_dir}/sweep-cell.json" "${cell_dir}/cell.json" \
    "${cell_dir}/summary.json" "${cell_dir}/drivers.json" \
    "${cell_dir}/cgroup-summary.json" "${cell_dir}/metrics-summary.json" \
    "${cell_dir}/topology-preflight.json" "${cell_dir}/resource-topology-audit.json" \
    >"${cell_dir}/attestation-sha256.txt"

  jq -nc --slurpfile expected "${cell_dir}/sweep-cell.json" \
    --slurpfile summary "${cell_dir}/summary.json" \
    --slurpfile metrics "${cell_dir}/metrics-summary.json" \
    --slurpfile topology "${cell_dir}/resource-topology-audit.json" \
    --argjson threads_before "$threads_before" --argjson threads_after "$threads_after" '
    {expected:$expected[0],summary:$summary[0],metrics:$metrics[0],
     resource_topology:$topology[0],process_threads:{before:$threads_before,after:$threads_after},
     derived:{cpu_usec_per_ok:
       ($summary[0].cgroup_cpu[0].usage_usec_delta / $summary[0].ok_completed_within_plateau)},
     valid:true}' >>"${SWEEP_DIR}/sweep-results.ndjson"

  if (( DELETE_COMPLETED_JOBS == 1 )); then
    delete_active_driver_jobs || die "${cell_id}: failed to delete completed driver Jobs"
  else
    active_driver_run_id=""
  fi
done <"${SWEEP_DIR}/.execution-plan.tsv"

completed_cells=$(wc -l <"${SWEEP_DIR}/sweep-results.ndjson" | awk '{print $1}')
[[ "$completed_cells" == "$planned_cells" ]] \
  || die "only ${completed_cells}/${planned_cells} Rayon cells completed"

jq -s '
  def stats:
    map(select(. != null)) | sort as $values
    | ($values | length) as $count
    | if $count == 0 then null else
        {samples:$count,min:$values[0],max:$values[-1],
         mean:($values | add / $count),
         median:(if ($count % 2) == 1 then $values[($count/2|floor)]
           else (($values[$count/2-1] + $values[$count/2]) / 2) end)} end;
  {schema_version:1,cells:.,all_valid:all(.[];.valid),
   variants:(group_by(.expected.rayon_threads) | map({
     rayon_threads:.[0].expected.rayon_threads,repeats:length,
     useful_rps:(map(.summary.aggregate_useful_rps) | stats),
     latency_p50_us:(map(.summary.latency_us.p50) | stats),
     latency_p99_us:(map(.summary.latency_us.p99) | stats),
     average_cpu_cores:(map(.summary.cgroup_cpu[0].average_cpu_cores) | stats),
     cpu_usec_per_ok:(map(.derived.cpu_usec_per_ok) | stats),
     throttled_period_ratio:(map(.summary.cgroup_cpu[0].throttled_period_ratio) | stats),
     process_threads_after:(map(.process_threads.after) | stats),
     request_accounting_clean:all(.[];
       .summary.initiated_within_plateau > 0
       and .summary.completed_within_plateau == .summary.ok_completed_within_plateau
       and .summary.error_completed_within_plateau == 0
       and .summary.initiated_within_plateau ==
         (.summary.completed_within_plateau + .summary.drained_after_plateau)
       and .summary.health_event_violations == 0),
     topology_clean:all(.[];.resource_topology.gate_passed)}))}' \
  "${SWEEP_DIR}/sweep-results.ndjson" >"${SWEEP_DIR}/sweep-results.json"

current_cell=complete
sweep_complete=1
jq '{cells:(.cells|length),all_valid,variants}' "${SWEEP_DIR}/sweep-results.json"
