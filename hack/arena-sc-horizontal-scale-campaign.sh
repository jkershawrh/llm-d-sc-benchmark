#!/usr/bin/env bash
set -euo pipefail

# Infra-only horizontal scale campaign for the unchanged llm-d-sc image.
# The workload is a run-labeled temporary Deployment plus one serial cell of
# direct-IP ARMED Jobs at a time.  It never patches the reference Deployment or
# the upstream SC source/runtime implementation.

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "${SCRIPT_DIR}/.." && pwd)

KUBECONFIG_PATH=${KUBECONFIG_PATH:-/tmp/llm-d-sc-arena-kubeconfig}
NAMESPACE=${NAMESPACE:-llm-d-sc-scaleout}
REFERENCE_DEPLOYMENT=${REFERENCE_DEPLOYMENT:-classifier-target}
MODEL_CLAIM=${MODEL_CLAIM:-classifier-model}
TARGET_NODE=${TARGET_NODE:-gnr2.fm2aihpcsed.com}
DRIVER_NODE=${DRIVER_NODE:-rhgnr1}
TARGET_CONTAINER=llm-d-sc
LOCK_NAME=${LOCK_NAME:-sc-benchmark-matrix-lock}

SCALE_RUN_ID=${SCALE_RUN_ID:?set a unique lower-case DNS-safe SCALE_RUN_ID}
CAMPAIGN_INDEX=${CAMPAIGN_INDEX:?set the irreversible [22B,23B) campaign allocation index}
RUNG_REPLICAS=${RUNG_REPLICAS:-20}
PLAN_ONLY=${PLAN_ONLY:-0}
RESULT_ROOT=${RESULT_ROOT:-${REPO_ROOT}/results/scaleout-campaigns}
RUN_DIR=${RUN_DIR:-${RESULT_ROOT}/${SCALE_RUN_ID}}

START_LEAD_SECONDS=${START_LEAD_SECONDS:-180}
ARMED_LEAD_SECONDS=${ARMED_LEAD_SECONDS:-90}
BATCH_STABILITY_SECONDS=${BATCH_STABILITY_SECONDS:-120}
BATCH_STARTUP_TIMEOUT_SECONDS=${BATCH_STARTUP_TIMEOUT_SECONDS:-900}
HEALTH_INTERVAL_SECONDS=${HEALTH_INTERVAL_SECONDS:-5}
MAX_DRAIN_SECONDS=${MAX_DRAIN_SECONDS:-90}
METRIC_SETTLE_SECONDS=${METRIC_SETTLE_SECONDS:-15}
TARGET_COUNTER_SETTLE_SECONDS=${TARGET_COUNTER_SETTLE_SECONDS:-30}
OC_REQUEST_TIMEOUT=${OC_REQUEST_TIMEOUT:-30s}
CURL_CONNECT_TIMEOUT_SECONDS=${CURL_CONNECT_TIMEOUT_SECONDS:-10}
CURL_MAX_TIME_SECONDS=${CURL_MAX_TIME_SECONDS:-90}

readonly ARMED_DRIVER_IMAGE='image-registry.openshift-image-registry.svc:5000/llm-d-sc-gremlins/llm-d-sc-benchmark-driver-armed-51541f00e5fa@sha256:ef0f32ad3a7a29f4cd1f68ae8b8cfbc1bf36d66a173df8f68fd531db9d762aae'
readonly ARMED_DRIVER_SOURCE_SHA256='51541f00e5fa6e1918b4e57b9bfa432337345b1854b7289c836c3752543929d9'
readonly TARGET_IMAGE_DIGEST='sha256:04323612ce3f73873b4c3ed6e09264e828241537e2c1a4231b43f32e9744d5aa'
readonly TARGET_IMAGE_REF='image-registry.openshift-image-registry.svc:5000/llm-d-sc-gremlins/llm-d-sc-gremlin@sha256:04323612ce3f73873b4c3ed6e09264e828241537e2c1a4231b43f32e9744d5aa'
readonly MODEL_SHA256='7914abbd152278879b4c3235d188e3006753bb778b7de6266fbcbe4c4ba2ef2f'
readonly TOKENIZER_SHA256='851ca67100d372ca3ae031a6abd168f53489eebfd7d89523f35c5c9b4d372c3c'
readonly DRIVER_PACKAGE_VERSION='0.1.0'
readonly ARMING_PROTOCOL='sustained-corpus-probe-armed-v1'

PLAN_RUNNER=${PLAN_RUNNER:-${SCRIPT_DIR}/arena-sc-horizontal-scale-plan.py}
PREFLIGHT_RUNNER=${PREFLIGHT_RUNNER:-${SCRIPT_DIR}/arena-sc-horizontal-scale-preflight.py}
SUMMARY_RUNNER=${SUMMARY_RUNNER:-${SCRIPT_DIR}/arena-sc-horizontal-scale-summarize.py}
TOPOLOGY_RUNNER=${TOPOLOGY_RUNNER:-${SCRIPT_DIR}/arena-sc-topology-preflight.py}

TARGET_DEPLOYMENT="sso-${SCALE_RUN_ID}-target"
TARGET_SELECTOR="benchmark.llm-d/run-id=${SCALE_RUN_ID},benchmark.llm-d/component=scaleout-target"
RUN_SELECTOR="benchmark.llm-d/run-id=${SCALE_RUN_ID}"

k=(oc --kubeconfig "$KUBECONFIG_PATH" --request-timeout="$OC_REQUEST_TIMEOUT")
run_dir_owned=0
lock_acquired=0
target_created=0
live_started=0
measurement_complete=0
cleanup_verified=0
last_error=""
final_decision=""

die() {
  last_error=$*
  echo "ERROR: ${last_error}" >&2
  if (( run_dir_owned == 1 )); then
    printf '%s\n' "$last_error" >"${RUN_DIR}/campaign-error.txt"
  fi
  exit 2
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
}

positive_integer() {
  local name
  local value
  name=$1
  value=$2
  [[ "$value" =~ ^[1-9][0-9]*$ ]] || die "${name} must be a positive integer"
}

sha256_path() {
  "${sha256[@]}" "$1" | awk '{print $1}'
}

cleanup() {
  local entry_status
  local exit_status
  local cleanup_error
  local owner
  local remaining_jobs
  local remaining_pods
  local remaining_deployments
  local reference_unchanged
  local status
  entry_status=$?
  exit_status=$entry_status
  cleanup_error=""
  owner=""
  remaining_jobs=-1
  remaining_pods=-1
  remaining_deployments=-1
  reference_unchanged=false
  status=aborted
  trap - EXIT INT TERM ERR
  set +e

  if (( run_dir_owned == 1 )) && [[ -s "${RUN_DIR}/sequence-ledger.json" ]]; then
    python3 "$SUMMARY_RUNNER" "$RUN_DIR" --ledger-only \
      --output "${RUN_DIR}/sequence-ledger-final.json" >/dev/null 2>&1 || true
  fi

  if (( live_started == 1 )); then
    "${k[@]}" delete jobs -n "$NAMESPACE" -l "$RUN_SELECTOR" \
      --ignore-not-found --cascade=foreground --wait=true --timeout=300s >/dev/null 2>&1 \
      || cleanup_error="failed to delete scaleout driver Jobs"
  fi
  if (( target_created == 1 )); then
    "${k[@]}" delete deployment "$TARGET_DEPLOYMENT" -n "$NAMESPACE" \
      --ignore-not-found --cascade=foreground --wait=true --timeout=600s >/dev/null 2>&1 \
      || cleanup_error="${cleanup_error:+${cleanup_error}; }failed to delete temporary target Deployment"
  fi
  if (( live_started == 1 )); then
    remaining_jobs=$("${k[@]}" get jobs -n "$NAMESPACE" -l "$RUN_SELECTOR" -o json 2>/dev/null | jq '.items|length')
    remaining_pods=$("${k[@]}" get pods -n "$NAMESPACE" -l "$RUN_SELECTOR" -o json 2>/dev/null | jq '.items|length')
    remaining_deployments=$("${k[@]}" get deployments -n "$NAMESPACE" -l "$RUN_SELECTOR" -o json 2>/dev/null | jq '.items|length')
    if [[ "$remaining_jobs" == 0 && "$remaining_pods" == 0 && "$remaining_deployments" == 0 ]]; then
      cleanup_verified=1
    else
      cleanup_error="${cleanup_error:+${cleanup_error}; }run-labeled workload remains after cleanup"
    fi
    if "${k[@]}" get deployment "$REFERENCE_DEPLOYMENT" -n "$NAMESPACE" -o json \
        >"${RUN_DIR}/reference-deployment-after.json" 2>/dev/null; then
      if [[ -s "${RUN_DIR}/reference-deployment-before.json" ]] && \
          jq -S '.spec' "${RUN_DIR}/reference-deployment-before.json" >"${RUN_DIR}/reference-spec-before.json" && \
          jq -S '.spec' "${RUN_DIR}/reference-deployment-after.json" >"${RUN_DIR}/reference-spec-after.json" && \
          cmp -s "${RUN_DIR}/reference-spec-before.json" "${RUN_DIR}/reference-spec-after.json"; then
        reference_unchanged=true
      else
        cleanup_error="${cleanup_error:+${cleanup_error}; }reference Deployment spec changed; framework did not restore because it never owns that Deployment"
      fi
    else
      cleanup_error="${cleanup_error:+${cleanup_error}; }cannot verify reference Deployment after cleanup"
    fi
  fi
  if (( lock_acquired == 1 )); then
    owner=$("${k[@]}" get configmap "$LOCK_NAME" -n "$NAMESPACE" -o jsonpath='{.data.run-id}' 2>/dev/null)
    if [[ "$owner" == "$SCALE_RUN_ID" ]]; then
      "${k[@]}" delete configmap "$LOCK_NAME" -n "$NAMESPACE" --wait=true --timeout=60s >/dev/null 2>&1 \
        || cleanup_error="${cleanup_error:+${cleanup_error}; }failed to release benchmark lock"
    else
      cleanup_error="${cleanup_error:+${cleanup_error}; }benchmark lock ownership changed"
    fi
  fi

  if [[ -n "$cleanup_error" && $exit_status -eq 0 ]]; then exit_status=1; fi
  if [[ -n "$cleanup_error" ]]; then
    status=cleanup_failed
  elif (( measurement_complete == 1 )); then
    status=completed
  elif [[ "$PLAN_ONLY" == 1 && $entry_status -eq 0 ]]; then
    status=planned
  fi
  if (( run_dir_owned == 1 )); then
    jq -n \
      --arg run_id "$SCALE_RUN_ID" --arg status "$status" --arg decision "$final_decision" \
      --arg error "${last_error}${cleanup_error:+${last_error:+; }${cleanup_error}}" \
      --arg completed_at "$(date -u +%FT%TZ)" --argjson exit_status "$exit_status" \
      --argjson cleanup_verified "$cleanup_verified" --argjson remaining_jobs "${remaining_jobs:--1}" \
      --argjson remaining_pods "${remaining_pods:--1}" --argjson remaining_deployments "${remaining_deployments:--1}" \
      --argjson reference_unchanged "$reference_unchanged" \
      '{schema_version:1,run_id:$run_id,status:$status,
        decision:(if $decision=="" then null else $decision end),exit_status:$exit_status,
        completed_at:$completed_at,error:(if $error=="" then null else $error end),
        cleanup:{verified:$cleanup_verified,remaining_jobs:$remaining_jobs,
          remaining_pods:$remaining_pods,remaining_deployments:$remaining_deployments,
          reference_deployment_spec_unchanged:$reference_unchanged,
          restoration:"temporary workload deleted; reference Deployment was never mutated"}}' \
      >"${RUN_DIR}/campaign-status.json"
  fi
  exit "$exit_status"
}

trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
trap '[[ -n "$last_error" ]] || last_error="command failed at line ${LINENO}"' ERR

for command in jq git python3; do require_command "$command"; done
if command -v sha256sum >/dev/null 2>&1; then
  sha256=(sha256sum)
elif command -v shasum >/dev/null 2>&1; then
  sha256=(shasum -a 256)
else
  die "required SHA-256 tool not found"
fi

[[ "$SCALE_RUN_ID" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ ]] || die "SCALE_RUN_ID must be a lower-case DNS label"
(( ${#SCALE_RUN_ID} <= 38 )) || die "SCALE_RUN_ID must be at most 38 characters"
[[ "$CAMPAIGN_INDEX" =~ ^[0-9]+$ ]] || die "CAMPAIGN_INDEX must be an unsigned integer"
(( CAMPAIGN_INDEX < 100 )) || die "CAMPAIGN_INDEX must be in [0,100) for the audited sequence namespace"
[[ "$RUNG_REPLICAS" == 20 || "$RUNG_REPLICAS" == 30 || "$RUNG_REPLICAS" == 40 || "$RUNG_REPLICAS" == 50 ]] \
  || die "RUNG_REPLICAS must be 20, 30, 40, or 50"
[[ "$PLAN_ONLY" == 0 || "$PLAN_ONLY" == 1 ]] || die "PLAN_ONLY must be 0 or 1"
for pair in \
  "START_LEAD_SECONDS:$START_LEAD_SECONDS" "ARMED_LEAD_SECONDS:$ARMED_LEAD_SECONDS" \
  "BATCH_STABILITY_SECONDS:$BATCH_STABILITY_SECONDS" \
  "BATCH_STARTUP_TIMEOUT_SECONDS:$BATCH_STARTUP_TIMEOUT_SECONDS" \
  "HEALTH_INTERVAL_SECONDS:$HEALTH_INTERVAL_SECONDS" "MAX_DRAIN_SECONDS:$MAX_DRAIN_SECONDS" \
  "METRIC_SETTLE_SECONDS:$METRIC_SETTLE_SECONDS" \
  "TARGET_COUNTER_SETTLE_SECONDS:$TARGET_COUNTER_SETTLE_SECONDS"; do
  positive_integer "${pair%%:*}" "${pair#*:}"
done
(( START_LEAD_SECONDS == 180 && ARMED_LEAD_SECONDS == 90 )) || die "frozen protocol requires start/ARMED lead 180s/90s"
(( BATCH_STABILITY_SECONDS == 120 )) || die "frozen protocol requires 120s stability per +2 batch"
(( HEALTH_INTERVAL_SECONDS <= 10 )) || die "health interval cannot exceed 10s"
(( MAX_DRAIN_SECONDS == 90 )) || die "frozen protocol requires a 90s maximum drain"
[[ "$OC_REQUEST_TIMEOUT" =~ ^[1-9][0-9]*s$ ]] || die "OC_REQUEST_TIMEOUT must be positive whole seconds"
[[ -x "$PLAN_RUNNER" && -x "$PREFLIGHT_RUNNER" && -x "$SUMMARY_RUNNER" && -x "$TOPOLOGY_RUNNER" ]] \
  || die "one or more framework runners are not executable"

[[ ! -e "$RUN_DIR" ]] || die "refusing to overwrite existing run directory: ${RUN_DIR}"
mkdir -p "$RUN_DIR"
run_dir_owned=1
git -C "$REPO_ROOT" rev-parse HEAD >"${RUN_DIR}/git-head.txt"
git -C "$REPO_ROOT" status --short >"${RUN_DIR}/git-status.txt"
driver_probe_source=${DRIVER_PROBE_SOURCE:-${REPO_ROOT}/instrumentation/reference/src/bin/sustained-corpus-probe.rs}
[[ -s "$driver_probe_source" ]] || die "missing benchmark-driver probe source: ${driver_probe_source}"
local_driver_source_sha=$(sha256_path "$driver_probe_source")
[[ "$local_driver_source_sha" == "$ARMED_DRIVER_SOURCE_SHA256" ]] \
  || die "local ARMED driver source does not match the attested image provenance"

python3 "$PLAN_RUNNER" \
  --run-id "$SCALE_RUN_ID" --campaign-index "$CAMPAIGN_INDEX" --replicas "$RUNG_REPLICAS" \
  --namespace "$NAMESPACE" --target-node "$TARGET_NODE" --driver-node "$DRIVER_NODE" \
  --output-plan "${RUN_DIR}/campaign-plan.json" --output-ledger "${RUN_DIR}/sequence-ledger.json" \
  --ledger-root "$RESULT_ROOT" || die "campaign plan or irreversible sequence allocation failed"
jq -n --arg sha "$(sha256_path "$PLAN_RUNNER")" --arg preflight "$(sha256_path "$PREFLIGHT_RUNNER")" \
  --arg summary "$(sha256_path "$SUMMARY_RUNNER")" --arg topology "$(sha256_path "$TOPOLOGY_RUNNER")" \
  --arg driver_source "$local_driver_source_sha" \
  '{schema_version:1,planner_sha256:$sha,capacity_preflight_sha256:$preflight,
    summarizer_sha256:$summary,topology_runner_sha256:$topology,
    local_driver_source_sha256:$driver_source}' >"${RUN_DIR}/framework-provenance.json"

if (( PLAN_ONLY == 1 )); then
  jq . "${RUN_DIR}/campaign-plan.json"
  exit 0
fi

for command in oc curl cmp sed; do require_command "$command"; done
live_started=1

"${k[@]}" get deployment "$REFERENCE_DEPLOYMENT" -n "$NAMESPACE" -o json \
  >"${RUN_DIR}/reference-deployment-before.json"
jq -e '.spec.replicas==0 and ((.status.replicas//0)==0) and ((.status.availableReplicas//0)==0)' \
  "${RUN_DIR}/reference-deployment-before.json" >/dev/null \
  || die "reference Deployment must be parked at zero; the separate scaleout workload will not mutate it"

for node in "$TARGET_NODE" "$DRIVER_NODE"; do
  "${k[@]}" wait --for=condition=Ready "node/${node}" --timeout=60s >/dev/null \
    || die "node ${node} is not Ready"
done

capture_capacity_inputs() {
  local prefix
  prefix=$1
  "${k[@]}" get resourcequota -n "$NAMESPACE" -o json >"${RUN_DIR}/${prefix}-resourcequotas.json"
  "${k[@]}" get pods -A -o json >"${RUN_DIR}/${prefix}-all-pods.json"
  "${k[@]}" get nodes "$TARGET_NODE" "$DRIVER_NODE" -o json >"${RUN_DIR}/${prefix}-nodes.json"
  "${k[@]}" get pvc "$MODEL_CLAIM" -n "$NAMESPACE" -o json >"${RUN_DIR}/${prefix}-model-pvc.json"
}

run_capacity_preflight() {
  local prefix
  local output
  prefix=$1
  output=$2
  python3 "$PREFLIGHT_RUNNER" \
    --resourcequotas "${RUN_DIR}/${prefix}-resourcequotas.json" \
    --pods "${RUN_DIR}/${prefix}-all-pods.json" --nodes "${RUN_DIR}/${prefix}-nodes.json" \
    --pvc "${RUN_DIR}/${prefix}-model-pvc.json" --replicas "$RUNG_REPLICAS" \
    --namespace "$NAMESPACE" --target-node "$TARGET_NODE" --driver-node "$DRIVER_NODE" \
    --claim-name "$MODEL_CLAIM" --output "$output"
}

capture_capacity_inputs capacity-initial
run_capacity_preflight capacity-initial "${RUN_DIR}/capacity-preflight-initial.json" \
  || die "initial quota/node/storage preflight denied r${RUNG_REPLICAS} before any workload mutation"

lock_manifest=$(jq -cn --arg name "$LOCK_NAME" --arg namespace "$NAMESPACE" --arg run "$SCALE_RUN_ID" \
  '{apiVersion:"v1",kind:"ConfigMap",metadata:{name:$name,namespace:$namespace},
    data:{"run-id":$run,"kind":"unchanged-sc-horizontal-scale"}}')
if ! printf '%s\n' "$lock_manifest" | "${k[@]}" create -f - >/dev/null; then
  die "benchmark lock ${LOCK_NAME} is already held"
fi
lock_acquired=1

capture_capacity_inputs capacity-locked
run_capacity_preflight capacity-locked "${RUN_DIR}/capacity-preflight.json" \
  || die "locked quota/node/storage preflight denied r${RUNG_REPLICAS}"

"${k[@]}" get events -n "$NAMESPACE" -o json >"${RUN_DIR}/events-before.json"
"${k[@]}" get pods -n "$NAMESPACE" -l "$RUN_SELECTOR" -o json >"${RUN_DIR}/run-pods-before.json"
jq -e '.items|length==0' "${RUN_DIR}/run-pods-before.json" >/dev/null \
  || die "run label already selects Pods; workload separation is not clean"

jq -n \
  --arg name "$TARGET_DEPLOYMENT" --arg namespace "$NAMESPACE" --arg run "$SCALE_RUN_ID" \
  --arg node "$TARGET_NODE" --arg image "$TARGET_IMAGE_REF" --arg claim "$MODEL_CLAIM" '
  {apiVersion:"apps/v1",kind:"Deployment",metadata:{name:$name,namespace:$namespace,
    labels:{"benchmark.llm-d/run-id":$run,"benchmark.llm-d/component":"scaleout-target"}},
   spec:{replicas:0,revisionHistoryLimit:1,
    selector:{matchLabels:{"benchmark.llm-d/run-id":$run,"benchmark.llm-d/component":"scaleout-target"}},
    strategy:{type:"RollingUpdate",rollingUpdate:{maxSurge:0,maxUnavailable:1}},
    template:{metadata:{labels:{"benchmark.llm-d/run-id":$run,"benchmark.llm-d/component":"scaleout-target"}},
     spec:{nodeSelector:{"kubernetes.io/hostname":$node},
      affinity:{nodeAffinity:{requiredDuringSchedulingIgnoredDuringExecution:{nodeSelectorTerms:[{matchExpressions:[
        {key:"kubernetes.io/hostname",operator:"In",values:[$node]}]}]}}},
      securityContext:{runAsNonRoot:true,seccompProfile:{type:"RuntimeDefault"}},
      containers:[{name:"llm-d-sc",image:$image,imagePullPolicy:"IfNotPresent",
        env:[{name:"LLM_D_SC_MODEL_DIR",value:"/models"},{name:"LLM_D_SC_CLASSIFIER",value:"complexity"},
          {name:"LLM_D_SC_LISTEN",value:"0.0.0.0:50051"},{name:"LLM_D_SC_INFERENCE_WORKERS",value:"1"},
          {name:"LLM_D_SC_METRICS_LOG_SECS",value:"10"},{name:"RAYON_NUM_THREADS",value:"1"}],
        ports:[{name:"grpc",containerPort:50051,protocol:"TCP"}],
        readinessProbe:{tcpSocket:{port:"grpc"},initialDelaySeconds:3,periodSeconds:2,timeoutSeconds:1,failureThreshold:3},
        livenessProbe:{tcpSocket:{port:"grpc"},initialDelaySeconds:30,periodSeconds:30,timeoutSeconds:1,failureThreshold:3},
        resources:{requests:{cpu:"2",memory:"4Gi"},limits:{cpu:"2",memory:"4Gi"}},
        securityContext:{allowPrivilegeEscalation:false,readOnlyRootFilesystem:true,capabilities:{drop:["ALL"]}},
        volumeMounts:[{name:"models",mountPath:"/models",readOnly:true}]}],
      volumes:[{name:"models",persistentVolumeClaim:{claimName:$claim}}]}}}}' \
  >"${RUN_DIR}/target-deployment-manifest.json"
"${k[@]}" create -f "${RUN_DIR}/target-deployment-manifest.json" \
  -o json >"${RUN_DIR}/target-deployment-created.json"
target_created=1

mkdir -p "${RUN_DIR}/scale-batches"
printf '{"schema_version":1,"batches":[' >"${RUN_DIR}/scale-batches.json.partial"
batch_first=1
for (( replicas=2; replicas<=RUNG_REPLICAS; replicas+=2 )); do
  batch_dir="${RUN_DIR}/scale-batches/r${replicas}"
  mkdir -p "$batch_dir"
  "${k[@]}" scale deployment "$TARGET_DEPLOYMENT" -n "$NAMESPACE" --replicas="$replicas" >/dev/null
  ready_deadline=$(( $(date -u +%s) + BATCH_STARTUP_TIMEOUT_SECONDS ))
  while true; do
    "${k[@]}" get deployment "$TARGET_DEPLOYMENT" -n "$NAMESPACE" -o json >"${batch_dir}/deployment-current.json"
    "${k[@]}" get pods -n "$NAMESPACE" -l "$TARGET_SELECTOR" -o json >"${batch_dir}/pods-current.json"
    if jq -e --argjson replicas "$replicas" '
        .spec.replicas==$replicas and (.status.observedGeneration//0)>=.metadata.generation
        and (.status.replicas//0)==$replicas and (.status.updatedReplicas//0)==$replicas
        and (.status.availableReplicas//0)==$replicas and (.status.unavailableReplicas//0)==0' \
        "${batch_dir}/deployment-current.json" >/dev/null && \
       jq -e --argjson replicas "$replicas" --arg node "$TARGET_NODE" --arg digest "$TARGET_IMAGE_DIGEST" '
        (.items|length)==$replicas and all(.items[];
          .metadata.deletionTimestamp==null and .status.phase=="Running" and .spec.nodeName==$node
          and .status.qosClass=="Guaranteed" and any(.status.conditions[]?;.type=="Ready" and .status=="True")
          and (.status.containerStatuses|length)==1 and .status.containerStatuses[0].restartCount==0
          and (.status.containerStatuses[0].imageID|endswith($digest)))' \
        "${batch_dir}/pods-current.json" >/dev/null; then
      break
    fi
    (( $(date -u +%s) < ready_deadline )) || die "r${replicas} did not become fully Ready before the batch timeout"
    sleep 5
  done
  uid_set=$(jq -r '[.items[].metadata.uid]|sort|join(",")' "${batch_dir}/pods-current.json")
  stable_start=$(date -u +%s)
  stable_end=$((stable_start + BATCH_STABILITY_SECONDS))
  while (( $(date -u +%s) < stable_end )); do
    "${k[@]}" get pods -n "$NAMESPACE" -l "$TARGET_SELECTOR" -o json >"${batch_dir}/pods-stability-current.json"
    current_uid_set=$(jq -r '[.items[].metadata.uid]|sort|join(",")' "${batch_dir}/pods-stability-current.json")
    [[ "$current_uid_set" == "$uid_set" ]] || die "r${replicas} target UID set changed during the 120s stability gate"
    jq -e --argjson replicas "$replicas" --arg node "$TARGET_NODE" --arg digest "$TARGET_IMAGE_DIGEST" '
      (.items|length)==$replicas and all(.items[];
        .status.phase=="Running" and .spec.nodeName==$node
        and any(.status.conditions[]?;.type=="Ready" and .status=="True")
        and .status.containerStatuses[0].restartCount==0
        and (.status.containerStatuses[0].imageID|endswith($digest)))' \
      "${batch_dir}/pods-stability-current.json" >/dev/null \
      || die "r${replicas} target health changed during the 120s stability gate"
    for node in "$TARGET_NODE" "$DRIVER_NODE"; do
      "${k[@]}" get node "$node" -o json | jq -e 'any(.status.conditions[]?;.type=="Ready" and .status=="True")' >/dev/null \
        || die "node ${node} became non-Ready during scale stability"
    done
    sleep 5
  done
  "${k[@]}" get pods -n "$NAMESPACE" -l "$TARGET_SELECTOR" -o json >"${batch_dir}/pods-stable.json"
  jq -n --argjson replicas "$replicas" --argjson start "$stable_start" --argjson end "$(date -u +%s)" \
    --arg uid_set "$uid_set" '{replicas:$replicas,stable_start_epoch_s:$start,stable_end_epoch_s:$end,
      stable_seconds_observed:($end-$start),ready_replicas:$replicas,uid_set_sha256:null,
      uid_set:$uid_set,uid_set_stable:true,restart_free:true,nodes_ready:true}' >"${batch_dir}/batch.json"
  if (( batch_first == 0 )); then printf ',' >>"${RUN_DIR}/scale-batches.json.partial"; fi
  jq -c . "${batch_dir}/batch.json" >>"${RUN_DIR}/scale-batches.json.partial"
  batch_first=0
done
printf ']}\n' >>"${RUN_DIR}/scale-batches.json.partial"
mv "${RUN_DIR}/scale-batches.json.partial" "${RUN_DIR}/scale-batches.json"

"${k[@]}" get pods -n "$NAMESPACE" -l "$TARGET_SELECTOR" -o json >"${RUN_DIR}/targets-inventory-snapshot.json"
topology_stdout="${RUN_DIR}/topology-preflight-stdout.txt"
topology_stderr="${RUN_DIR}/topology-preflight-stderr.txt"
set +e
python3 "$TOPOLOGY_RUNNER" live --kubeconfig "$KUBECONFIG_PATH" --namespace "$NAMESPACE" \
  --selector "$TARGET_SELECTOR" --expected-pods "$RUNG_REPLICAS" --container "$TARGET_CONTAINER" --format json \
  >"$topology_stdout" 2>"$topology_stderr"
topology_exit=$?
set -e
(( topology_exit == 0 )) || die "exact target topology preflight failed"
jq -s -e 'length==1 and .[0].schema_version==1 and .[0].verdict=="PASS"
  and .[0].placement_verdict=="PASS" and .[0].gate_passed==true' "$topology_stdout" >/dev/null \
  || die "topology preflight did not emit one load-authorizing report"
jq -s '.[0]' "$topology_stdout" >"${RUN_DIR}/topology-preflight-report.json"
jq -e --argjson replicas "$RUNG_REPLICAS" '
  .summary.pods==$replicas and .summary.pods_validated==$replicas
  and (.pods|length)==$replicas and all(.pods[];
    .complete_smt_sibling_sets==true and (.cpuset|type=="string" and length>0))' \
  "${RUN_DIR}/topology-preflight-report.json" >/dev/null \
  || die "topology report lacks exact complete-SMT coverage for every target"

mkdir -p "${RUN_DIR}/target-runtime"
jq -n '{schema_version:1,pods:[]}' >"${RUN_DIR}/target-inventory.json"
endpoint_ordinal=0
while IFS= read -r target_pod; do
  pod_file="${RUN_DIR}/target-runtime/e$(printf '%02d' "$endpoint_ordinal")-pod.json"
  cgroup_file="${RUN_DIR}/target-runtime/e$(printf '%02d' "$endpoint_ordinal")-cgroup.txt"
  process_file="${RUN_DIR}/target-runtime/e$(printf '%02d' "$endpoint_ordinal")-process.txt"
  "${k[@]}" get pod "$target_pod" -n "$NAMESPACE" -o json >"$pod_file"
  "${k[@]}" exec -n "$NAMESPACE" "$target_pod" -c "$TARGET_CONTAINER" -- sh -c '
    printf "cpuset_cpus_effective "; cat /sys/fs/cgroup/cpuset.cpus.effective
    printf "cpu_max "; cat /sys/fs/cgroup/cpu.max' >"$cgroup_file" \
    || die "cannot capture pre-load cgroup for ${target_pod}"
  "${k[@]}" exec -n "$NAMESPACE" "$target_pod" -c "$TARGET_CONTAINER" -- sh -c '
    printf "pid1_exe "; readlink /proc/1/exe
    tr "\000" "\n" </proc/1/environ | awk -F= "\$1==\"LLM_D_SC_INFERENCE_WORKERS\" || \$1==\"RAYON_NUM_THREADS\" || \$1==\"CANDLE_NUM_THREADS\" || \$1==\"LLM_D_SC_METRICS_LOG_SECS\""' \
    >"$process_file" || die "cannot capture pre-load PID1 environment for ${target_pod}"
  cpuset=$(awk '$1=="cpuset_cpus_effective"{print $2}' "$cgroup_file")
  cpu_max=$(awk '$1=="cpu_max"{$1="";sub(/^ /,"");print}' "$cgroup_file")
  pid1_executable=$(awk '$1=="pid1_exe"{print $2}' "$process_file")
  workers=$(awk -F= '$1=="LLM_D_SC_INFERENCE_WORKERS"{print $2}' "$process_file")
  rayon=$(awk -F= '$1=="RAYON_NUM_THREADS"{print $2}' "$process_file")
  metrics_log=$(awk -F= '$1=="LLM_D_SC_METRICS_LOG_SECS"{print $2}' "$process_file")
  candle_count=$(awk -F= '$1=="CANDLE_NUM_THREADS"{n++} END{print n+0}' "$process_file")
  [[ "$pid1_executable" == /usr/local/bin/llm-d-sc && "$workers" == 1 && "$rayon" == 1 && "$metrics_log" == 10 && "$candle_count" == 0 ]] \
    || die "${target_pod} actual PID1 environment is not exact W1/RT1/Candle-unset"
  topology_row=$(jq -c --arg name "$target_pod" '.pods[]|select(.name==$name)' "${RUN_DIR}/topology-preflight-report.json")
  [[ -n "$topology_row" ]] || die "${target_pod} missing from topology report"
  [[ "$(jq -r .cpuset <<<"$topology_row")" == "$cpuset" ]] || die "${target_pod} runtime/topology cpuset mismatch"
  row=$(jq -n --slurpfile pod "$pod_file" --argjson endpoint "$endpoint_ordinal" \
    --arg cpuset "$cpuset" --arg cpu_max "$cpu_max" --arg executable "$pid1_executable" \
    --arg workers "$workers" --arg rayon "$rayon" --arg metrics_log "$metrics_log" \
    --argjson topology "$topology_row" '
    ($pod[0]) as $p | {endpoint_ordinal:$endpoint,name:$p.metadata.name,uid:$p.metadata.uid,
      ip:$p.status.podIP,node:$p.spec.nodeName,ready:any($p.status.conditions[]?;.type=="Ready" and .status=="True"),
      restart_count:$p.status.containerStatuses[0].restartCount,image_id:$p.status.containerStatuses[0].imageID,
      qos_class:$p.status.qosClass,cpuset_cpus_effective:$cpuset,cpu_max:$cpu_max,
      complete_smt_sibling_sets:$topology.complete_smt_sibling_sets,pid1_executable:$executable,
      environment:{LLM_D_SC_INFERENCE_WORKERS:$workers,RAYON_NUM_THREADS:$rayon,
        LLM_D_SC_METRICS_LOG_SECS:$metrics_log}}')
  jq --argjson row "$row" '.pods += [$row]' "${RUN_DIR}/target-inventory.json" \
    >"${RUN_DIR}/target-inventory.json.next"
  mv "${RUN_DIR}/target-inventory.json.next" "${RUN_DIR}/target-inventory.json"
  endpoint_ordinal=$((endpoint_ordinal + 1))
done < <(jq -r '.items|sort_by(.metadata.name)[].metadata.name' "${RUN_DIR}/targets-inventory-snapshot.json")
(( endpoint_ordinal == RUNG_REPLICAS )) || die "target runtime inventory count mismatch"

mkdir -p "${RUN_DIR}/target-logs-baseline"
jq -n '{schema_version:1,pods:[]}' >"${RUN_DIR}/target-counters-baseline.json"
sleep 12
while IFS=$'\t' read -r endpoint target_pod started_at; do
  log_file="${RUN_DIR}/target-logs-baseline/e$(printf '%02d' "$endpoint").txt"
  "${k[@]}" logs -n "$NAMESPACE" "$target_pod" -c "$TARGET_CONTAINER" --timestamps=true --since-time="$started_at" >"$log_file"
  grep -q 'llm-d-sc metrics:' "$log_file" && die "endpoint ${endpoint} is not traffic-clean before first load"
  jq --argjson endpoint "$endpoint" --arg pod "$target_pod" \
    '.pods += [{endpoint_ordinal:$endpoint,pod_name:$pod,counters:{served:0,hits:0,misses:0}}]' \
    "${RUN_DIR}/target-counters-baseline.json" >"${RUN_DIR}/target-counters-baseline.json.next"
  mv "${RUN_DIR}/target-counters-baseline.json.next" "${RUN_DIR}/target-counters-baseline.json"
done < <(jq -r --slurpfile snapshot "${RUN_DIR}/targets-inventory-snapshot.json" '
  .pods[] as $p | ($snapshot[0].items[]|select(.metadata.name==$p.name)) as $raw
  | [$p.endpoint_ordinal,$p.name,$raw.status.containerStatuses[0].state.running.startedAt]|@tsv' "${RUN_DIR}/target-inventory.json")

: >"${RUN_DIR}/health-monitor.ndjson"
capture_health() {
  local pods_file
  local nodes_file
  local sample_file
  pods_file="${RUN_DIR}/health-current-pods.json"
  nodes_file="${RUN_DIR}/health-current-nodes.json"
  sample_file="${RUN_DIR}/health-current-sample.json"
  "${k[@]}" get pods -n "$NAMESPACE" -l "$TARGET_SELECTOR" -o json >"$pods_file" \
    || die "health capture cannot list target Pods"
  "${k[@]}" get nodes "$TARGET_NODE" "$DRIVER_NODE" -o json >"$nodes_file" \
    || die "health capture cannot list nodes"
  jq -n --slurpfile pods "$pods_file" --slurpfile nodes "$nodes_file" \
    --argjson epoch "$(date -u +%s)" --arg target_node "$TARGET_NODE" --arg driver_node "$DRIVER_NODE" '
    {schema_version:1,sample_epoch_s:$epoch,
     targets:[$pods[0].items[]|{name:.metadata.name,uid:.metadata.uid,ip:.status.podIP,
       image_id:.status.containerStatuses[0].imageID,ready:any(.status.conditions[]?;.type=="Ready" and .status=="True"),
       restart_count:.status.containerStatuses[0].restartCount}],
     nodes_ready:all(([$target_node,$driver_node]|unique[]); . as $name
       | any($nodes[0].items[]?;.metadata.name==$name and any(.status.conditions[]?;.type=="Ready" and .status=="True")))}' \
    >"$sample_file"
  jq -e --slurpfile expected "${RUN_DIR}/target-inventory.json" '
    ([.targets[]|[.uid,.ip,.image_id]]|sort)==([$expected[0].pods[]|[.uid,.ip,.image_id]]|sort)
    and (.targets|length)==($expected[0].pods|length)
    and all(.targets[];.ready==true and .restart_count==0) and .nodes_ready==true' "$sample_file" >/dev/null \
    || die "target identity/readiness/restart/image or node readiness changed"
  jq -c . "$sample_file" >>"${RUN_DIR}/health-monitor.ndjson"
}

capture_health
mkdir -p "${RUN_DIR}/cells"
cell_count=$(jq '.cells|length' "${RUN_DIR}/campaign-plan.json")
for (( cell_ordinal=0; cell_ordinal<cell_count; cell_ordinal++ )); do
  cell_id=$(jq -r --argjson ordinal "$cell_ordinal" '.cells[]|select(.ordinal==$ordinal)|.cell_id' "${RUN_DIR}/campaign-plan.json")
  cell_dir="${RUN_DIR}/cells/c$(printf '%02d' "$cell_ordinal")-${cell_id}"
  mkdir -p "${cell_dir}/job-manifests" "${cell_dir}/arming" "${cell_dir}/drivers"
  existing_jobs=$("${k[@]}" get jobs -n "$NAMESPACE" -l "$RUN_SELECTOR" -o json | jq '.items|length')
  (( existing_jobs == 0 )) || die "${cell_id}: prior-cell Jobs remain; cells would overlap"
  start_epoch_ms=$(( $(date -u +%s) * 1000 + START_LEAD_SECONDS * 1000 ))
  end_epoch_ms=$((start_epoch_ms + 180000))
  jq -n --arg cell "$cell_id" --argjson ordinal "$cell_ordinal" --argjson start "$start_epoch_ms" \
    --argjson end "$end_epoch_ms" --argjson expected "$(jq --argjson ordinal "$cell_ordinal" '[.jobs[]|select(.cell_ordinal==$ordinal)]|length' "${RUN_DIR}/campaign-plan.json")" \
    '{schema_version:1,cell_id:$cell,ordinal:$ordinal,start_epoch_ms:$start,end_epoch_ms:$end,
      duration_seconds:180,expected_jobs:$expected,armed_verified_epoch_ms:null,
      jobs_deleted_before_next_cell:false,completed_epoch_ms:null}' >"${cell_dir}/cell-runtime.json"

  while IFS= read -r job_spec; do
    job_name=$(jq -r .job_id <<<"$job_spec")
    endpoint=$(jq -r .endpoint_ordinal <<<"$job_spec")
    rate=$(jq -r .offered_rps <<<"$job_spec")
    slots=$(jq -r .expected_slots <<<"$job_spec")
    sequence=$(jq -r .sequence_base <<<"$job_spec")
    nonce=$(jq -r .arming_nonce <<<"$job_spec")
    target_ip=$(jq -r --argjson endpoint "$endpoint" '.pods[]|select(.endpoint_ordinal==$endpoint)|.ip' "${RUN_DIR}/target-inventory.json")
    target_uid=$(jq -r --argjson endpoint "$endpoint" '.pods[]|select(.endpoint_ordinal==$endpoint)|.uid' "${RUN_DIR}/target-inventory.json")
    target_pod=$(jq -r --argjson endpoint "$endpoint" '.pods[]|select(.endpoint_ordinal==$endpoint)|.name' "${RUN_DIR}/target-inventory.json")
    "${k[@]}" create job "$job_name" -n "$NAMESPACE" --image="$ARMED_DRIVER_IMAGE" --dry-run=client -o json \
      >"${cell_dir}/job-manifests/${job_name}.base.json"
    jq --arg run "$SCALE_RUN_ID" --arg cell "$cell_id" --arg ordinal "$cell_ordinal" \
      --arg endpoint "$endpoint" --arg target_ip "$target_ip" --arg target_uid "$target_uid" \
      --arg target_pod "$target_pod" --arg node "$DRIVER_NODE" --arg image "$ARMED_DRIVER_IMAGE" \
      --arg start "$start_epoch_ms" --arg rate "$rate" --arg slots "$slots" --arg sequence "$sequence" \
      --arg nonce "$nonce" --arg topology "cross-node-direct-${TARGET_NODE}-from-${DRIVER_NODE}" \
      --arg target_image "$TARGET_IMAGE_DIGEST" --arg model "$MODEL_SHA256" --arg tokenizer "$TOKENIZER_SHA256" '
      .metadata.labels += {"benchmark.llm-d/run-id":$run,"benchmark.llm-d/component":"scaleout-driver",
        "benchmark.llm-d/cell":$cell,"benchmark.llm-d/cell-ordinal":$ordinal,
        "benchmark.llm-d/endpoint-ordinal":$endpoint}
      | .metadata.annotations += {"benchmark.llm-d/target-pod":$target_pod,
          "benchmark.llm-d/target-uid":$target_uid,"benchmark.llm-d/target-ip":$target_ip,
          "benchmark.llm-d/start-epoch-ms":$start}
      | .spec.suspend=true | .spec.backoffLimit=0 | .spec.activeDeadlineSeconds=600
      | .spec.ttlSecondsAfterFinished=86400
      | .spec.template.metadata.labels += {"benchmark.llm-d/run-id":$run,
          "benchmark.llm-d/component":"scaleout-driver","benchmark.llm-d/cell":$cell,
          "benchmark.llm-d/cell-ordinal":$ordinal,"benchmark.llm-d/endpoint-ordinal":$endpoint}
      | .spec.template.spec.nodeSelector={"kubernetes.io/hostname":$node}
      | .spec.template.spec.securityContext={runAsNonRoot:true,seccompProfile:{type:"RuntimeDefault"}}
      | .spec.template.spec.containers[0].command=["/usr/local/bin/llm-d-sc-sustained-corpus-probe"]
      | .spec.template.spec.containers[0].args=["--target",($target_ip+":50051"),"--token-count","64",
          "--sequence-base",$sequence,"--max-rows","10000","--tokenizer-sha256",$tokenizer,
          "--concurrency","1","--connections","1","--warmup-requests","0","--duration-seconds","180",
          "--start-epoch-ms",$start,"--target-image",$target_image,"--model-sha256",$model,
          "--topology",$topology,"--raw-latencies","--driver-image",$image,"--offered-rps",$rate,
          "--max-in-flight","512","--dispatch-late-after-ms","1","--drop-late-after-ms","100",
          "--rpc-timeout-ms","30000","--armed-run-id",$run,"--armed-job-id",.metadata.name,
          "--armed-nonce",$nonce]
      | .spec.template.spec.containers[0].resources={requests:{cpu:"500m",memory:"256Mi"},limits:{cpu:"4",memory:"1Gi"}}
      | .spec.template.spec.containers[0].securityContext={allowPrivilegeEscalation:false,
          readOnlyRootFilesystem:true,capabilities:{drop:["ALL"]}}' \
      "${cell_dir}/job-manifests/${job_name}.base.json" >"${cell_dir}/job-manifests/${job_name}.json"
    "${k[@]}" create -f "${cell_dir}/job-manifests/${job_name}.json" >/dev/null \
      || die "${cell_id}: failed to precreate ${job_name}"
    capture_health
  done < <(jq -c --argjson ordinal "$cell_ordinal" '.jobs[]|select(.cell_ordinal==$ordinal)' "${RUN_DIR}/campaign-plan.json")

  expected_jobs=$(jq --argjson ordinal "$cell_ordinal" '[.jobs[]|select(.cell_ordinal==$ordinal)]|length' "${RUN_DIR}/campaign-plan.json")
  "${k[@]}" get jobs -n "$NAMESPACE" -l "${RUN_SELECTOR},benchmark.llm-d/cell-ordinal=${cell_ordinal}" -o json \
    >"${cell_dir}/jobs-precreated.json"
  jq -e --argjson expected "$expected_jobs" '(.items|length)==$expected and all(.items[];.spec.suspend==true)' \
    "${cell_dir}/jobs-precreated.json" >/dev/null || die "${cell_id}: suspended Job set is incomplete"
  while IFS= read -r job_name; do
    "${k[@]}" patch job "$job_name" -n "$NAMESPACE" --type=merge -p '{"spec":{"suspend":false}}' >/dev/null \
      || die "${cell_id}: failed to release ${job_name}"
  done < <(jq -r --argjson ordinal "$cell_ordinal" '.jobs[]|select(.cell_ordinal==$ordinal)|.job_id' "${RUN_DIR}/campaign-plan.json")

  armed_deadline_s=$((start_epoch_ms / 1000 - ARMED_LEAD_SECONDS))
  armed_ok=0
  while (( $(date -u +%s) < armed_deadline_s )); do
    capture_health
    while IFS=$'\t' read -r endpoint job_name; do
      "${k[@]}" logs -n "$NAMESPACE" job/"$job_name" >"${cell_dir}/arming/e$(printf '%02d' "$endpoint").stdout" 2>/dev/null || true
    done < <(jq -r --argjson ordinal "$cell_ordinal" '.jobs[]|select(.cell_ordinal==$ordinal)|[.endpoint_ordinal,.job_id]|@tsv' "${RUN_DIR}/campaign-plan.json")
    if python3 "$SUMMARY_RUNNER" "$RUN_DIR" --validate-armed-cell "$cell_ordinal" \
        --output "${cell_dir}/driver-armed.json" >/dev/null 2>"${cell_dir}/armed-validation-last-error.txt"; then
      armed_ok=1
      break
    fi
    sleep 2
  done
  (( armed_ok == 1 )) || die "${cell_id}: complete explicit ARMED barrier did not close by T0-90s"
  armed_verified_epoch_ms=$(jq -r .verified_epoch_ms "${cell_dir}/driver-armed.json")
  jq --argjson verified "$armed_verified_epoch_ms" '.armed_verified_epoch_ms=$verified' "${cell_dir}/cell-runtime.json" \
    >"${cell_dir}/cell-runtime.json.next"
  mv "${cell_dir}/cell-runtime.json.next" "${cell_dir}/cell-runtime.json"

  "${k[@]}" get pods -n "$NAMESPACE" -l "${RUN_SELECTOR},benchmark.llm-d/cell-ordinal=${cell_ordinal}" -o json \
    >"${cell_dir}/driver-pods-armed.json"
  driver_digest=${ARMED_DRIVER_IMAGE##*@}
  jq -e --argjson expected "$expected_jobs" --arg node "$DRIVER_NODE" --arg digest "$driver_digest" '
    (.items|length)==$expected and all(.items[];
      .status.phase=="Running" and .spec.nodeName==$node
      and any(.status.conditions[]?;.type=="Ready" and .status=="True")
      and (.status.containerStatuses|length)==1 and .status.containerStatuses[0].restartCount==0
      and (.status.containerStatuses[0].imageID|endswith($digest)))' \
    "${cell_dir}/driver-pods-armed.json" >/dev/null \
    || die "${cell_id}: pinned driver Pods are not Ready/restart-free on ${DRIVER_NODE}"

  completion_deadline_s=$((end_epoch_ms / 1000 + MAX_DRAIN_SECONDS))
  while true; do
    capture_health
    jobs_json=$("${k[@]}" get jobs -n "$NAMESPACE" -l "${RUN_SELECTOR},benchmark.llm-d/cell-ordinal=${cell_ordinal}" -o json)
    job_count=$(jq '.items|length' <<<"$jobs_json")
    failed_count=$(jq '[.items[]|select((.status.failed//0)>0)]|length' <<<"$jobs_json")
    complete_count=$(jq '[.items[]|select(any(.status.conditions[]?;.type=="Complete" and .status=="True"))]|length' <<<"$jobs_json")
    (( job_count == expected_jobs )) || die "${cell_id}: driver Job set changed during measurement"
    (( failed_count == 0 )) || die "${cell_id}: one or more driver Jobs failed"
    if (( complete_count == expected_jobs )); then break; fi
    (( $(date -u +%s) <= completion_deadline_s )) || die "${cell_id}: one or more drivers exceeded the 90s drain limit"
    sleep "$HEALTH_INTERVAL_SECONDS"
  done
  printf '%s\n' "$jobs_json" >"${cell_dir}/jobs-after.json"

  while IFS=$'\t' read -r endpoint job_name; do
    raw_file="${cell_dir}/drivers/e$(printf '%02d' "$endpoint").raw"
    report_file="${cell_dir}/drivers/e$(printf '%02d' "$endpoint").json"
    "${k[@]}" logs -n "$NAMESPACE" job/"$job_name" >"$raw_file" \
      || die "${cell_id}: cannot collect ${job_name} report"
    jq -s -e '[.[]|select(.schema_version==2 and .probe=="sustained_exact_token_corpus")]
      | if length==1 then .[0] else error("expected exactly one final report") end' "$raw_file" >"$report_file" \
      || die "${cell_id}: ${job_name} did not emit one final report"
  done < <(jq -r --argjson ordinal "$cell_ordinal" '.jobs[]|select(.cell_ordinal==$ordinal)|[.endpoint_ordinal,.job_id]|@tsv' "${RUN_DIR}/campaign-plan.json")

  python3 "$SUMMARY_RUNNER" "$RUN_DIR" --validate-completed-cell "$cell_ordinal" \
    --output "${cell_dir}/cell-summary.json" \
    >"${cell_dir}/cell-validation.stdout" 2>"${cell_dir}/cell-validation.stderr" \
    || die "${cell_id}: driver/accounting/transport gate failed"
  completed_epoch_ms=$(( $(date -u +%s) * 1000 ))
  "${k[@]}" delete jobs -n "$NAMESPACE" -l "${RUN_SELECTOR},benchmark.llm-d/cell-ordinal=${cell_ordinal}" \
    --cascade=foreground --wait=true --timeout=300s >/dev/null \
    || die "${cell_id}: failed to delete the serial cell Job set"
  remaining=$("${k[@]}" get jobs -n "$NAMESPACE" -l "$RUN_SELECTOR" -o json | jq '.items|length')
  (( remaining == 0 )) || die "${cell_id}: Job cleanup failed before the next cell"
  jq --argjson completed "$completed_epoch_ms" '
    .completed_epoch_ms=$completed | .jobs_deleted_before_next_cell=true' "${cell_dir}/cell-runtime.json" \
    >"${cell_dir}/cell-runtime.json.next"
  mv "${cell_dir}/cell-runtime.json.next" "${cell_dir}/cell-runtime.json"
  python3 "$SUMMARY_RUNNER" "$RUN_DIR" --ledger-only \
    --output "${RUN_DIR}/sequence-ledger-final.json" >/dev/null \
    || die "${cell_id}: cannot update sequence lifecycle ledger"
  capture_health
done

"${k[@]}" get pods -n "$NAMESPACE" -l "$TARGET_SELECTOR" -o json >"${RUN_DIR}/targets-after.json"
jq -e --slurpfile expected "${RUN_DIR}/target-inventory.json" '
  ([.items[]|[.metadata.uid,.status.podIP,.status.containerStatuses[0].imageID]]|sort)
   ==([$expected[0].pods[]|[.uid,.ip,.image_id]]|sort)
  and all(.items[];any(.status.conditions[]?;.type=="Ready" and .status=="True")
    and .status.containerStatuses[0].restartCount==0)' "${RUN_DIR}/targets-after.json" >/dev/null \
  || die "final target identity/health differs from the frozen inventory"

mkdir -p "${RUN_DIR}/target-logs-final"
counter_deadline=$(( $(date -u +%s) + TARGET_COUNTER_SETTLE_SECONDS ))
while true; do
  counters_ready=1
  jq -n '{schema_version:1,pods:[]}' >"${RUN_DIR}/target-counters-final.json.next"
  while IFS=$'\t' read -r endpoint target_pod started_at; do
    log_file="${RUN_DIR}/target-logs-final/e$(printf '%02d' "$endpoint").txt"
    "${k[@]}" logs -n "$NAMESPACE" "$target_pod" -c "$TARGET_CONTAINER" --timestamps=true --since-time="$started_at" >"$log_file" \
      || die "cannot capture final counters for endpoint ${endpoint}"
    served=$(sed -n 's/.*llm-d-sc metrics: served=\([0-9][0-9]*\).*/\1/p' "$log_file" | tail -n 1)
    hits=$(sed -n 's/.*llm-d-sc metrics: served=[0-9][0-9]* hits=\([0-9][0-9]*\).*/\1/p' "$log_file" | tail -n 1)
    misses=$(sed -n 's/.*llm-d-sc metrics: served=[0-9][0-9]* hits=[0-9][0-9]* misses=\([0-9][0-9]*\).*/\1/p' "$log_file" | tail -n 1)
    expected_ok=$(jq -s --argjson endpoint "$endpoint" '
      [.[].endpoint_results[]|select(.endpoint_ordinal==$endpoint)|.ok_total]|add//0' "${RUN_DIR}"/cells/*/cell-summary.json)
    if [[ -z "$served" || -z "$hits" || -z "$misses" || "$served" != "$expected_ok" ]]; then counters_ready=0; fi
    served=${served:-0}; hits=${hits:-0}; misses=${misses:-0}
    jq --argjson endpoint "$endpoint" --arg pod "$target_pod" --argjson served "$served" \
      --argjson hits "$hits" --argjson misses "$misses" '
      .pods += [{endpoint_ordinal:$endpoint,pod_name:$pod,counters:{served:$served,hits:$hits,misses:$misses}}]' \
      "${RUN_DIR}/target-counters-final.json.next" >"${RUN_DIR}/target-counters-final.json.row"
    mv "${RUN_DIR}/target-counters-final.json.row" "${RUN_DIR}/target-counters-final.json.next"
  done < <(jq -r --slurpfile snapshot "${RUN_DIR}/targets-inventory-snapshot.json" '
    .pods[] as $p | ($snapshot[0].items[]|select(.metadata.name==$p.name)) as $raw
    | [$p.endpoint_ordinal,$p.name,$raw.status.containerStatuses[0].state.running.startedAt]|@tsv' "${RUN_DIR}/target-inventory.json")
  mv "${RUN_DIR}/target-counters-final.json.next" "${RUN_DIR}/target-counters-final.json"
  if (( counters_ready == 1 )); then break; fi
  (( $(date -u +%s) < counter_deadline )) || die "target served counters did not reconcile exactly with endpoint OK completions"
  capture_health
  sleep 2
done

# Runtime T0 values, not the cluster-free plan, define the actual query window.
telemetry_start=$(jq -s '[.[].start_epoch_ms]|min/1000-30' "${RUN_DIR}"/cells/*/cell-runtime.json)
telemetry_end=$(jq -s '[.[].end_epoch_ms]|max/1000+30' "${RUN_DIR}"/cells/*/cell-runtime.json)
capture_after=$(( ${telemetry_end%.*} + METRIC_SETTLE_SECONDS ))
while (( $(date -u +%s) < capture_after )); do capture_health; sleep 2; done

mkdir -p "${RUN_DIR}/metrics"
prom_host=$("${k[@]}" -n openshift-monitoring get route thanos-querier -o jsonpath='{.spec.host}')
auth_token=$("${k[@]}" whoami -t)
query_range() {
  local name
  local query
  name=$1
  query=$2
  curl -ksS --connect-timeout "$CURL_CONNECT_TIMEOUT_SECONDS" --max-time "$CURL_MAX_TIME_SECONDS" \
    --get -H "Authorization: Bearer ${auth_token}" --data-urlencode "query=${query}" \
    --data-urlencode "start=${telemetry_start}" --data-urlencode "end=${telemetry_end}" \
    --data-urlencode 'step=5' "https://${prom_host}/api/v1/query_range" >"${RUN_DIR}/metrics/${name}.json"
  jq -e '.status=="success"' "${RUN_DIR}/metrics/${name}.json" >/dev/null || die "telemetry query failed: ${name}"
}
pod_regex="${TARGET_DEPLOYMENT}-.*"
query_range pod_cpu_otel "k8s_pod_cpu_usage{k8s_namespace_name=\"${NAMESPACE}\",k8s_pod_name=~\"${pod_regex}\"}"
query_range container_cpu_otel "container_cpu_usage{k8s_namespace_name=\"${NAMESPACE}\",k8s_pod_name=~\"${pod_regex}\",k8s_container_name=\"${TARGET_CONTAINER}\"}"
query_range container_cpu_cadvisor "sum by (pod)(rate(container_cpu_usage_seconds_total{namespace=\"${NAMESPACE}\",pod=~\"${pod_regex}\",container=\"${TARGET_CONTAINER}\"}[30s]))"
query_range throttle_ratio "sum by (pod)(rate(container_cpu_cfs_throttled_periods_total{namespace=\"${NAMESPACE}\",pod=~\"${pod_regex}\",container=\"${TARGET_CONTAINER}\"}[30s])) / sum by (pod)(rate(container_cpu_cfs_periods_total{namespace=\"${NAMESPACE}\",pod=~\"${pod_regex}\",container=\"${TARGET_CONTAINER}\"}[30s]))"
query_range memory_working_set "container_memory_working_set_bytes{namespace=\"${NAMESPACE}\",pod=~\"${pod_regex}\",container=\"${TARGET_CONTAINER}\"}"
query_range cpu_pressure_waiting "rate(container_pressure_cpu_waiting_seconds_total{namespace=\"${NAMESPACE}\",pod=~\"${pod_regex}\",container=\"${TARGET_CONTAINER}\"}[30s])"
query_range restarts "kube_pod_container_status_restarts_total{namespace=\"${NAMESPACE}\",pod=~\"${pod_regex}\",container=\"${TARGET_CONTAINER}\"}"
query_range pod_ready "kube_pod_status_ready{namespace=\"${NAMESPACE}\",pod=~\"${pod_regex}\",condition=\"true\"}"
query_range node_ready "kube_node_status_condition{condition=\"Ready\",status=\"true\",node=~\"${TARGET_NODE}|${DRIVER_NODE}\"}"
unset auth_token
jq -n --argjson start "$telemetry_start" --argjson end "$telemetry_end" '
  {schema_version:1,start_epoch_s:$start,end_epoch_s:$end,step_seconds:5,max_gap_seconds:10}' \
  >"${RUN_DIR}/telemetry-window.json"

"${k[@]}" get events -n "$NAMESPACE" -o json >"${RUN_DIR}/events-after.json"
target_uids=$(jq '[.pods[].uid]' "${RUN_DIR}/target-inventory.json")
jq --argjson uids "$target_uids" '
  {schema_version:1,violations:[.items[]|select(.type=="Warning")
    | select(.involvedObject.uid as $uid|$uids|index($uid))
    | {reason,message,involvedObject,firstTimestamp,lastTimestamp,eventTime,count}]}' \
  "${RUN_DIR}/events-after.json" >"${RUN_DIR}/health-event-violations.json"
"${k[@]}" get nodes "$TARGET_NODE" "$DRIVER_NODE" -o json >"${RUN_DIR}/nodes-after.json"

if ! python3 "$SUMMARY_RUNNER" "$RUN_DIR" --output "${RUN_DIR}/campaign-summary.json" \
    >"${RUN_DIR}/campaign-summary.stdout" 2>"${RUN_DIR}/campaign-summary.stderr"; then
  last_error=$(tail -n 1 "${RUN_DIR}/campaign-summary.stderr" 2>/dev/null || true)
  exit 6
fi
measurement_complete=1
final_decision=$(jq -r '.decision.status' "${RUN_DIR}/campaign-summary.json")
cat "${RUN_DIR}/campaign-summary.json"
