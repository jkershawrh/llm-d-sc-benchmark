#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "${SCRIPT_DIR}/.." && pwd)

KUBECONFIG_PATH=${KUBECONFIG_PATH:-/tmp/llm-d-sc-arena-kubeconfig}
NAMESPACE=${NAMESPACE:-llm-d-sc-scaleout}
RUN_ID=${RUN_ID:?set a unique DNS-safe RUN_ID}
DRIVER_IMAGE=${DRIVER_IMAGE:?set the signal-emulator image by digest}
REQUESTS=${REQUESTS:-5000000}
CONCURRENCY=${CONCURRENCY:-125}
TOTAL_CONNECTIONS=${TOTAL_CONNECTIONS:-125}
CONTEXT_BYTES=${CONTEXT_BYTES:-256}
REPETITIONS=${REPETITIONS:-1}
TREATMENTS=${TREATMENTS:-clusterip gateway direct}
FAIL_ON_RESPONSE_ERRORS=${FAIL_ON_RESPONSE_ERRORS:-true}
FAIL_ON_TARGET_HEALTH_ERRORS=${FAIL_ON_TARGET_HEALTH_ERRORS:-true}
METRIC_SETTLE_SECONDS=${METRIC_SETTLE_SECONDS:-12}
METRIC_BRACKET_SECONDS=${METRIC_BRACKET_SECONDS:-45}
DRIVER_NODE=${DRIVER_NODE:-rhgnr1}
TARGET_NODE=${TARGET_NODE:-}
RESULT_ROOT=${RESULT_ROOT:-${REPO_ROOT}/results/transport}
RUN_DIR=${RUN_DIR:-${RESULT_ROOT}/${RUN_ID}}
LOCK_NAME=sc-transport-matrix-lock
MANIFEST=${REPO_ROOT}/deploy/arena/arena-transport-gateway.yaml
TARGET_SELECTOR=benchmark.llm-d/component=transport-target
NETWORK_SUMMARIZER=${REPO_ROOT}/hack/arena-sc-transport-network-summarize.py
RESOURCE_SUMMARIZER=${REPO_ROOT}/hack/arena-sc-transport-resource-summarize.py
EXTERNAL_SUMMARIZER=${REPO_ROOT}/hack/arena-sc-transport-external-summarize.py
HEALTH_SUMMARIZER=${REPO_ROOT}/hack/arena-sc-transport-health-summarize.py
CAMPAIGN_SUMMARIZER=${REPO_ROOT}/hack/arena-sc-transport-summarize.py

k=(oc --kubeconfig "$KUBECONFIG_PATH" --request-timeout=30s)
lock_acquired=0
resources_created=0
last_phase=preflight
failure_class=harness

die() { echo "ERROR: $*" >&2; exit 2; }
positive_integer() { [[ "$2" =~ ^[1-9][0-9]*$ ]] || die "$1 must be positive"; }
oc_retry() {
  for attempt in 1 2 3 4 5; do
    if "${k[@]}" "$@"; then return 0; fi
    echo "OpenShift request failed (attempt ${attempt}/5): oc $*" >&2
    sleep $((attempt * 2))
  done
  return 1
}

capture_active_target_pods() {
  output=$1
  oc_retry get pods -n "$NAMESPACE" -l "$TARGET_SELECTOR" -o json \
    | jq '.items = [.items[] | select(.metadata.deletionTimestamp == null)]' >"$output"
}

wait_for_five_stable_targets() {
  output=$1
  stable_observations=0
  for _ in $(seq 1 120); do
    capture_active_target_pods "$output"
    if jq -e '.items|length==5
      and all(.[]; any(.status.conditions[]?; .type=="Ready" and .status=="True"))' \
      "$output" >/dev/null; then
      stable_observations=$((stable_observations + 1))
      if (( stable_observations == 3 )); then return 0; fi
    else
      stable_observations=0
    fi
    sleep 2
  done
  die "five non-terminating ready transport targets did not stabilize"
}

cleanup() {
  status=$?
  trap - EXIT INT TERM
  set +e
  if (( status != 0 )) && [[ -d "$RUN_DIR" ]]; then
    jq -n --arg run_id "$RUN_ID" --arg failed_at "$(date -u +%FT%TZ)" \
      --arg phase "$last_phase" --argjson exit_code "$status" \
      --arg failure_class "$failure_class" \
      '{schema_version:1,run_id:$run_id,status:"failed",failure_class:$failure_class,
        failed_at:$failed_at,phase:$phase,exit_code:$exit_code}' >"$RUN_DIR/campaign-status.json"
  fi
  if (( lock_acquired == 1 )); then
    oc_retry delete jobs -n "$NAMESPACE" -l "benchmark.llm-d/run-id=${RUN_ID}" \
      --ignore-not-found --wait=true --timeout=300s >/dev/null 2>&1
    if (( resources_created == 1 )); then
      oc_retry delete -f "$MANIFEST" --ignore-not-found --wait=true --timeout=600s >/dev/null 2>&1
    fi
    owner=$(oc_retry get configmap "$LOCK_NAME" -n "$NAMESPACE" -o jsonpath='{.data.run-id}' 2>/dev/null)
    if [[ "$owner" == "$RUN_ID" ]]; then
      oc_retry delete configmap "$LOCK_NAME" -n "$NAMESPACE" --wait=true >/dev/null 2>&1
    fi
  fi
  exit "$status"
}
trap cleanup EXIT INT TERM

for command in curl jq python3; do command -v "$command" >/dev/null || die "missing $command"; done
[[ "$RUN_ID" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ ]] || die "RUN_ID must be DNS-safe"
[[ ! -e "$RUN_DIR" ]] || die "refusing to overwrite $RUN_DIR"
for pair in REQUESTS:$REQUESTS CONCURRENCY:$CONCURRENCY TOTAL_CONNECTIONS:$TOTAL_CONNECTIONS REPETITIONS:$REPETITIONS; do
  positive_integer "${pair%%:*}" "${pair#*:}"
done
read -r -a treatments <<<"$TREATMENTS"
[[ ${#treatments[@]} -gt 0 ]] || die "TREATMENTS cannot be empty"
[[ "$FAIL_ON_RESPONSE_ERRORS" == true || "$FAIL_ON_RESPONSE_ERRORS" == false ]] \
  || die "FAIL_ON_RESPONSE_ERRORS must be true or false"
[[ "$FAIL_ON_TARGET_HEALTH_ERRORS" == true || "$FAIL_ON_TARGET_HEALTH_ERRORS" == false ]] \
  || die "FAIL_ON_TARGET_HEALTH_ERRORS must be true or false"
[[ -n "$DRIVER_NODE" ]] || die "DRIVER_NODE cannot be empty"
if [[ -n "$TARGET_NODE" ]]; then
  [[ "$TARGET_NODE" != "$DRIVER_NODE" ]] \
    || die "TARGET_NODE and DRIVER_NODE must differ when target isolation is requested"
fi
treatment_seen=" "
for treatment in "${treatments[@]}"; do
  [[ "$treatment" == clusterip || "$treatment" == gateway || "$treatment" == direct ]] \
    || die "unknown treatment $treatment"
  [[ "$treatment_seen" != *" $treatment "* ]] || die "duplicate treatment $treatment"
  treatment_seen+="$treatment "
done
oc_retry get --raw=/readyz >/dev/null || die "OpenShift API is not reachable"
mkdir -p "$RUN_DIR"
git -C "$REPO_ROOT" rev-parse HEAD >"$RUN_DIR/framework-git-head.txt"
git -C "$REPO_ROOT" status --short >"$RUN_DIR/framework-git-status.txt"
printf '%s\n' "$DRIVER_IMAGE" >"$RUN_DIR/driver-image.txt"
jq -n --arg driver_node "$DRIVER_NODE" --arg target_node "$TARGET_NODE" \
  '{schema_version:1,driver_node:$driver_node,target_node:($target_node|select(length>0))}' \
  >"$RUN_DIR/requested-topology.json"

if ! "${k[@]}" create configmap "$LOCK_NAME" -n "$NAMESPACE" \
  --from-literal=run-id="$RUN_ID" --from-literal=created-at="$(date -u +%FT%TZ)" >/dev/null; then
  die "another transport matrix owns $LOCK_NAME"
fi
lock_acquired=1

last_phase=deploying-targets
oc_retry apply -f "$MANIFEST" >/dev/null
resources_created=1
if [[ -n "$TARGET_NODE" ]]; then
  target_patch=$(jq -nc --arg node "$TARGET_NODE" \
    '{spec:{template:{spec:{affinity:null,topologySpreadConstraints:null,
      nodeSelector:{"kubernetes.io/hostname":$node}}}}}')
  oc_retry patch deployment classifier-transport-target -n "$NAMESPACE" \
    --type=merge -p "$target_patch" >/dev/null
fi
"${k[@]}" rollout status deployment/classifier-transport-target -n "$NAMESPACE" --timeout=900s
"${k[@]}" wait gateway/classifier-transport-gateway -n "$NAMESPACE" \
  --for=condition=Programmed --timeout=300s
route_accepted=0
for _ in $(seq 1 60); do
  accepted=$(oc_retry get grpcroute classifier-transport-gateway -n "$NAMESPACE" -o json \
    | jq '[.status.parents[]?.conditions[]? | select(.type=="Accepted" and .status=="True")] | length')
  if [[ "$accepted" -gt 0 ]]; then route_accepted=1; break; fi
  sleep 5
done
(( route_accepted == 1 )) || die "GRPCRoute was not accepted within 300 seconds"
sleep "$METRIC_SETTLE_SECONDS"

oc_retry get deployment classifier-transport-target -n "$NAMESPACE" -o json \
  >"$RUN_DIR/target-deployment.json"
oc_retry get gateway classifier-transport-gateway -n "$NAMESPACE" -o json \
  >"$RUN_DIR/gateway.json"
oc_retry get grpcroute classifier-transport-gateway -n "$NAMESPACE" -o json \
  >"$RUN_DIR/grpcroute.json"
wait_for_five_stable_targets "$RUN_DIR/target-pods-start.json"

pod_names=()
while IFS= read -r value; do pod_names+=("$value"); done < <(
  jq -r '.items | sort_by(.metadata.name)[] | select(.status.conditions[]? | .type=="Ready" and .status=="True") | .metadata.name' "$RUN_DIR/target-pods-start.json"
)
pod_ips=()
while IFS= read -r value; do pod_ips+=("$value"); done < <(
  jq -r '.items | sort_by(.metadata.name)[] | select(.status.conditions[]? | .type=="Ready" and .status=="True") | .status.podIP' "$RUN_DIR/target-pods-start.json"
)
[[ ${#pod_ips[@]} -eq 5 ]] || die "expected five ready transport targets, got ${#pod_ips[@]}"
gateway_address=$(jq -r '.status.addresses[0].value // empty' "$RUN_DIR/gateway.json")
[[ -n "$gateway_address" ]] || die "Gateway has no programmed address"
prom_host=$(oc_retry -n openshift-monitoring get route thanos-querier -o jsonpath='{.spec.host}')
auth_token=$(oc_retry whoami -t)
pod_regex=$(printf '%s\n' "${pod_names[@]}" | paste -sd'|' -)
network_query="sum by (pod)(container_network_receive_bytes_total{namespace=\"${NAMESPACE}\",pod=~\"${pod_regex}\",container=\"POD\",interface=\"eth0\"})"

wait_for_network_baseline() {
  baseline_file=$RUN_DIR/network-baseline-query.json
  for _ in $(seq 1 24); do
    curl -ksS --retry 5 --retry-delay 2 --retry-all-errors --connect-timeout 10 --max-time 90 \
      --get -H "Authorization: Bearer ${auth_token}" \
      --data-urlencode "query=${network_query}" \
      "https://${prom_host}/api/v1/query" >"$baseline_file"
    observed=$(jq '[.data.result[]? | .metric.pod] | unique | length' "$baseline_file")
    if [[ "$observed" -eq "${#pod_names[@]}" ]]; then return 0; fi
    sleep 5
  done
  die "OpenShift monitoring did not expose baseline counters for all target Pods"
}

wait_for_network_baseline

capture_network_distribution() {
  cell_dir=$1
  job_json=$2
  start_epoch=$(python3 -c 'import datetime,json,sys; print(int(datetime.datetime.fromisoformat(json.load(open(sys.argv[1]))["status"]["startTime"].replace("Z","+00:00")).timestamp()))' "$job_json")
  completion_epoch=$(python3 -c 'import datetime,json,sys; print(int(datetime.datetime.fromisoformat(json.load(open(sys.argv[1]))["status"]["completionTime"].replace("Z","+00:00")).timestamp()))' "$job_json")
  query_start=$((start_epoch - METRIC_BRACKET_SECONDS))
  query_end=$((completion_epoch + METRIC_BRACKET_SECONDS))
  query_range_to() {
    output=$1
    query=$2
    curl -ksS --retry 5 --retry-delay 2 --retry-all-errors --connect-timeout 10 --max-time 90 \
      --get -H "Authorization: Bearer ${auth_token}" \
      --data-urlencode "query=${query}" \
      --data-urlencode "start=${query_start}" \
      --data-urlencode "end=${query_end}" \
      --data-urlencode 'step=5' \
      "https://${prom_host}/api/v1/query_range" >"$cell_dir/${output}.json"
    jq -e '.status=="success"' "$cell_dir/${output}.json" >/dev/null
  }
  driver_pod=$(jq -r '.items | first | .metadata.name // empty' "$cell_dir/driver-pod.json")
  [[ -n "$driver_pod" ]] || die "driver Pod identity is missing"
  query_range_to network-receive-query "$network_query"
  query_range_to target-cpu-query \
    "sum by (pod)(rate(container_cpu_usage_seconds_total{namespace=\"${NAMESPACE}\",pod=~\"${pod_regex}\",container=\"llm-d-sc\"}[60s]))"
  query_range_to target-throttle-query \
    "sum by (pod)(rate(container_cpu_cfs_throttled_periods_total{namespace=\"${NAMESPACE}\",pod=~\"${pod_regex}\",container=\"llm-d-sc\"}[60s])) / sum by (pod)(rate(container_cpu_cfs_periods_total{namespace=\"${NAMESPACE}\",pod=~\"${pod_regex}\",container=\"llm-d-sc\"}[60s]))"
  query_range_to driver-cpu-query \
    "sum(rate(container_cpu_usage_seconds_total{namespace=\"${NAMESPACE}\",pod=\"${driver_pod}\",container=\"driver\"}[60s]))"
  query_range_to gateway-cpu-query \
    "sum(rate(container_cpu_usage_seconds_total{namespace=\"${NAMESPACE}\",pod=~\"classifier-transport-gateway.*\",container=\"istio-proxy\"}[60s]))"
  node_regex="${TARGET_NODE:-gnr2.fm2aihpcsed.com}|${DRIVER_NODE}"
  query_range_to otel-target-cpu-query \
    "k8s_pod_cpu_usage{k8s_namespace_name=\"${NAMESPACE}\",k8s_pod_name=~\"${pod_regex}\"}"
  query_range_to otel-target-memory-query \
    "k8s_pod_memory_working_set_bytes{k8s_namespace_name=\"${NAMESPACE}\",k8s_pod_name=~\"${pod_regex}\"}"
  query_range_to otel-target-network-errors-query \
    "k8s_pod_network_errors_total{k8s_namespace_name=\"${NAMESPACE}\",k8s_pod_name=~\"${pod_regex}\"}"
  query_range_to otel-collector-up-query \
    "up{namespace=\"${NAMESPACE}\",service=\"llm-d-sc-otel\"}"
  query_range_to node-retransmits-query \
    "node_netstat_Tcp_RetransSegs{instance=~\"${node_regex}\"}"
  query_range_to node-softnet-drops-query \
    "node_softnet_dropped_total{instance=~\"${node_regex}\"}"
  query_range_to node-conntrack-query \
    "node_nf_conntrack_entries{instance=~\"${node_regex}\"}"
  query_range_to node-conntrack-limit-query \
    "node_nf_conntrack_entries_limit{instance=~\"${node_regex}\"}"
  query_range_to node-tcp-inuse-query \
    "node_sockstat_TCP_inuse{instance=~\"${node_regex}\"}"
  query_range_to node-tcp-timewait-query \
    "node_sockstat_TCP_tw{instance=~\"${node_regex}\"}"
  query_range_to node-load1-query \
    "node_load1{instance=~\"${node_regex}\"}"
  pod_error_regex="${pod_regex}|${driver_pod}"
  query_range_to pod-receive-errors-query \
    "sum by (pod)(container_network_receive_errors_total{namespace=\"${NAMESPACE}\",pod=~\"${pod_error_regex}\"})"
  query_range_to pod-transmit-errors-query \
    "sum by (pod)(container_network_transmit_errors_total{namespace=\"${NAMESPACE}\",pod=~\"${pod_error_regex}\"})"
  query_range_to pod-receive-drops-query \
    "sum by (pod)(container_network_receive_packets_dropped_total{namespace=\"${NAMESPACE}\",pod=~\"${pod_error_regex}\"})"
  query_range_to pod-transmit-drops-query \
    "sum by (pod)(container_network_transmit_packets_dropped_total{namespace=\"${NAMESPACE}\",pod=~\"${pod_error_regex}\"})"
  python3 "$NETWORK_SUMMARIZER" "$cell_dir/network-receive-query.json" "$job_json" \
    "$RUN_DIR/target-pods-start.json" "$cell_dir/network-distribution.json"
  python3 "$RESOURCE_SUMMARIZER" "$cell_dir" "$cell_dir/resource-summary.json"
  python3 "$EXTERNAL_SUMMARIZER" "$cell_dir" "$cell_dir/external-telemetry-summary.json"
}

job_manifest() {
  job=$1
  shift
  args_json=$(printf '%s\n' "$@" | jq -R . | jq -s .)
  jq -n \
    --arg name "$job" --arg ns "$NAMESPACE" --arg run "$RUN_ID" --arg image "$DRIVER_IMAGE" \
    --arg driver_node "$DRIVER_NODE" \
    --argjson args "$args_json" \
    '{apiVersion:"batch/v1",kind:"Job",metadata:{name:$name,namespace:$ns,
      labels:{"benchmark.llm-d/run-id":$run,"benchmark.llm-d/component":"signal-emulator"}},
      spec:{backoffLimit:0,ttlSecondsAfterFinished:86400,template:{metadata:{labels:{
        "benchmark.llm-d/run-id":$run,"benchmark.llm-d/component":"signal-emulator"}},
        spec:{restartPolicy:"Never",nodeSelector:{"kubernetes.io/hostname":$driver_node},containers:[{
          name:"driver",image:$image,imagePullPolicy:"IfNotPresent",args:$args,
          resources:{requests:{cpu:"2",memory:"512Mi"},limits:{cpu:"8",memory:"2Gi"}},
          securityContext:{allowPrivilegeEscalation:false,capabilities:{drop:["ALL"]},readOnlyRootFilesystem:true}}]}}}}'
}

run_cell() {
  cell=$1
  repetition=$2
  cell_dir="$RUN_DIR/${repetition}-${cell}"
  mkdir -p "$cell_dir"
  last_phase="${repetition}-${cell}-running"
  capture_active_target_pods "$cell_dir/target-pods-before.json"
  oc_retry get nodes gnr2.fm2aihpcsed.com rhgnr1 -o json >"$cell_dir/nodes-before.json"
  oc_retry get events -n "$NAMESPACE" --sort-by=.lastTimestamp -o json \
    >"$cell_dir/events-before.json"
  jq -e --slurpfile start "$RUN_DIR/target-pods-start.json" '
    .items|length==5
    and all(.[]; any(.status.conditions[]?; .type=="Ready" and .status=="True"))
    and ([.[] | [.metadata.name,.metadata.uid,.status.podIP]] | sort)
      == ([$start[0].items[] | [.metadata.name,.metadata.uid,.status.podIP]] | sort)' \
    "$cell_dir/target-pods-before.json" >/dev/null || {
      failure_class=target_preflight_gate
      die "$cell repetition $repetition did not start with the original five ready targets"
    }
  common=(--run-id "${RUN_ID}-${repetition}-${cell}" --topology "arena-r5-${cell}"
    --cache-mode hit --context-bytes "$CONTEXT_BYTES" --concurrency "$CONCURRENCY"
    --requests "$REQUESTS")
  for ip in "${pod_ips[@]}"; do common+=(--warm-target "${ip}:50051"); done
  case "$cell" in
    clusterip)
      common+=(--connections-per-target "$TOTAL_CONNECTIONS"
        --target "classifier-transport-target.${NAMESPACE}.svc:50051")
      ;;
    gateway)
      common+=(--connections-per-target "$TOTAL_CONNECTIONS" --target "${gateway_address}:50051")
      ;;
    direct)
      (( TOTAL_CONNECTIONS % ${#pod_ips[@]} == 0 )) || die "TOTAL_CONNECTIONS must divide by five"
      common+=(--connections-per-target "$((TOTAL_CONNECTIONS / ${#pod_ips[@]}))")
      for ip in "${pod_ips[@]}"; do common+=(--target "${ip}:50051"); done
      ;;
    *) die "unknown cell $cell" ;;
  esac
  job="sc-${RUN_ID}-${repetition}-${cell}"
  job_manifest "$job" "${common[@]}" | oc_retry apply -f - >/dev/null
  if ! "${k[@]}" wait job/$job -n "$NAMESPACE" --for=condition=Complete --timeout=900s; then
    failure_class=workload_job_gate
    oc_retry get job/$job -n "$NAMESPACE" -o json >"$cell_dir/job-failed.json" || true
    oc_retry logs -n "$NAMESPACE" job/$job >"$cell_dir/driver.log" 2>&1 || true
    die "$cell repetition $repetition did not complete"
  fi
  last_phase="${repetition}-${cell}-harvesting"
  oc_retry logs -n "$NAMESPACE" job/$job >"$cell_dir/result.json"
  jq -e '.kind=="llm-d-sc-signal-emulator-result"
    and .selected_requests>0
    and ([.endpoints[].statuses | to_entries[] | .value] | add)==.selected_requests' \
    "$cell_dir/result.json" >/dev/null || {
      failure_class=workload_accounting_gate
      die "$cell repetition $repetition has invalid response accounting"
    }
  if [[ "$FAIL_ON_RESPONSE_ERRORS" == true ]] \
    && ! jq -e '.successful_requests==.selected_requests' "$cell_dir/result.json" >/dev/null; then
    failure_class=application_response_gate
    die "$cell repetition $repetition returned non-OK responses"
  fi
  oc_retry get job/$job -n "$NAMESPACE" -o json >"$cell_dir/job.json"
  oc_retry get pods -n "$NAMESPACE" -l job-name="$job" -o json >"$cell_dir/driver-pod.json"
  sleep "$METRIC_BRACKET_SECONDS"
  capture_network_distribution "$cell_dir" "$cell_dir/job.json"
  capture_active_target_pods "$cell_dir/target-pods-after.json"
  oc_retry get nodes gnr2.fm2aihpcsed.com rhgnr1 -o json >"$cell_dir/nodes-after.json"
  oc_retry get events -n "$NAMESPACE" --sort-by=.lastTimestamp -o json >"$cell_dir/events-after.json"
  python3 "$HEALTH_SUMMARIZER" \
    "$cell_dir/target-pods-before.json" "$cell_dir/target-pods-after.json" \
    "$cell_dir/events-before.json" "$cell_dir/events-after.json" \
    "$cell_dir/health-summary.json" >/dev/null
  jq -e '.identity_stable' "$cell_dir/health-summary.json" >/dev/null || {
    failure_class=target_identity_gate
    die "$cell repetition $repetition replaced a target Pod during the measurement"
  }
  jq -e '.items|length==2
    and all(.[]; any(.status.conditions[]?; .type=="Ready" and .status=="True"))' \
    "$cell_dir/nodes-after.json" >/dev/null || {
    failure_class=node_health_gate
    die "$cell repetition $repetition failed node health gates"
  }
  if [[ "$FAIL_ON_TARGET_HEALTH_ERRORS" == true ]] \
    && ! jq -e '.health_slo_pass' "$cell_dir/health-summary.json" >/dev/null; then
    failure_class=target_health_gate
    die "$cell repetition $repetition failed target health gates"
  fi
  oc_retry delete job/$job -n "$NAMESPACE" --wait=true --timeout=300s >/dev/null
}

for repetition in $(seq 1 "$REPETITIONS"); do
  offset=$(((repetition - 1) % ${#treatments[@]}))
  for index in $(seq 0 $((${#treatments[@]} - 1))); do
    cell=${treatments[$(((index + offset) % ${#treatments[@]}))]}
    run_cell "$cell" "$repetition"
  done
done

last_phase=finalizing
capture_active_target_pods "$RUN_DIR/target-pods-end.json"
python3 "$CAMPAIGN_SUMMARIZER" "$RUN_DIR" --repetitions "$REPETITIONS" \
  --treatments "${treatments[@]}" \
  --output "$RUN_DIR/transport-summary.json" >/dev/null
unset auth_token
jq -n --arg run_id "$RUN_ID" --arg completed_at "$(date -u +%FT%TZ)" \
  --argjson repetitions "$REPETITIONS" \
  '{schema_version:1,run_id:$run_id,status:"completed",completed_at:$completed_at,repetitions:$repetitions}' \
  >"$RUN_DIR/campaign-status.json"
echo "transport matrix complete: $RUN_DIR"
