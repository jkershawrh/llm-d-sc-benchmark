#!/usr/bin/env bash
set -euo pipefail

RESULT_DIR=${1:?usage: arena-sc-capture-thanos.sh RESULT_DIR}
KUBECONFIG_PATH=${KUBECONFIG_PATH:-/tmp/llm-d-sc-arena-kubeconfig}

cell=${RESULT_DIR}/cell.json
targets=${RESULT_DIR}/targets-before.json
test -s "$cell"
test -s "$targets"

namespace=$(jq -r .namespace "$cell")
start_epoch_ms=$(jq -r .start_epoch_ms "$cell")
duration_seconds=$(jq -r .duration_seconds "$cell")
start=$((start_epoch_ms / 1000 - 30))
end=$((start_epoch_ms / 1000 + duration_seconds + 30))
pod_regex=$(jq -r '[.items[].metadata.name] | join("|")' "$targets")

k=(oc --kubeconfig "$KUBECONFIG_PATH")
prom_host=$("${k[@]}" -n openshift-monitoring get route thanos-querier -o jsonpath='{.spec.host}')
auth_token=$("${k[@]}" whoami -t)
mkdir -p "$RESULT_DIR/metrics"

query_range() {
  local name=$1 query=$2
  curl -ksS --get -H "Authorization: Bearer ${auth_token}" \
    --data-urlencode "query=${query}" \
    --data-urlencode "start=${start}" \
    --data-urlencode "end=${end}" \
    --data-urlencode 'step=5' \
    "https://${prom_host}/api/v1/query_range" \
    >"$RESULT_DIR/metrics/${name}.json"
  jq -e '.status == "success"' "$RESULT_DIR/metrics/${name}.json" >/dev/null
}

query_range pod_cpu_otel \
  "k8s_pod_cpu_usage{k8s_namespace_name=\"${namespace}\",k8s_pod_name=~\"${pod_regex}\"}"
query_range container_cpu_otel \
  "container_cpu_usage{k8s_namespace_name=\"${namespace}\",k8s_pod_name=~\"${pod_regex}\",k8s_container_name=\"llm-d-sc\"}"
query_range container_cpu_cadvisor \
  "sum by (pod)(rate(container_cpu_usage_seconds_total{namespace=\"${namespace}\",pod=~\"${pod_regex}\",container=\"llm-d-sc\"}[30s]))"
query_range throttle_ratio \
  "sum by (pod)(rate(container_cpu_cfs_throttled_periods_total{namespace=\"${namespace}\",pod=~\"${pod_regex}\",container=\"llm-d-sc\"}[30s])) / sum by (pod)(rate(container_cpu_cfs_periods_total{namespace=\"${namespace}\",pod=~\"${pod_regex}\",container=\"llm-d-sc\"}[30s]))"
query_range memory_working_set \
  "container_memory_working_set_bytes{namespace=\"${namespace}\",pod=~\"${pod_regex}\",container=\"llm-d-sc\"}"
query_range cpu_pressure_waiting \
  "rate(container_pressure_cpu_waiting_seconds_total{namespace=\"${namespace}\",pod=~\"${pod_regex}\",container=\"llm-d-sc\"}[30s])"
query_range restarts \
  "kube_pod_container_status_restarts_total{namespace=\"${namespace}\",pod=~\"${pod_regex}\",container=\"llm-d-sc\"}"
query_range pod_ready \
  "kube_pod_status_ready{namespace=\"${namespace}\",pod=~\"${pod_regex}\",condition=\"true\"}"
query_range node_ready \
  'kube_node_status_condition{condition="Ready",status="true",node=~"gnr2.fm2aihpcsed.com|rhgnr1"}'

unset auth_token

jq -n \
  --argjson plateau_start "$((start_epoch_ms / 1000))" \
  --argjson plateau_end "$((start_epoch_ms / 1000 + duration_seconds))" \
  --slurpfile cpu "$RESULT_DIR/metrics/pod_cpu_otel.json" \
  --slurpfile cadvisor "$RESULT_DIR/metrics/container_cpu_cadvisor.json" \
  --slurpfile throttle "$RESULT_DIR/metrics/throttle_ratio.json" \
  --slurpfile ready "$RESULT_DIR/metrics/pod_ready.json" \
  --slurpfile nodes "$RESULT_DIR/metrics/node_ready.json" \
  'def plateau: map(select(.[0] >= $plateau_start and .[0] <= $plateau_end));
   {plateau_start:$plateau_start,plateau_end:$plateau_end,
    pod_cpu_otel:[ $cpu[0].data.result[]? | (.values|plateau) as $v | select($v|length>0) | {pod:.metric.k8s_pod_name,samples:($v|length),min:($v|map(.[1]|tonumber)|min),max:($v|map(.[1]|tonumber)|max),mean:($v|map(.[1]|tonumber)|add/length)} ],
    pod_cpu_cadvisor:[ $cadvisor[0].data.result[]? | (.values|plateau) as $v | select($v|length>0) | {pod:.metric.pod,samples:($v|length),min:($v|map(.[1]|tonumber)|min),max:($v|map(.[1]|tonumber)|max),mean:($v|map(.[1]|tonumber)|add/length)} ],
    throttle_ratio:[ $throttle[0].data.result[]? | (.values|plateau) as $v | select($v|length>0) | {pod:.metric.pod,samples:($v|length),max:($v|map(.[1]|tonumber)|max),mean:($v|map(.[1]|tonumber)|add/length)} ],
    pod_ready_min:[ $ready[0].data.result[]? | (.values|plateau) as $v | select($v|length>0) | {pod:.metric.pod,min:($v|map(.[1]|tonumber)|min)} ],
    node_ready_min:[ $nodes[0].data.result[]? | (.values|plateau) as $v | select($v|length>0) | {node:.metric.node,min:($v|map(.[1]|tonumber)|min)} ]}' \
  >"$RESULT_DIR/metrics-summary.json"

cat "$RESULT_DIR/metrics-summary.json"
