def window_values($series):
  [$series.values[]?
   | select((.[0] | tonumber) >= $start and (.[0] | tonumber) <= $end)];

def max_gap($values):
  ([range(1; $values | length) as $i
    | (($values[$i][0] | tonumber) - ($values[$i - 1][0] | tonumber))]
   | max // 0);

def detail($doc; $name):
  [$doc.data.result[]? | select(.metric[$label] == $name)] as $series
  | (if ($series | length) == 1 then window_values($series[0]) else [] end) as $values
  | {
      name: $name,
      series: ($series | length),
      samples: ($values | length),
      first_sample_epoch:
        (if ($values | length) > 0 then ($values[0][0] | tonumber) else null end),
      last_sample_epoch:
        (if ($values | length) > 0 then ($values[-1][0] | tonumber) else null end),
      max_gap_seconds:
        (if ($values | length) > 0 then max_gap($values) else null end),
      complete:
        (($series | length) == 1
         and ($values | length) > 0
         and (($values[0][0] | tonumber) - $start) <= $max_gap
         and ($end - ($values[-1][0] | tonumber)) <= $max_gap
         and max_gap($values) <= $max_gap)
    };

($result[0] // {}) as $doc
| [$expected_names[] | detail($doc; .)] as $details
| [$doc.data.result[]?.metric[$label] // empty] as $reported_names
| ($doc.status == "success") as $query_succeeded
| (if $query_succeeded | not then "query_error"
   elif ($details | map(select(.series > 0)) | length) == 0 then "absent"
   elif all($details[]; .complete) then "complete"
   else "degraded"
   end) as $quality
| ($role == "auxiliary_attribution") as $informational
| {
    schema_version: 1,
    metric: $metric,
    role: $role,
    source: $source,
    label: $label,
    query_status: ($doc.status // "missing"),
    query_error_type: ($doc.errorType // null),
    query_error: ($doc.error // null),
    plateau_start_epoch: $start,
    plateau_end_epoch: $end,
    completeness_max_gap_seconds: $max_gap,
    expected_series: ($expected_names | length),
    observed_expected_series: ($details | map(select(.series > 0)) | length),
    reported_series: ($doc.data.result // [] | length),
    missing_names: [$details[] | select(.series == 0) | .name],
    duplicate_names: [$details[] | select(.series > 1) | .name],
    incomplete_names: [$details[] | select(.series > 0 and (.complete | not)) | .name],
    unexpected_names: [$reported_names[] | select(. as $name | $expected_names | index($name) | not)] | unique,
    series: $details,
    quality: $quality,
    validity_policy: (if $informational then "informational" else "required" end),
    coverage_gate_pass: ($informational or $quality == "complete")
  }
