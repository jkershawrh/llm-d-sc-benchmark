//! Opt-in OTLP tracing for the instrumented benchmark candidate.

use std::io;

use opentelemetry::global;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::trace::{Sampler, SdkTracerProvider};
use opentelemetry_sdk::Resource;

/// Keeps the exporter runtime and provider alive for the process lifetime.
pub struct Guard {
    _runtime: tokio::runtime::Runtime,
    provider: SdkTracerProvider,
    pub sample_ratio: f64,
}

impl Drop for Guard {
    fn drop(&mut self) {
        let _ = self.provider.shutdown();
    }
}

/// Initialize tracing when `LLM_D_SC_TRACE_SAMPLE_RATIO` is greater than zero.
pub fn init_from_env() -> io::Result<Option<Guard>> {
    let ratio = std::env::var("LLM_D_SC_TRACE_SAMPLE_RATIO")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .unwrap_or(0.0);
    if !(0.0..=1.0).contains(&ratio) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "LLM_D_SC_TRACE_SAMPLE_RATIO must be between 0 and 1",
        ));
    }
    if ratio == 0.0 {
        return Ok(None);
    }

    let endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
        .unwrap_or_else(|_| "http://127.0.0.1:4317".to_string());
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(io::Error::other)?;
    let exporter = runtime
        .block_on(async {
            opentelemetry_otlp::SpanExporter::builder()
                .with_tonic()
                .with_endpoint(endpoint)
                .build()
        })
        .map_err(io::Error::other)?;
    let provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_sampler(Sampler::TraceIdRatioBased(ratio))
        .with_resource(Resource::builder().with_service_name("llm-d-sc").build())
        .build();
    global::set_tracer_provider(provider.clone());
    Ok(Some(Guard {
        _runtime: runtime,
        provider,
        sample_ratio: ratio,
    }))
}

#[cfg(test)]
mod tests {
    #[test]
    fn u136_trace_ratio_validation_is_bounded() {
        for valid in [0.0, 0.01, 0.1, 1.0] {
            assert!((0.0..=1.0).contains(&valid));
        }
        for invalid in [-0.1, 1.1] {
            assert!(!(0.0..=1.0).contains(&invalid));
        }
    }
}
