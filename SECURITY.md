# Security and data handling

This benchmark targets authenticated clusters but does not require credentials
in the repository. Do not commit kubeconfigs, tokens, passwords, pull secrets,
private model artifacts, or production request content.

Use synthetic benchmark contexts only. Inspect captured logs and traces before
sharing them because cluster metadata, internal service names, and image
locations may be operationally sensitive even when they are not credentials.
