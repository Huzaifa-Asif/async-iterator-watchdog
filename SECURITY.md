# Security Policy

## Supported versions

Security fixes are provided for the latest published major version.

## Reporting a vulnerability

Please use GitHub's private vulnerability reporting feature for this repository. Do not include exploit details, credentials, private stream data, or provider payloads in a public issue.

Include affected versions, runtime, a minimal reproduction, impact, and any proposed mitigation. You should receive an acknowledgement within seven days. Disclosure timing will be coordinated after validation and a fix plan.

## Security model

The package does not open network connections, persist values, inspect credentials, or execute lifecycle scripts. It invokes the source iterator's `next()` and, on abnormal termination, may invoke its `return()` method. Both are untrusted application callbacks and may throw, reject, mutate state, or never settle.

Timeouts stop the wrapper from waiting; they do not forcibly terminate the producer. Applications remain responsible for cancelling transports and redacting data before logging errors or values.
