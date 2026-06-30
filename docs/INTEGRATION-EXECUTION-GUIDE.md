# DataWeave Integration Execution Guide

## Process Architecture Decision

### Should DataWeave Run in a Separate Process?

**Short Answer**: **No, use the in-process Node.js native addon** for the initial integration.

**Detailed Analysis**:

#### Option 1: In-Process (Native Addon) ✅ RECOMMENDED

```
┌─────────────────────────────────────┐
│   Node.js Process (api-platform-api)│
│                                     │
│  ┌──────────────────────────────┐  │
│  │  Express App                 │  │
│  │  ├─ Controllers              │  │
│  │  ├─ Services                 │  │
│  │  └─ DataWeave Native Addon   │  │
│  │     (N-API FFI)              │  │
│  │     └─ GraalVM Isolate       │  │
│  └──────────────────────────────┘  │
└─────────────────────────────────────┘
```

**Pros**:
- ✅ **Low latency** - No IPC overhead (~1-5ms per call)
- ✅ **Simple deployment** - Single process, no orchestration
- ✅ **Easier debugging** - Single process to attach debugger
- ✅ **Better resource sharing** - Shared memory space
- ✅ **Native error handling** - Exceptions propagate naturally
- ✅ **No serialization cost** - Direct memory access to data
- ✅ **Proven stability** - N-API is stable across Node versions

**Cons**:
- ⚠️ **Crash risk** - Native crash can kill entire process (mitigated by GraalVM isolate)
- ⚠️ **Memory isolation** - Limited (but GraalVM provides isolate-level isolation)
- ⚠️ **Can't scale independently** - Tied to Node.js process scaling

#### Option 2: Separate Process (Worker Pool)

```
┌──────────────────────┐         ┌──────────────────────┐
│  Node.js Process     │  IPC    │  DataWeave Worker 1  │
│  (api-platform-api)  │◄───────►│  (dwlib process)     │
│                      │         └──────────────────────┘
│  ├─ Controllers      │         ┌──────────────────────┐
│  ├─ Services         │  IPC    │  DataWeave Worker 2  │
│  └─ DW Client        │◄───────►│  (dwlib process)     │
│                      │         └──────────────────────┘
└──────────────────────┘         ┌──────────────────────┐
                          IPC    │  DataWeave Worker N  │
                         ◄───────►│  (dwlib process)     │
                                 └──────────────────────┘
```

**Pros**:
- ✅ **Crash isolation** - Worker crash doesn't kill main process
- ✅ **Independent scaling** - Scale workers separately
- ✅ **Resource limits** - Can enforce per-worker CPU/memory limits
- ✅ **Hot reload** - Can restart workers without app downtime

**Cons**:
- ❌ **Higher latency** - IPC overhead (~10-50ms per call)
- ❌ **Complex deployment** - Process orchestration, health checks
- ❌ **Serialization cost** - JSON encode/decode on every call
- ❌ **More moving parts** - Worker pool management, queue handling
- ❌ **Harder debugging** - Multi-process debugging required
- ❌ **Resource overhead** - Multiple processes = more memory

#### Option 3: Separate Microservice

```
┌──────────────────────┐    HTTP   ┌──────────────────────┐
│  api-platform-api    │◄─────────►│  dataweave-service   │
│                      │           │  (separate deployment)│
└──────────────────────┘           └──────────────────────┘
```

**Pros**:
- ✅ **Complete isolation** - Total failure isolation
- ✅ **Independent deployment** - Can deploy/scale separately
- ✅ **Language agnostic** - Can use any language for service

**Cons**:
- ❌ **Very high latency** - Network overhead (~50-200ms)
- ❌ **Operational complexity** - Service discovery, load balancing
- ❌ **Distributed failures** - Network partitions, timeouts
- ❌ **Cost** - Separate infrastructure

### Recommendation: Start with In-Process (Option 1)

**Why**:
1. **GraalVM provides isolate-level isolation** - Crashes in DataWeave isolate don't kill Node.js
2. **Performance is critical** - api-platform-api has strict SLAs (p95 < 200ms)
3. **Simpler operations** - No process orchestration needed
4. **Lower risk** - Fewer moving parts means fewer failure modes
5. **Easy to migrate** - Can move to separate process later if needed

**Migration Path (if needed later)**:
```
Phase 1: In-process addon (now)
         ↓
Phase 2: If scaling issues → Worker pool
         ↓
Phase 3: If isolation issues → Microservice
```

---

## Execution Guide

### Phase 0: Pre-Flight Checks (Day 1-2)

#### Step 1: Verify Build Environment

```bash
# 1. Clone both repositories
cd ~/repos/emu
git clone https://github.com/mulesoft/api-platform-api.git
cd data-weave-cli
git checkout feat/harden-native-bindings-production

# 2. Test native library build in Kilonova environment
docker run --rm \
  -v $(pwd):/workspace \
  -w /workspace \
  artifacts.msap.io/mulesoft/kilonova-node-builder:22-1.0.0 \
  bash -c "./gradlew :native-lib:nativeCompile && ls -lh native-lib/build/native/nativeCompile/"

# Expected output: dwlib.so (Linux), dwlib.dylib (macOS)
```

**Success Criteria**: Native library builds without errors.

#### Step 2: Test Node.js Addon Build

```bash
# 1. Build Node.js package
docker run --rm \
  -v $(pwd):/workspace \
  -w /workspace \
  artifacts.msap.io/mulesoft/kilonova-node-builder:22-1.0.0 \
  bash -c "cd /workspace/native-lib/node && npm install && npx node-gyp rebuild"

# 2. Test addon loads
docker run --rm \
  -v $(pwd):/workspace \
  -w /workspace/native-lib/node \
  artifacts.msap.io/mulesoft/kilonova-node-builder:22-1.0.0 \
  node -e "const dw = require('./dist'); console.log('✅ Addon loaded'); dw.run('output json --- {test: true}');"
```

**Success Criteria**: Addon loads and runs a simple transformation.

#### Step 3: Memory Baseline Test

```bash
# Run continuous load test for 1 hour
cd native-lib/node
node --expose-gc memory-test.js

# memory-test.js
const dw = require('./dist');

let iteration = 0;
const startMem = process.memoryUsage();

setInterval(() => {
  // Run 100 transformations
  for (let i = 0; i < 100; i++) {
    const result = dw.run(`
      output json
      ---
      { iteration: ${iteration}, data: (1 to 100) map $ }
    `);
  }
  
  // Force GC and check memory
  if (global.gc) global.gc();
  const currentMem = process.memoryUsage();
  const heapDiff = (currentMem.heapUsed - startMem.heapUsed) / 1024 / 1024;
  
  console.log(`[${iteration}] Heap growth: ${heapDiff.toFixed(2)} MB`);
  
  iteration++;
}, 1000);
```

**Success Criteria**: Heap growth < 10MB/hour (indicates no memory leak).

#### Step 4: Create Feature Flag

```bash
# In LaunchDarkly console (https://app.launchdarkly.com)
# Create flag: dataweave.enabled
# Type: Boolean
# Default: false
# Environments:
#   - kdev: false
#   - kqa: false
#   - kstg: false
#   - kprod: false
#   - kprod-eu: false
```

---

### Phase 1: Infrastructure Integration (Day 3-5)

#### Step 1: Create Feature Branch

```bash
cd ~/repos/emu/api-platform-api
git checkout master
git pull origin master
git checkout -b feat/dataweave-native-integration
```

#### Step 2: Add Package Dependency

```bash
# Option A: Install from npm (once published)
npm install @dataweave/native@1.0.0

# Option B: Install from local tarball (for testing)
cd ~/repos/emu/data-weave-cli
./gradlew :native-lib:buildNodePackage
cp native-lib/node/dataweave-native-1.0.0.tgz ~/repos/emu/api-platform-api/
cd ~/repos/emu/api-platform-api
npm install ./dataweave-native-1.0.0.tgz
```

**Verify**:
```bash
# Should succeed without errors
node -e "require('@dataweave/native')"
```

#### Step 3: Create Service Wrapper

```bash
# Create the service file
cat > api/data/services/dataweaveTransformService.js << 'EOF'
/**
 * DataWeave transformation service with feature flag control.
 * @module dataweaveTransformService
 */

const logger = require('winston');

/**
 * @typedef {Object} DataWeaveTransformOptions
 * @property {number} [timeout=5000] - Timeout in milliseconds
 * @property {string} [featureFlag='dataweave.enabled'] - Feature flag name
 */

/**
 * @typedef {Object} DataWeaveTransformResult
 * @property {boolean} success - Whether transformation succeeded
 * @property {*} [result] - Transformation result (if success)
 * @property {string} [error] - Error message (if failed)
 */

/**
 * Create DataWeave transformation service.
 * @param {Object} config - Application configuration
 * @param {Object} featureFlagService - LaunchDarkly service
 * @returns {Object} DataWeave service interface
 */
function dataweaveTransformService(config, featureFlagService) {
  let dataweave = null;
  let initialized = false;
  let initError = null;

  /**
   * Lazy initialization of DataWeave runtime.
   * @private
   */
  function ensureInitialized() {
    if (initialized) return;
    
    try {
      // Require only when needed to avoid startup failures
      dataweave = require('@dataweave/native');
      dataweave.initialize();
      initialized = true;
      logger.info('[DataWeave] Runtime initialized successfully');
    } catch (error) {
      initError = error;
      logger.error('[DataWeave] Failed to initialize runtime', {
        error: error.message,
        stack: error.stack
      });
      throw new Error(`DataWeave initialization failed: ${error.message}`);
    }
  }

  /**
   * Transform data using a DataWeave script.
   * 
   * @param {string} script - DataWeave transformation script
   * @param {Object} inputs - Input data (e.g., { payload: {...} })
   * @param {DataWeaveTransformOptions} [options={}] - Transformation options
   * @returns {Promise<DataWeaveTransformResult>} Transformation result
   * 
   * @example
   * const result = await transform(
   *   'output json --- { name: payload.firstName }',
   *   { payload: { firstName: 'John' } }
   * );
   * // result: { success: true, result: { name: 'John' } }
   */
  async function transform(script, inputs = {}, options = {}) {
    const {
      timeout = 5000,
      featureFlag = 'dataweave.enabled'
    } = options;

    // Check feature flag first (fast path for disabled)
    const enabled = await featureFlagService.isEnabled(featureFlag);
    if (!enabled) {
      return {
        success: false,
        error: 'DataWeave transformations are currently disabled'
      };
    }

    // Validate inputs
    if (typeof script !== 'string' || !script.trim()) {
      return {
        success: false,
        error: 'Invalid script: must be a non-empty string'
      };
    }

    // Check size limits
    const scriptSize = Buffer.byteLength(script, 'utf8');
    const inputSize = Buffer.byteLength(JSON.stringify(inputs), 'utf8');
    
    if (scriptSize > 10 * 1024) { // 10KB
      return {
        success: false,
        error: 'Script too large (max 10KB)'
      };
    }
    
    if (inputSize > 1 * 1024 * 1024) { // 1MB
      return {
        success: false,
        error: 'Input too large (max 1MB)'
      };
    }

    try {
      ensureInitialized();
    } catch (error) {
      return {
        success: false,
        error: `Initialization error: ${error.message}`
      };
    }

    // Execute with timeout
    const startTime = Date.now();
    
    try {
      const result = await Promise.race([
        new Promise((resolve, reject) => {
          try {
            const dwResult = dataweave.run(script, inputs);
            
            if (dwResult.success) {
              const output = JSON.parse(dwResult.getString());
              resolve({ success: true, result: output });
            } else {
              reject(new Error(`DataWeave error: ${dwResult.error}`));
            }
          } catch (err) {
            reject(err);
          }
        }),
        new Promise((_, reject) =>
          setTimeout(
            () => reject(new Error('Transformation timeout')),
            timeout
          )
        )
      ]);

      const duration = Date.now() - startTime;
      
      logger.info('[DataWeave] Transformation succeeded', {
        durationMs: duration,
        scriptSize,
        inputSize
      });

      return result;
      
    } catch (error) {
      const duration = Date.now() - startTime;
      
      logger.warn('[DataWeave] Transformation failed', {
        error: error.message,
        durationMs: duration,
        scriptSize,
        inputSize
      });

      return {
        success: false,
        error: error.message
      };
    }
  }

  /**
   * Health check for DataWeave runtime.
   * @returns {Promise<boolean>} True if healthy
   */
  async function healthCheck() {
    try {
      if (!initialized) {
        ensureInitialized();
      }

      const result = dataweave.run(
        'output application/json --- {status: "ok", timestamp: now()}',
        {}
      );

      return result.success;
    } catch (error) {
      logger.error('[DataWeave] Health check failed', {
        error: error.message
      });
      return false;
    }
  }

  /**
   * Get runtime status information.
   * @returns {Object} Status information
   */
  function getStatus() {
    return {
      initialized,
      initError: initError ? initError.message : null,
      healthy: initialized && !initError
    };
  }

  return {
    transform,
    healthCheck,
    getStatus
  };
}

module.exports = dataweaveTransformService;
EOF
```

#### Step 4: Add Type Definitions

```bash
# Create type definitions
cat > types/dataweave.js << 'EOF'
/**
 * DataWeave service type definitions.
 * @module types/dataweave
 */

/**
 * Options for DataWeave transformation.
 * @typedef {Object} DataWeaveTransformOptions
 * @property {number} [timeout=5000] - Timeout in milliseconds
 * @property {string} [featureFlag='dataweave.enabled'] - Feature flag name
 */

/**
 * Result of a DataWeave transformation.
 * @typedef {Object} DataWeaveTransformResult
 * @property {boolean} success - Whether transformation succeeded
 * @property {*} [result] - Transformation result (if success=true)
 * @property {string} [error] - Error message (if success=false)
 */

/**
 * DataWeave transformation service interface.
 * @typedef {Object} DataWeaveTransformService
 * @property {(script: string, inputs: Object, options?: DataWeaveTransformOptions) => Promise<DataWeaveTransformResult>} transform
 * @property {() => Promise<boolean>} healthCheck
 * @property {() => Object} getStatus
 */

module.exports = {};
EOF
```

#### Step 5: Create Unit Tests

```bash
# Create test file
cat > test/api/unit/dataweaveTransformService.test.js << 'EOF'
const { expect } = require('chai');
const sinon = require('sinon');
const proxyquire = require('proxyquire');

describe('dataweaveTransformService', () => {
  let service;
  let mockDataweave;
  let mockFeatureFlagService;
  let mockLogger;

  beforeEach(() => {
    mockDataweave = {
      initialize: sinon.stub(),
      run: sinon.stub()
    };

    mockFeatureFlagService = {
      isEnabled: sinon.stub().resolves(true)
    };

    mockLogger = {
      info: sinon.stub(),
      warn: sinon.stub(),
      error: sinon.stub()
    };

    const DataweaveTransformService = proxyquire(
      '../../../api/data/services/dataweaveTransformService',
      {
        '@dataweave/native': mockDataweave,
        'winston': mockLogger
      }
    );

    service = DataweaveTransformService({}, mockFeatureFlagService);
  });

  describe('#transform', () => {
    it('should check feature flag before transformation', async () => {
      mockFeatureFlagService.isEnabled.resolves(false);

      const result = await service.transform('output json --- {}', {});

      expect(result.success).to.be.false;
      expect(result.error).to.include('disabled');
      expect(mockDataweave.initialize).to.not.have.been.called;
    });

    it('should initialize DataWeave on first call', async () => {
      mockDataweave.run.returns({
        success: true,
        getString: () => '{"result": true}'
      });

      await service.transform('output json --- {result: true}', {});

      expect(mockDataweave.initialize).to.have.been.calledOnce;
    });

    it('should not re-initialize on subsequent calls', async () => {
      mockDataweave.run.returns({
        success: true,
        getString: () => '{"result": true}'
      });

      await service.transform('output json --- {}', {});
      await service.transform('output json --- {}', {});

      expect(mockDataweave.initialize).to.have.been.calledOnce;
    });

    it('should transform successfully', async () => {
      mockDataweave.run.returns({
        success: true,
        getString: () => JSON.stringify({ transformed: true })
      });

      const result = await service.transform(
        'output json --- {transformed: true}',
        { payload: { data: 123 } }
      );

      expect(result.success).to.be.true;
      expect(result.result).to.deep.equal({ transformed: true });
    });

    it('should handle DataWeave errors', async () => {
      mockDataweave.run.returns({
        success: false,
        error: 'Parse error at line 1'
      });

      const result = await service.transform('invalid script', {});

      expect(result.success).to.be.false;
      expect(result.error).to.include('Parse error');
    });

    it('should timeout long-running transformations', async () => {
      mockDataweave.run.callsFake(() => {
        return new Promise(() => {}); // Never resolves
      });

      const result = await service.transform(
        'output json --- {}',
        {},
        { timeout: 100 }
      );

      expect(result.success).to.be.false;
      expect(result.error).to.include('timeout');
    });

    it('should reject scripts larger than 10KB', async () => {
      const largeScript = 'output json --- ' + 'x'.repeat(11 * 1024);

      const result = await service.transform(largeScript, {});

      expect(result.success).to.be.false;
      expect(result.error).to.include('too large');
    });

    it('should reject inputs larger than 1MB', async () => {
      const largeInput = { data: 'x'.repeat(2 * 1024 * 1024) };

      const result = await service.transform(
        'output json --- {}',
        largeInput
      );

      expect(result.success).to.be.false;
      expect(result.error).to.include('too large');
    });

    it('should handle initialization failures gracefully', async () => {
      mockDataweave.initialize.throws(new Error('Native module load failed'));

      const result = await service.transform('output json --- {}', {});

      expect(result.success).to.be.false;
      expect(result.error).to.include('Initialization error');
    });
  });

  describe('#healthCheck', () => {
    it('should return true when runtime is healthy', async () => {
      mockDataweave.run.returns({ success: true });

      const healthy = await service.healthCheck();

      expect(healthy).to.be.true;
    });

    it('should return false on initialization failure', async () => {
      mockDataweave.initialize.throws(new Error('Load failed'));

      const healthy = await service.healthCheck();

      expect(healthy).to.be.false;
    });

    it('should return false when runtime fails', async () => {
      mockDataweave.run.returns({ success: false });

      const healthy = await service.healthCheck();

      expect(healthy).to.be.false;
    });
  });

  describe('#getStatus', () => {
    it('should return uninitialized status initially', () => {
      const status = service.getStatus();

      expect(status.initialized).to.be.false;
      expect(status.healthy).to.be.false;
    });

    it('should return initialized status after first call', async () => {
      mockDataweave.run.returns({
        success: true,
        getString: () => '{}'
      });

      await service.transform('output json --- {}', {});
      const status = service.getStatus();

      expect(status.initialized).to.be.true;
      expect(status.healthy).to.be.true;
    });
  });
});
EOF
```

#### Step 6: Run Tests Locally

```bash
# Run unit tests
npm run test-unit -- test/api/unit/dataweaveTransformService.test.js

# Expected output: All tests passing
```

#### Step 7: Update Dockerfile

```bash
# Verify Dockerfile.local builds with new dependency
docker build -f Dockerfile.local -t api-platform-api:test .

# Test container starts
docker run --rm api-platform-api:test node -e "console.log('Testing DataWeave...'); require('@dataweave/native');"
```

#### Step 8: Commit and Push

```bash
git add package.json package-lock.json \
  api/data/services/dataweaveTransformService.js \
  types/dataweave.js \
  test/api/unit/dataweaveTransformService.test.js

git commit -m "feat: add DataWeave native transformation service

- Add @dataweave/native dependency
- Create dataweaveTransformService with feature flag control
- Add comprehensive unit tests (100% coverage)
- Add type definitions for TypeScript checking
- Lazy initialization to avoid startup failures
- Size limits and timeout protection"

git push origin feat/dataweave-native-integration
```

#### Step 9: Create Pull Request

```bash
# Use GitHub CLI or web interface
gh pr create \
  --title "feat: Add DataWeave native transformation service (Phase 1)" \
  --body "$(cat << 'PRBODY'
## Summary
Infrastructure integration for DataWeave native bindings. This PR adds the service wrapper and tests but does not use it in runtime code yet.

## Changes
- ✅ Added @dataweave/native dependency
- ✅ Created dataweaveTransformService with feature flag control
- ✅ Added unit tests (100% coverage)
- ✅ Added type definitions
- ✅ Docker build verified

## Safety
- No runtime code changes (service not called yet)
- Feature flag controlled (disabled by default)
- Lazy initialization (won't block startup if native module fails)
- Size limits and timeout protection

## Testing
- [x] Unit tests passing locally
- [x] Docker build successful
- [x] Type checking passing
- [ ] CI tests passing (pending)

## Rollout Plan
This is Phase 1 of 3:
1. **Phase 1 (this PR)**: Infrastructure only, no runtime usage
2. Phase 2: Internal endpoint for pilot testing
3. Phase 3: Production rollout with gradual traffic ramp

See: docs/INTEGRATION-PLAN-API-PLATFORM.md

## Checklist
- [x] Tests added with 100% coverage
- [x] Type definitions added
- [x] Docker build verified
- [x] No production code changes
- [x] Feature flag configured
PRBODY
)" \
  --base master
```

---

### Phase 2: Internal Pilot (Day 6-10)

#### Step 1: Create Internal Endpoint

```bash
# Create controller
cat > api/controllers/internal/dataweaveTestController.js << 'EOF'
/**
 * Internal testing controller for DataWeave transformations.
 * NOT exposed to customers - internal/support use only.
 * @module controllers/internal/dataweaveTestController
 */

const logger = require('winston');

/**
 * @param {import('../../data/services/dataweaveTransformService')} dataweaveTransformService
 */
function dataweaveTestController(dataweaveTransformService) {
  
  /**
   * Test a DataWeave transformation.
   * POST /internal/v1/dataweave/test
   * @param {import('express').Request} req
   * @param {import('express').Response} res
   * @param {import('express').NextFunction} next
   */
  async function test(req, res, next) {
    try {
      const { script, inputs, timeout } = req.body;
      
      logger.info('[DataWeaveTest] Transformation requested', {
        userId: req.user?.id,
        orgId: req.organization?.id,
        scriptLength: script?.length,
        inputSize: JSON.stringify(inputs || {}).length
      });
      
      const result = await dataweaveTransformService.transform(
        script,
        inputs || {},
        {
          timeout: timeout || 5000,
          featureFlag: 'dataweave.internal-test.enabled'
        }
      );
      
      if (result.success) {
        logger.info('[DataWeaveTest] Transformation succeeded');
        res.json({
          success: true,
          result: result.result
        });
      } else {
        logger.warn('[DataWeaveTest] Transformation failed', {
          error: result.error
        });
        res.status(400).json({
          success: false,
          error: result.error
        });
      }
    } catch (error) {
      logger.error('[DataWeaveTest] Unexpected error', {
        error: error.message,
        stack: error.stack
      });
      next(error);
    }
  }
  
  /**
   * Health check for DataWeave runtime.
   * GET /internal/v1/dataweave/health
   * @param {import('express').Request} req
   * @param {import('express').Response} res
   */
  async function health(req, res) {
    const healthy = await dataweaveTransformService.healthCheck();
    const status = dataweaveTransformService.getStatus();
    
    res.status(healthy ? 200 : 503).json({
      healthy,
      ...status
    });
  }
  
  return { test, health };
}

module.exports = dataweaveTestController;
EOF

# Add route to internalV1Router
cat >> api/routers/internalV1Router.js << 'EOF'

// DataWeave testing endpoints (internal only)
router.post('/dataweave/test',
  authenticationMiddleware.internalOnly,
  dataweaveTestController.test
);

router.get('/dataweave/health',
  authenticationMiddleware.internalOnly,
  dataweaveTestController.health
);
EOF
```

#### Step 2: Add HTTP Integration Tests

```bash
cat > test/api/http/dataweaveTestController.test.js << 'EOF'
const request = require('supertest');
const { expect } = require('chai');

describe('POST /internal/v1/dataweave/test', () => {
  let app;
  let authToken;
  
  before(async () => {
    // Setup test app with seeded data
    app = require('../../support/testApp');
    authToken = await testHelpers.getInternalAuthToken();
    
    // Enable feature flag for tests
    await featureFlagService.enable('dataweave.internal-test.enabled');
  });
  
  after(async () => {
    await featureFlagService.disable('dataweave.internal-test.enabled');
  });
  
  it('should transform simple JSON', async () => {
    const response = await request(app)
      .post('/internal/v1/dataweave/test')
      .set('Authorization', `Bearer ${authToken}`)
      .send({
        script: `
          %dw 2.0
          output application/json
          ---
          {
            result: "success",
            input: payload.value
          }
        `,
        inputs: {
          payload: { value: 123 }
        }
      })
      .expect(200);
    
    expect(response.body.success).to.be.true;
    expect(response.body.result).to.deep.equal({
      result: 'success',
      input: 123
    });
  });
  
  it('should handle invalid scripts', async () => {
    const response = await request(app)
      .post('/internal/v1/dataweave/test')
      .set('Authorization', `Bearer ${authToken}`)
      .send({
        script: 'invalid script',
        inputs: {}
      })
      .expect(400);
    
    expect(response.body.success).to.be.false;
    expect(response.body.error).to.exist;
  });
  
  it('should respect timeout limits', async () => {
    const response = await request(app)
      .post('/internal/v1/dataweave/test')
      .set('Authorization', `Bearer ${authToken}`)
      .send({
        script: `
          %dw 2.0
          output application/json
          ---
          (1 to 1000000) map { id: $ }
        `,
        inputs: {},
        timeout: 100
      })
      .expect(400);
    
    expect(response.body.error).to.include('timeout');
  });
  
  it('should require authentication', async () => {
    await request(app)
      .post('/internal/v1/dataweave/test')
      .send({ script: '', inputs: {} })
      .expect(401);
  });
});

describe('GET /internal/v1/dataweave/health', () => {
  let app;
  let authToken;
  
  before(async () => {
    app = require('../../support/testApp');
    authToken = await testHelpers.getInternalAuthToken();
  });
  
  it('should return health status', async () => {
    const response = await request(app)
      .get('/internal/v1/dataweave/health')
      .set('Authorization', `Bearer ${authToken}`)
      .expect(200);
    
    expect(response.body).to.have.property('healthy');
    expect(response.body).to.have.property('initialized');
  });
});
EOF
```

#### Step 3: Deploy to kdev

```bash
# Merge PR to master
# CI automatically deploys to kdev

# Monitor deployment
cpc status --product api-manager --component api --env kdev

# Check health endpoint
curl -H "Authorization: Bearer $INTERNAL_TOKEN" \
  https://api-manager-api.kdev.msap.io/internal/v1/dataweave/health
```

#### Step 4: Manual Testing in kdev

```bash
# Test transformation
curl -X POST \
  -H "Authorization: Bearer $INTERNAL_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "script": "%dw 2.0\noutput application/json\n---\n{ test: payload.value * 2 }",
    "inputs": { "payload": { "value": 21 } }
  }' \
  https://api-manager-api.kdev.msap.io/internal/v1/dataweave/test

# Expected: {"success": true, "result": {"test": 42}}
```

#### Step 5: Load Testing

```bash
# Use existing load test framework or k6
cat > load-test-dataweave.js << 'EOF'
import http from 'k6/http';
import { check, sleep } from 'k6';

export let options = {
  stages: [
    { duration: '2m', target: 10 },  // Ramp up
    { duration: '5m', target: 100 }, // Sustained load
    { duration: '2m', target: 0 },   // Ramp down
  ],
  thresholds: {
    http_req_duration: ['p(95)<500'], // 95% < 500ms
    http_req_failed: ['rate<0.01'],   // Error rate < 1%
  },
};

export default function() {
  const payload = {
    script: `
      %dw 2.0
      output application/json
      ---
      {
        timestamp: now(),
        data: payload.items map { id: $.id, value: $.value * 2 }
      }
    `,
    inputs: {
      payload: {
        items: [
          { id: 1, value: 10 },
          { id: 2, value: 20 },
          { id: 3, value: 30 }
        ]
      }
    }
  };
  
  const res = http.post(
    'https://api-manager-api.kdev.msap.io/internal/v1/dataweave/test',
    JSON.stringify(payload),
    {
      headers: {
        'Authorization': `Bearer ${__ENV.AUTH_TOKEN}`,
        'Content-Type': 'application/json',
      },
    }
  );
  
  check(res, {
    'status is 200': (r) => r.status === 200,
    'response time < 500ms': (r) => r.timings.duration < 500,
    'has result': (r) => JSON.parse(r.body).success === true,
  });
  
  sleep(1);
}
EOF

# Run load test
k6 run load-test-dataweave.js
```

#### Step 6: Monitor for 1 Week

```bash
# Check metrics daily
# - Error rate
# - Latency (p50, p95, p99)
# - Memory usage
# - CPU usage
```

---

### Phase 3: Production Rollout (Day 11-25)

#### Step 1: Enable for 10% of Organizations (Day 11)

```bash
# In LaunchDarkly console
# Update flag: dataweave.internal-test.enabled
# Environment: kprod
# Rollout: 10% of organizations
```

#### Step 2: Monitor for 3 Days

Watch for:
- Error rate spikes
- Latency increases
- Memory leaks
- Customer complaints

#### Step 3: Increase to 50% (Day 14)

```bash
# If no issues, increase to 50%
# LaunchDarkly: Update rollout to 50%
```

#### Step 4: Monitor for 3 More Days

#### Step 5: Enable for 100% (Day 17)

```bash
# If no issues, enable for all
# LaunchDarkly: Update rollout to 100%
```

#### Step 6: Remove Feature Flag (Day 24)

```bash
# After 1 week stable at 100%
# Remove feature flag checks from code
# Clean up flag in LaunchDarkly
```

---

## Success Metrics Tracking

Create a dashboard to track:

```javascript
// Metrics to log
{
  "dataweave.transform.count": 1,
  "dataweave.transform.success": true,
  "dataweave.transform.duration_ms": 45,
  "dataweave.transform.script_size_bytes": 256,
  "dataweave.transform.input_size_bytes": 512,
  "dataweave.transform.output_size_bytes": 1024,
  "dataweave.memory.heap_used_mb": 150
}
```

---

## Rollback Procedures

### Emergency Rollback (< 1 minute)

```bash
# Disable feature flag immediately
# Via LaunchDarkly console or API
curl -X PATCH \
  -H "Authorization: Bearer $LD_API_KEY" \
  -d '{"on": false}' \
  https://app.launchdarkly.com/api/v2/flags/default/dataweave.internal-test.enabled
```

### Code Rollback (< 5 minutes)

```bash
# Revert the integration PR
git revert <commit-hash>
git push origin master

# Or roll back to previous deployment
cpc rollback --product api-manager --component api --env kprod
```

---

## Next Steps After Successful Integration

1. **Expand Use Cases**
   - Policy template rendering
   - CSV export transformations
   - Email template rendering

2. **Optimize Performance**
   - Cache compiled scripts
   - Connection pooling
   - Parallel execution

3. **Consider Process Isolation**
   - If memory usage becomes problematic
   - If scaling independently is needed
   - Migrate to worker pool architecture

---

## Appendix: Quick Reference Commands

```bash
# Build native library
./gradlew :native-lib:nativeCompile

# Build Node.js package
./gradlew :native-lib:buildNodePackage

# Run unit tests
npm run test-unit -- test/api/unit/dataweaveTransformService.test.js

# Run HTTP tests
npm run test-http -- test/api/http/dataweaveTestController.test.js

# Check health
curl https://api-manager-api.kdev.msap.io/internal/v1/dataweave/health

# Test transformation
curl -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"script": "...", "inputs": {...}}' \
  https://api-manager-api.kdev.msap.io/internal/v1/dataweave/test

# Monitor deployment
cpc status --product api-manager --component api --env kdev

# Check logs
cpc logs --product api-manager --component api --env kdev --follow
```

---

**Last Updated**: 2026-06-30  
**Version**: 1.0
