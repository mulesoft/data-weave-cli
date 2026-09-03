package org.mule.weave.lib;

import org.graalvm.nativeimage.CurrentIsolate;
import org.graalvm.nativeimage.c.type.CCharPointer;
import org.graalvm.nativeimage.c.type.CTypeConversion;
import org.graalvm.word.PointerBase;
import org.mule.weave.v2.parser.ast.variables.NameIdentifier;
import org.mule.weave.v2.sdk.NameIdentifierHelper;
import org.mule.weave.v2.sdk.WeaveResource;
import org.mule.weave.v2.sdk.WeaveResourceResolver;
import scala.Option;
import scala.collection.JavaConverters;
import scala.collection.immutable.Seq;
import scala.collection.immutable.Seq$;

import java.util.Collections;

/**
 * WeaveResourceResolver implementation backed by a C function pointer callback.
 * Delegates module resolution to the host environment (Node.js, Python, etc.).
 */
public class CallbackWeaveResourceResolver implements WeaveResourceResolver {
    private final NativeCallbacks.ResolveModuleCallback callback;
    private final PointerBase ctx;

    public CallbackWeaveResourceResolver(NativeCallbacks.ResolveModuleCallback callback, PointerBase ctx) {
        if (callback.isNull()) {
            throw new IllegalArgumentException("Resolver callback cannot be null");
        }
        this.callback = callback;
        this.ctx = ctx;
    }

    @Override
    public Option<WeaveResource> resolve(NameIdentifier nameIdentifier) {
        // Convert NameIdentifier to file path (outside try block for error logging)
        String path = NameIdentifierHelper.toWeaveFilePath(nameIdentifier, "/");

        try {
            // Convert path to C string
            try (CTypeConversion.CCharPointerHolder pathHolder =
                    CTypeConversion.toCString(path)) {
                CCharPointer pathPtr = pathHolder.get();

                // Invoke callback (blocks if threadsafe function is in use)
                CCharPointer resultPtr = callback.invoke(
                    CurrentIsolate.getCurrentThread(),
                    ctx,
                    pathPtr
                );

                // Null means "not found"
                if (resultPtr.isNull()) {
                    return Option.empty();
                }

                // Copy result to Java string immediately (host will free pointer)
                String source = CTypeConversion.toJavaString(resultPtr);

                // Return as WeaveResource
                return Option.apply(
                    WeaveResource.apply(path, source)
                );
            }
        } catch (Exception e) {
            // Log and return empty on any error. Mirrors the C-side resolver bridge's
            // policy (see resolve_module_callback in addon.c): both the exception
            // message AND the module path are resolver-controlled/dynamic content
            // (module source, file paths, credentials can leak through either), so
            // the default log line is fully static/content-free, with no path and no
            // message. Only include them when the caller has opted in via
            // DATAWEAVE_RESOLVER_DEBUG=1.
            if ("1".equals(System.getenv("DATAWEAVE_RESOLVER_DEBUG"))) {
                System.err.println("Error resolving module " + path + ": " + e.getMessage());
            } else {
                System.err.println(
                    "Error resolving module (details suppressed; set "
                    + "DATAWEAVE_RESOLVER_DEBUG=1 to log path/message — may expose "
                    + "resolver-controlled data).");
            }
            return Option.empty();
        }
    }

    @Override
    public Seq<WeaveResource> resolveAll(NameIdentifier nameIdentifier) {
        // Not used for module resolution, return single result or empty
        Option<WeaveResource> result = resolve(nameIdentifier);
        if (result.isDefined()) {
            return JavaConverters.asScalaBuffer(Collections.singletonList(result.get()))
                    .toList();
        } else {
            return (Seq<WeaveResource>) Seq$.MODULE$.<WeaveResource>empty();
        }
    }
}
