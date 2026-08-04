package org.mule.weave.lib;

import org.graalvm.nativeimage.CurrentIsolate;
import org.graalvm.nativeimage.IsolateThread;
import org.graalvm.nativeimage.c.type.CCharPointer;
import org.graalvm.nativeimage.c.type.CTypeConversion;
import org.mule.weave.v2.parser.ast.variables.NameIdentifier;
import org.mule.weave.v2.sdk.NameIdentifierHelper;
import org.mule.weave.v2.sdk.WeaveResource;
import org.mule.weave.v2.sdk.WeaveResourceResolver;
import scala.Option;

/**
 * WeaveResourceResolver implementation backed by a C function pointer callback.
 * Delegates module resolution to the host environment (Node.js, Python, etc.).
 */
public class CallbackWeaveResourceResolver implements WeaveResourceResolver {
    private final NativeCallbacks.ResolveModuleCallback callback;

    public CallbackWeaveResourceResolver(NativeCallbacks.ResolveModuleCallback callback) {
        if (callback == null) {
            throw new IllegalArgumentException("Resolver callback cannot be null");
        }
        this.callback = callback;
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
            // Log and return empty on any error
            System.err.println("Error resolving module " + path + ": " + e.getMessage());
            return Option.empty();
        }
    }

    @Override
    public scala.collection.immutable.Seq<WeaveResource> resolveAll(NameIdentifier nameIdentifier) {
        // Not used for module resolution, return single result or empty
        Option<WeaveResource> result = resolve(nameIdentifier);
        if (result.isDefined()) {
            return scala.collection.JavaConverters.asScalaBuffer(
                java.util.Collections.singletonList(result.get())
            ).toList();
        } else {
            return scala.collection.immutable.Seq$.MODULE$.empty();
        }
    }
}
