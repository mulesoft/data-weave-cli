package org.mule.weave.lib;

import org.json.JSONObject;
import org.mule.weave.v2.runtime.BindingValue;
import org.mule.weave.v2.runtime.DataWeaveResult;
import org.mule.weave.v2.runtime.ScriptingBindings;
import org.mule.weave.v2.runtime.api.DWResult;
import org.mule.weave.v2.runtime.api.DWScript;
import org.mule.weave.v2.runtime.api.DWScriptingEngine;
import scala.Option;
import scala.Tuple2;
import scala.collection.immutable.Map;
import scala.collection.immutable.Map$;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Base64;

/**
 * Singleton wrapper around a {@link DWScriptingEngine} used to compile and execute DataWeave scripts.
 *
 * <p>Execution results are returned as a JSON string containing a base64-encoded payload plus metadata
 * (mime type, charset, and whether the result is binary). Errors are returned as a JSON string with
 * {@code success=false} and an escaped error message.</p>
 */
public class ScriptRuntime {

    private static final ScriptRuntime INSTANCE = new ScriptRuntime();

    /**
     * Returns the singleton instance.
     *
     * @return the shared {@link ScriptRuntime}
     */
    public static ScriptRuntime getInstance() {
        return INSTANCE;
    }

    private DWScriptingEngine engine;

    private ScriptRuntime() {
        engine = DWScriptingEngine.builder().build();
    }

    /**
     * Executes a DataWeave script with no input bindings.
     *
     * @param script the DataWeave script source
     * @return a JSON string describing either the successful result or an error
     */
    public String run(String script) {
        return run(script, null);
    }

    /**
     * Executes a DataWeave script with optional input bindings encoded as JSON.
     *
     * <p>The expected JSON structure maps binding names to an object containing {@code content}
     * (base64), {@code mimeType}, optional {@code charset}, and optional {@code properties}.</p>
     *
     * @param script the DataWeave script source
     * @param inputsJson JSON string encoding the input bindings map, or {@code null}
     * @return a JSON string describing either the successful result or an error
     */
    public String run(String script, String inputsJson) {
        ScriptingBindings bindings = parseJsonInputsToBindings(inputsJson);
        String[] inputs = bindings.bindingNames();

        try {
            DWScript compiled = engine.compileDWScript(script, inputs);
            DWResult dwResult = compiled.writeDWResult(bindings);

            String encodedResult;
            if (dwResult.getContent() instanceof InputStream) {
                try {
                    byte[] ba = ((InputStream) dwResult.getContent()).readAllBytes();
                    encodedResult = Base64.getEncoder().encodeToString(ba);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            } else {
                throw new RuntimeException("Result is not an InputStream: " + dwResult.getContent().getClass().getName());
            }

            return "{"
                + "\"success\":true,"
                + "\"result\":\"" + encodedResult + "\","
                + "\"mimeType\":\"" + dwResult.getMimeType() + "\","
                + "\"charset\":\"" + dwResult.getCharset() + "\","
                + "\"binary\":" + ((DataWeaveResult) dwResult).isBinary()
                + "}";
        } catch (Exception e) {
            String message = e.getMessage();
            if (message == null || message.trim().isEmpty()) {
                message = e.toString();
            }

            return "{"
                + "\"success\":false,"
                + "\"error\":\"" + escapeJsonString(message) + "\""
                + "}";
        }
    }

    /**
     * Executes a DataWeave script and returns a {@link StreamSession} whose {@link java.io.InputStream}
     * can be read incrementally, avoiding loading the entire result into memory.
     *
     * @param script    the DataWeave script source
     * @param inputsJson JSON string encoding the input bindings map, or {@code null}
     * @return a {@link StreamSession} with the result stream and metadata, or an error session
     */
    public StreamSession runStreaming(String script, String inputsJson) {
        ScriptingBindings bindings = parseJsonInputsToBindings(inputsJson);
        String[] inputs = bindings.bindingNames();

        try {
            DWScript compiled = engine.compileDWScript(script, inputs);
            DWResult dwResult = compiled.writeDWResult(bindings);

            if (dwResult.getContent() instanceof InputStream) {
                return new StreamSession(
                        (InputStream) dwResult.getContent(),
                        dwResult.getMimeType(),
                        dwResult.getCharset().name(),
                        ((DataWeaveResult) dwResult).isBinary()
                );
            } else {
                return StreamSession.ofError("Result is not an InputStream: " + dwResult.getContent().getClass().getName());
            }
        } catch (Exception e) {
            String message = e.getMessage();
            if (message == null || message.trim().isEmpty()) {
                message = e.toString();
            }
            return StreamSession.ofError(message);
        }
    }

    private ScriptingBindings parseJsonInputsToBindings(String inputsJson) {
        ScriptingBindings bindings = new ScriptingBindings();

        if (inputsJson == null || inputsJson.trim().isEmpty()) {
            return bindings;
        }

        try {
            JSONObject root = new JSONObject(inputsJson);

            for (String name : root.keySet()) {
                JSONObject entry = root.getJSONObject(name);

                if (entry.has("streamHandle")) {
                    long streamHandle = Long.parseLong(entry.getString("streamHandle"));
                    InputStreamSession inputSession = InputStreamSession.get(streamHandle);
                    if (inputSession == null) {
                        throw new RuntimeException("Invalid streamHandle " + streamHandle + " for input '" + name + "'");
                    }
                    String mimeTypeRaw = entry.optString("mimeType", inputSession.getMimeType());
                    String charsetRaw = entry.optString("charset", inputSession.getCharset());
                    Charset charset = Charset.forName(charsetRaw);
                    Option<String> mimeType = Option.apply(mimeTypeRaw);

                    BindingValue bindingValue = new BindingValue(inputSession.getInputStream(), mimeType, Map$.MODULE$.empty(), charset);
                    bindings.addBinding(name, bindingValue);

                } else if (entry.has("content")) {
                    String contentRaw = entry.getString("content");
                    String mimeTypeRaw = entry.optString("mimeType", null);
                    String charsetRaw = entry.optString("charset", "UTF-8");

                    Map<String, Object> properties = Map$.MODULE$.empty();
                    if (entry.has("properties") && !entry.isNull("properties")) {
                        JSONObject propsObj = entry.getJSONObject("properties");
                        properties = parseJsonProperties(propsObj);
                    }

                    Charset charset = Charset.forName(charsetRaw);
                    Option<String> mimeType = Option.apply(mimeTypeRaw);

                    byte[] content = Base64.getDecoder().decode(contentRaw);
                    BindingValue bindingValue = new BindingValue(content, mimeType, properties, charset);
                    bindings.addBinding(name, bindingValue);
                }
            }
        } catch (Exception e) {
            System.err.println("Error parsing JSON inputs: " + e.getMessage());
            e.printStackTrace();
        }

        return bindings;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> parseJsonProperties(JSONObject propsObj) {
        Map<String, Object> result = Map$.MODULE$.empty();

        for (String key : propsObj.keySet()) {
            Object val = propsObj.get(key);
            if (val instanceof String || val instanceof Boolean) {
                result = (Map<String, Object>) result.$plus(new Tuple2<>(key, val));
            } else if (val instanceof Number) {
                Number num = (Number) val;
                Object boxed;
                if (val instanceof Double || val instanceof Float) {
                    boxed = num.doubleValue();
                } else {
                    boxed = num.longValue();
                }
                result = (Map<String, Object>) result.$plus(new Tuple2<>(key, boxed));
            } else if (val == JSONObject.NULL) {
                throw new IllegalArgumentException("properties values cannot be null (key '" + key + "')");
            } else {
                throw new IllegalArgumentException("properties values must be primitive (string/number/boolean) (key '" + key + "')");
            }
        }

        return result;
    }

    private String escapeJsonString(String input) {
        if (input == null) {
            return "";
        }

        return input
            .replace("\\", "\\\\")
            .replace("\"", "\\\"")
            .replace("\n", "\\n")
            .replace("\r", "\\r")
            .replace("\t", "\\t");
    }
}
