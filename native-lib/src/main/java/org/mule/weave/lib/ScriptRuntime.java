package org.mule.weave.lib;

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

    private ScriptingBindings parseJsonInputsToBindings(String inputsJson) {
        ScriptingBindings bindings = new ScriptingBindings();
        
        if (inputsJson == null || inputsJson.trim().isEmpty()) {
            return bindings;
        }
        
        try {
            String json = inputsJson.trim();

            // Parse top-level entries: "name": { ... }
            int pos = 1; // Skip opening brace
            
            while (pos < json.length()) {
                // Skip whitespace, commas
                while (pos < json.length() && (Character.isWhitespace(json.charAt(pos)) || json.charAt(pos) == ',')) {
                    pos++;
                }
                
                if (pos >= json.length() || json.charAt(pos) == '}') break;
                
                // Expect a quoted string (binding name)
                if (json.charAt(pos) != '"') break;
                
                int nameEnd = findClosingQuote(json, pos + 1);
                if (nameEnd == -1) break;
                
                String name = json.substring(pos + 1, nameEnd);
                pos = nameEnd + 1; // Move past the closing quote
                
                // Skip whitespace and colon
                while (pos < json.length() && (Character.isWhitespace(json.charAt(pos)) || json.charAt(pos) == ':')) {
                    pos++;
                }
                
                // Expect opening brace for nested object
                if (pos >= json.length() || json.charAt(pos) != '{') break;
                
                int objEnd = findClosingBrace(json, pos + 1);
                if (objEnd == -1) break;
                
                String nestedContent = json.substring(pos + 1, objEnd);
                pos = objEnd + 1;
                
                String contentRaw = extractStringValue(nestedContent, "content");
                if (contentRaw != null) {
                    String mimeTypeRaw = extractStringValue(nestedContent, "mimeType");
                    String propertiesRaw = null;
                    if (nestedContent.indexOf("\"properties\": {") != -1) {
                        propertiesRaw = nestedContent.substring(nestedContent.indexOf("\"properties\": {") + 14, nestedContent.lastIndexOf("}") + 1);
                    }
                    String charsetRaw = extractStringValue(nestedContent, "charset");

                    Map<String, Object> properties = Map$.MODULE$.empty();
                    if (propertiesRaw != null) {
                        properties = parseJsonProperties(propertiesRaw);
                    }
                    Charset charset = Charset.forName(charsetRaw != null ? charsetRaw : "UTF-8");
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

    private Map<String, Object> parseJsonProperties(String jsonProperties) {
        if (jsonProperties == null || jsonProperties.trim().isEmpty()) {
            return Map$.MODULE$.empty();
        }

        String json = jsonProperties.trim();
        if (json.charAt(0) != '{') {
            throw new IllegalArgumentException("properties must be a JSON object (must start with '{'): " + jsonProperties);
        }

        int end = findClosingBrace(json, 1);
        if (end == -1) {
            throw new IllegalArgumentException("properties must be a valid JSON object (missing closing '}'): " + jsonProperties);
        }

        // Disallow trailing non-whitespace after the object
        for (int i = end + 1; i < json.length(); i++) {
            if (!Character.isWhitespace(json.charAt(i))) {
                throw new IllegalArgumentException("properties must contain a single JSON object (unexpected trailing content): " + jsonProperties);
            }
        }

        Map<String, Object> result = Map$.MODULE$.empty();
        int pos = 1; // skip '{'

        while (pos < end) {
            while (pos < end && (Character.isWhitespace(json.charAt(pos)) || json.charAt(pos) == ',')) {
                pos++;
            }

            if (pos >= end) {
                break;
            }

            if (json.charAt(pos) != '"') {
                throw new IllegalArgumentException("properties keys must be quoted strings at position " + pos + ": " + jsonProperties);
            }

            int keyEnd = findClosingQuote(json, pos + 1);
            if (keyEnd == -1 || keyEnd > end) {
                throw new IllegalArgumentException("properties has an unterminated key string: " + jsonProperties);
            }

            String key = unescapeJsonString(json.substring(pos + 1, keyEnd));
            pos = keyEnd + 1;

            while (pos < end && Character.isWhitespace(json.charAt(pos))) {
                pos++;
            }
            if (pos >= end || json.charAt(pos) != ':') {
                throw new IllegalArgumentException("properties expected ':' after key '" + key + "': " + jsonProperties);
            }
            pos++;

            while (pos < end && Character.isWhitespace(json.charAt(pos))) {
                pos++;
            }
            if (pos >= end) {
                throw new IllegalArgumentException("properties missing value for key '" + key + "': " + jsonProperties);
            }

            Object value;
            char c = json.charAt(pos);
            if (c == '"') {
                int valueEnd = findClosingQuote(json, pos + 1);
                if (valueEnd == -1 || valueEnd > end) {
                    throw new IllegalArgumentException("properties has an unterminated string value for key '" + key + "': " + jsonProperties);
                }
                value = unescapeJsonString(json.substring(pos + 1, valueEnd));
                pos = valueEnd + 1;
            } else if (c == 't' || c == 'f') {
                if (json.startsWith("true", pos)) {
                    value = Boolean.TRUE;
                    pos += 4;
                } else if (json.startsWith("false", pos)) {
                    value = Boolean.FALSE;
                    pos += 5;
                } else {
                    throw new IllegalArgumentException("properties invalid boolean value for key '" + key + "' at position " + pos + ": " + jsonProperties);
                }
            } else if (c == 'n') {
                throw new IllegalArgumentException("properties values cannot be null (key '" + key + "'): " + jsonProperties);
            } else if (c == '{' || c == '[') {
                throw new IllegalArgumentException("properties values must be primitive (string/number/boolean) (key '" + key + "'): " + jsonProperties);
            } else {
                int numEnd = pos;
                while (numEnd < end) {
                    char nc = json.charAt(numEnd);
                    if (nc == ',' || nc == '}' || Character.isWhitespace(nc)) {
                        break;
                    }
                    numEnd++;
                }
                String numStr = json.substring(pos, numEnd);
                if (numStr.isEmpty()) {
                    throw new IllegalArgumentException("properties invalid number value for key '" + key + "' at position " + pos + ": " + jsonProperties);
                }
                try {
                    if (numStr.indexOf('.') >= 0 || numStr.indexOf('e') >= 0 || numStr.indexOf('E') >= 0) {
                        value = Double.parseDouble(numStr);
                    } else {
                        value = Long.parseLong(numStr);
                    }
                } catch (NumberFormatException nfe) {
                    throw new IllegalArgumentException("properties invalid number value for key '" + key + "': " + numStr, nfe);
                }
                pos = numEnd;
            }

            result = (Map<String, Object>) result.$plus(new Tuple2<>(key, value));
        }

        return result;
    }

    /**
     * Parse a JSON string value starting at position (which should be at the opening quote).
     * Returns the unescaped string content (without quotes).
     */
    private String parseString(String json, int startPos) {
        if (json.charAt(startPos) != '"') return null;
        
        int endPos = findClosingQuote(json, startPos + 1);
        if (endPos == -1) return null;
        
        String escaped = json.substring(startPos + 1, endPos);
        return unescapeJsonString(escaped);
    }
    
    /**
     * Extract a string value by key from a JSON object content.
     * Simplified version assuming all values are strings.
     */
    private String extractStringValue(String json, String key) {
        String searchKey = "\"" + key + "\"";
        int keyPos = json.indexOf(searchKey);
        if (keyPos == -1) return null;
        
        // Find the colon after the key
        int colonPos = keyPos + searchKey.length();
        while (colonPos < json.length() && json.charAt(colonPos) != ':') {
            colonPos++;
        }
        if (colonPos >= json.length()) return null;
        
        // Skip whitespace after colon
        int valueStart = colonPos + 1;
        while (valueStart < json.length() && Character.isWhitespace(json.charAt(valueStart))) {
            valueStart++;
        }
        
        if (valueStart >= json.length() || json.charAt(valueStart) != '"') return null;
        
        return parseString(json, valueStart);
    }
    
    /**
     * Find the closing quote, skipping escaped quotes.
     * Properly handles escaped backslashes.
     */
    private int findClosingQuote(String str, int startPos) {
        for (int i = startPos; i < str.length(); i++) {
            char c = str.charAt(i);
            
            if (c == '\\' && i + 1 < str.length()) {
                // Skip the escaped character
                i++;
            } else if (c == '"') {
                // Found unescaped quote
                return i;
            }
        }
        return -1;
    }
    
    /**
     * Find the closing brace, properly handling nested braces and strings.
     */
    private int findClosingBrace(String str, int startPos) {
        int depth = 1;
        boolean inString = false;
        
        for (int i = startPos; i < str.length(); i++) {
            char c = str.charAt(i);
            
            if (inString) {
                if (c == '\\' && i + 1 < str.length()) {
                    i++; // Skip escaped character
                } else if (c == '"') {
                    inString = false;
                }
            } else {
                if (c == '"') {
                    inString = true;
                } else if (c == '{') {
                    depth++;
                } else if (c == '}') {
                    depth--;
                    if (depth == 0) {
                        return i;
                    }
                }
            }
        }
        return -1;
    }
    
    /**
     * Unescapes JSON string escape sequences.
     * Order matters: handle \\\\ first to avoid conflicts with other escape sequences.
     */
    private String unescapeJsonString(String input) {
        if (input == null) {
            return null;
        }
        // Use a placeholder for escaped backslashes to avoid conflicts
        String placeholder = "\u0000"; // null character as temporary placeholder
        return input
            .replace("\\\\", placeholder)
            .replace("\\n", "\n")
            .replace("\\r", "\r")
            .replace("\\t", "\t")
            .replace("\\\"", "\"")
            .replace(placeholder, "\\");
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
