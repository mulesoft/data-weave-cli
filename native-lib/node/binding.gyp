{
  "targets": [
    {
      "target_name": "dwlib_addon",
      "sources": ["src/addon.c"],
      "include_dirs": [
        "<!@(node -p \"require('node-addon-api').include\")"
      ],
      "defines": ["NAPI_VERSION=8"],
      "cflags!": ["-fno-exceptions"],
      "cflags_cc!": ["-fno-exceptions"],
      "conditions": [
        ["OS=='mac'", {
          "xcode_settings": {
            "OTHER_CFLAGS": ["-std=c11"]
          }
        }],
        ["OS=='linux'", {
          "cflags": ["-std=c11"]
        }],
        ["OS=='win'", {
          "msvs_settings": {
            "VCCLCompilerTool": {
              "AdditionalOptions": ["/std:c11"]
            }
          }
        }]
      ]
    }
  ]
}
