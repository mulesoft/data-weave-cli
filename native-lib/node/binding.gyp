{
  "targets": [
    {
      "target_name": "dwlib_addon",
      "sources": ["src/addon.c"],
      "defines": ["NAPI_VERSION=8"],
      "conditions": [
        ["OS=='mac'", {
          "xcode_settings": {
            "OTHER_CFLAGS": ["-std=c11"]
          }
        }],
        ["OS=='linux'", {
          "cflags": ["-std=gnu11"]
        }]
      ]
    }
  ]
}
