#include "_cgo_export.h"

// These wrapper functions allow Go code to get function pointers
// to the Go-exported callback functions

int writeCallbackWrapper(void* ctx, const char* buffer, int length) {
    return goWriteCallback(ctx, (char*)buffer, length);
}

int readCallbackWrapper(void* ctx, char* buffer, int bufferSize) {
    return goReadCallback(ctx, buffer, bufferSize);
}
