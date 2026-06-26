#ifndef CALLBACKS_H
#define CALLBACKS_H

// Forward declarations for wrapper functions
int writeCallbackWrapper(void* ctx, const char* buffer, int length);
int readCallbackWrapper(void* ctx, char* buffer, int bufferSize);

#endif // CALLBACKS_H
