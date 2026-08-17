#ifndef STORE_SYSTEM_PROTOCOL_RESP_H
#define STORE_SYSTEM_PROTOCOL_RESP_H

#include <stddef.h>
#include <stdint.h>

#define RESP_MAX_FRAME_SIZE (64U * 1024U)
#define RESP_MAX_ARGUMENTS 128U

typedef struct resp_slice {
    const unsigned char *data;
    size_t length;
} resp_slice_t;

typedef struct resp_request {
    size_t argument_count;
    resp_slice_t arguments[RESP_MAX_ARGUMENTS];
} resp_request_t;

enum resp_parse_result {
    RESP_PARSE_ERROR = -1,
    RESP_PARSE_INCOMPLETE = 0,
    RESP_PARSE_COMPLETE = 1
};

int resp_parse_request(const unsigned char *data,
                       size_t length,
                       int end_of_stream,
                       resp_request_t *request,
                       size_t *consumed);

int resp_encode_simple_string(unsigned char *output,
                              size_t capacity,
                              const char *text,
                              size_t *output_length);
int resp_encode_error(unsigned char *output,
                      size_t capacity,
                      const char *text,
                      size_t *output_length);
int resp_encode_integer(unsigned char *output,
                        size_t capacity,
                        int64_t value,
                        size_t *output_length);
int resp_encode_bulk_string(unsigned char *output,
                            size_t capacity,
                            const unsigned char *data,
                            size_t length,
                            size_t *output_length);
int resp_encode_null_bulk_string(unsigned char *output,
                                 size_t capacity,
                                 size_t *output_length);

#endif
