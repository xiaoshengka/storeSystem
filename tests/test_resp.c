#include "protocol/resp.h"

#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void test_incremental_and_pipeline(void)
{
    static const unsigned char first[] =
        "*3\r\n$3\r\nSET\r\n$3\r\na\000b\r\n$3\r\nx\000y\r\n";
    static const unsigned char second[] = "*1\r\n$4\r\nPING\r\n";
    unsigned char pipeline[sizeof(first) + sizeof(second) - 2U];
    resp_request_t request;
    size_t consumed;
    size_t length = sizeof(first) - 1U;
    size_t split;
    unsigned char large_pipeline[RESP_MAX_FRAME_SIZE + 16U];

    for (split = 0; split < length; ++split) {
        assert(resp_parse_request(first,
                                  split,
                                  0,
                                  &request,
                                  &consumed) == RESP_PARSE_INCOMPLETE);
        assert(consumed == 0);
    }
    assert(resp_parse_request(first,
                              length,
                              0,
                              &request,
                              &consumed) == RESP_PARSE_COMPLETE);
    assert(consumed == length);
    assert(request.argument_count == 3U);
    assert(request.arguments[1].length == 3U);
    assert(memcmp(request.arguments[1].data, "a\000b", 3U) == 0);
    assert(memcmp(request.arguments[2].data, "x\000y", 3U) == 0);

    memcpy(pipeline, first, sizeof(first) - 1U);
    memcpy(pipeline + sizeof(first) - 1U, second, sizeof(second) - 1U);
    assert(resp_parse_request(pipeline,
                              sizeof(pipeline),
                              0,
                              &request,
                              &consumed) == RESP_PARSE_COMPLETE);
    assert(consumed == sizeof(first) - 1U);

    memcpy(large_pipeline, second, sizeof(second) - 1U);
    memset(large_pipeline + sizeof(second) - 1U,
           'x',
           sizeof(large_pipeline) - (sizeof(second) - 1U));
    assert(resp_parse_request(large_pipeline,
                              sizeof(large_pipeline),
                              0,
                              &request,
                              &consumed) == RESP_PARSE_COMPLETE);
    assert(consumed == sizeof(second) - 1U);
}

static void expect_protocol_error(const unsigned char *data,
                                  size_t length,
                                  int end_of_stream)
{
    resp_request_t request;
    size_t consumed = 99U;

    assert(resp_parse_request(data,
                              length,
                              end_of_stream,
                              &request,
                              &consumed) == RESP_PARSE_ERROR);
    assert(consumed == 0);
}

static void test_protocol_errors(void)
{
    static const unsigned char inline_request[] = "PING\r\n";
    static const unsigned char null_bulk[] = "*1\r\n$-1\r\n";
    static const unsigned char nested[] = "*1\r\n*1\r\n$4\r\nPING\r\n";
    static const unsigned char bad_crlf[] = "*1\n$4\r\nPING\r\n";
    static const unsigned char null_array[] = "*-1\r\n";
    static const unsigned char empty_array[] = "*0\r\n";
    static const unsigned char too_many[] = "*129\r\n";
    static const unsigned char overflow[] = "*184467440737095516160\r\n";
    static const unsigned char oversized_bulk[] = "*1\r\n$65537\r\n";
    static const unsigned char truncated[] = "*1\r\n$4\r\nPI";

    expect_protocol_error(inline_request, sizeof(inline_request) - 1U, 0);
    expect_protocol_error(null_bulk, sizeof(null_bulk) - 1U, 0);
    expect_protocol_error(nested, sizeof(nested) - 1U, 0);
    expect_protocol_error(bad_crlf, sizeof(bad_crlf) - 1U, 0);
    expect_protocol_error(null_array, sizeof(null_array) - 1U, 0);
    expect_protocol_error(empty_array, sizeof(empty_array) - 1U, 0);
    expect_protocol_error(too_many, sizeof(too_many) - 1U, 0);
    expect_protocol_error(overflow, sizeof(overflow) - 1U, 0);
    expect_protocol_error(oversized_bulk, sizeof(oversized_bulk) - 1U, 0);
    expect_protocol_error(truncated, sizeof(truncated) - 1U, 1);
}

static void test_maximum_frame(void)
{
    static const unsigned char prefix[] = "*2\r\n$3\r\nGET\r\n$65513\r\n";
    unsigned char *frame = malloc(RESP_MAX_FRAME_SIZE);
    resp_request_t request;
    size_t consumed;
    size_t payload_length = 65513U;

    assert(frame != NULL);
    assert(sizeof(prefix) - 1U + payload_length + 2U == RESP_MAX_FRAME_SIZE);
    memcpy(frame, prefix, sizeof(prefix) - 1U);
    memset(frame + sizeof(prefix) - 1U, 'k', payload_length);
    frame[RESP_MAX_FRAME_SIZE - 2U] = '\r';
    frame[RESP_MAX_FRAME_SIZE - 1U] = '\n';
    assert(resp_parse_request(frame,
                              RESP_MAX_FRAME_SIZE,
                              0,
                              &request,
                              &consumed) == RESP_PARSE_COMPLETE);
    assert(consumed == RESP_MAX_FRAME_SIZE);
    assert(request.arguments[1].length == payload_length);
    free(frame);
}

static void test_encoders(void)
{
    unsigned char output[64];
    static const unsigned char binary[] = {'a', 0, 'b'};
    size_t length;

    assert(resp_encode_simple_string(output, sizeof(output), "OK", &length) == 0);
    assert(length == 5U && memcmp(output, "+OK\r\n", length) == 0);
    assert(resp_encode_error(output, sizeof(output), "ERR test", &length) == 0);
    assert(length == 11U && memcmp(output, "-ERR test\r\n", length) == 0);
    assert(resp_encode_integer(output, sizeof(output), -42, &length) == 0);
    assert(length == 6U && memcmp(output, ":-42\r\n", length) == 0);
    assert(resp_encode_bulk_string(output,
                                   sizeof(output),
                                   binary,
                                   sizeof(binary),
                                   &length) == 0);
    assert(length == 9U);
    assert(memcmp(output, "$3\r\na\000b\r\n", length) == 0);
    assert(resp_encode_bulk_string(output, sizeof(output), NULL, 0, &length) == 0);
    assert(length == 6U && memcmp(output, "$0\r\n\r\n", length) == 0);
    assert(resp_encode_null_bulk_string(output, sizeof(output), &length) == 0);
    assert(length == 5U && memcmp(output, "$-1\r\n", length) == 0);
}

int main(void)
{
    test_incremental_and_pipeline();
    test_protocol_errors();
    test_maximum_frame();
    test_encoders();
    puts("test_resp: PASS");
    return 0;
}
