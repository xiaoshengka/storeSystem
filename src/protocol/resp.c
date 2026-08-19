#include "protocol/resp.h"

#include <limits.h>
#include <stdio.h>
#include <string.h>

static int incomplete_or_error(size_t available,
                               int end_of_stream,
                               size_t max_frame_size)
{
    return end_of_stream || available >= max_frame_size
               ? RESP_PARSE_ERROR
               : RESP_PARSE_INCOMPLETE;
}

static int parse_decimal_line(const unsigned char *data,
                              size_t length,
                              size_t *position,
                              size_t *value,
                              int end_of_stream,
                              size_t max_frame_size)
{
    size_t cursor = *position;
    size_t parsed = 0;
    size_t digits = 0;

    while (cursor < length && data[cursor] != '\r') {
        unsigned int digit;

        if (data[cursor] < '0' || data[cursor] > '9') {
            return RESP_PARSE_ERROR;
        }
        digit = (unsigned int)(data[cursor] - '0');
        if (parsed > (SIZE_MAX - digit) / 10U) {
            return RESP_PARSE_ERROR;
        }
        parsed = parsed * 10U + digit;
        digits++;
        cursor++;
        if (cursor > max_frame_size) {
            return RESP_PARSE_ERROR;
        }
    }
    if (cursor >= length) {
        return incomplete_or_error(length, end_of_stream, max_frame_size);
    }
    if (digits == 0 || cursor + 1U >= length) {
        return digits == 0 ? RESP_PARSE_ERROR
                           : incomplete_or_error(length,
                                                 end_of_stream,
                                                 max_frame_size);
    }
    if (data[cursor + 1U] != '\n') {
        return RESP_PARSE_ERROR;
    }
    *position = cursor + 2U;
    *value = parsed;
    return RESP_PARSE_COMPLETE;
}

int resp_parse_request(const unsigned char *data,
                       size_t length,
                       int end_of_stream,
                       resp_request_t *request,
                       size_t *consumed)
{
    return resp_parse_request_with_limit(data,
                                         length,
                                         end_of_stream,
                                         RESP_MAX_FRAME_SIZE,
                                         request,
                                         consumed);
}

int resp_parse_request_with_limit(const unsigned char *data,
                                  size_t length,
                                  int end_of_stream,
                                  size_t max_frame_size,
                                  resp_request_t *request,
                                  size_t *consumed)
{
    size_t argument_count;
    size_t position = 0;
    size_t index;
    int result;

    if (request == NULL || consumed == NULL || max_frame_size == 0 ||
        (data == NULL && length != 0)) {
        return RESP_PARSE_ERROR;
    }
    request->argument_count = 0;
    *consumed = 0;
    if (length == 0) {
        return end_of_stream ? RESP_PARSE_ERROR : RESP_PARSE_INCOMPLETE;
    }
    if (data[position++] != '*') {
        return RESP_PARSE_ERROR;
    }
    result = parse_decimal_line(data,
                                length,
                                &position,
                                &argument_count,
                                end_of_stream,
                                max_frame_size);
    if (result != RESP_PARSE_COMPLETE) {
        return result;
    }
    if (argument_count == 0 || argument_count > RESP_MAX_ARGUMENTS) {
        return RESP_PARSE_ERROR;
    }

    for (index = 0; index < argument_count; ++index) {
        size_t bulk_length;

        if (position >= length) {
            return incomplete_or_error(length,
                                       end_of_stream,
                                       max_frame_size);
        }
        if (data[position++] != '$') {
            return RESP_PARSE_ERROR;
        }
        result = parse_decimal_line(data,
                                    length,
                                    &position,
                                    &bulk_length,
                                    end_of_stream,
                                    max_frame_size);
        if (result != RESP_PARSE_COMPLETE) {
            return result;
        }
        if (bulk_length > max_frame_size ||
            position > max_frame_size - bulk_length) {
            return RESP_PARSE_ERROR;
        }
        if (length - position < bulk_length) {
            return incomplete_or_error(length,
                                       end_of_stream,
                                       max_frame_size);
        }
        request->arguments[index].data = data + position;
        request->arguments[index].length = bulk_length;
        position += bulk_length;
        if (position + 2U > length) {
            return incomplete_or_error(length,
                                       end_of_stream,
                                       max_frame_size);
        }
        if (position + 2U > max_frame_size || data[position] != '\r' ||
            data[position + 1U] != '\n') {
            return RESP_PARSE_ERROR;
        }
        position += 2U;
    }

    request->argument_count = argument_count;
    *consumed = position;
    return RESP_PARSE_COMPLETE;
}

static int encode_line(unsigned char prefix,
                       unsigned char *output,
                       size_t capacity,
                       const char *text,
                       size_t *output_length)
{
    size_t text_length;

    if (output == NULL || text == NULL || output_length == NULL) {
        return -1;
    }
    *output_length = 0;
    text_length = strlen(text);
    if (text_length > capacity || capacity - text_length < 3U) {
        return -1;
    }
    output[0] = prefix;
    memcpy(output + 1U, text, text_length);
    output[text_length + 1U] = '\r';
    output[text_length + 2U] = '\n';
    *output_length = text_length + 3U;
    return 0;
}

int resp_encode_simple_string(unsigned char *output,
                              size_t capacity,
                              const char *text,
                              size_t *output_length)
{
    return encode_line('+', output, capacity, text, output_length);
}

int resp_encode_error(unsigned char *output,
                      size_t capacity,
                      const char *text,
                      size_t *output_length)
{
    return encode_line('-', output, capacity, text, output_length);
}

int resp_encode_integer(unsigned char *output,
                        size_t capacity,
                        int64_t value,
                        size_t *output_length)
{
    int written;

    if (output == NULL || output_length == NULL || capacity == 0) {
        return -1;
    }
    *output_length = 0;
    written = snprintf((char *)output, capacity, ":%lld\r\n", (long long)value);
    if (written < 0 || (size_t)written >= capacity) {
        return -1;
    }
    *output_length = (size_t)written;
    return 0;
}

int resp_encode_bulk_string(unsigned char *output,
                            size_t capacity,
                            const unsigned char *data,
                            size_t length,
                            size_t *output_length)
{
    int header_length;
    size_t total;

    if (output == NULL || output_length == NULL || (data == NULL && length != 0)) {
        return -1;
    }
    *output_length = 0;
    header_length = snprintf((char *)output, capacity, "$%zu\r\n", length);
    if (header_length < 0 || (size_t)header_length >= capacity) {
        return -1;
    }
    if (length > SIZE_MAX - (size_t)header_length - 2U) {
        return -1;
    }
    total = (size_t)header_length + length + 2U;
    if (total > capacity) {
        return -1;
    }
    if (length > 0) {
        memcpy(output + (size_t)header_length, data, length);
    }
    output[(size_t)header_length + length] = '\r';
    output[(size_t)header_length + length + 1U] = '\n';
    *output_length = total;
    return 0;
}

int resp_encode_null_bulk_string(unsigned char *output,
                                 size_t capacity,
                                 size_t *output_length)
{
    static const unsigned char null_bulk[] = "$-1\r\n";

    if (output == NULL || output_length == NULL || capacity < sizeof(null_bulk) - 1U) {
        return -1;
    }
    memcpy(output, null_bulk, sizeof(null_bulk) - 1U);
    *output_length = sizeof(null_bulk) - 1U;
    return 0;
}
