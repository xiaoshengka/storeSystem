#include "service/kvstore_service.h"

#include <assert.h>
#include <stdio.h>
#include <string.h>

static void expect_response(const char *request, const char *expected)
{
    char response[512];
    size_t response_length = 0;

    (void)kvstore_execute_request(request,
                                  strlen(request),
                                  response,
                                  sizeof(response),
                                  &response_length);
    if (response_length != strlen(expected) ||
        memcmp(response, expected, response_length) != 0) {
        fprintf(stderr,
                "request '%s': expected '%s', got '%.*s'\n",
                request,
                expected,
                (int)response_length,
                response);
    }
    assert(response_length == strlen(expected));
    assert(memcmp(response, expected, response_length) == 0);
}

int main(void)
{
    char oversized[513];

    assert(kvstore_engine_init() == 0);

    expect_response("SET array-key one", "SUCCESS");
    expect_response("GET array-key", "one");
    expect_response("MOD array-key two", "SUCCESS");
    expect_response("GET array-key", "two");
    expect_response("DEL array-key", "SUCCESS");
    expect_response("COUNT", "0");

    expect_response("RSET tree-key one", "SUCCESS");
    expect_response("RGET tree-key", "one");
    expect_response("RMOD tree-key two", "SUCCESS");
    expect_response("RGET tree-key", "two");
    expect_response("RDEL tree-key", "SUCCESS");
    expect_response("RCOUNT", "0");

    expect_response("RSET tree-b two", "SUCCESS");
    expect_response("RSET tree-a one", "SUCCESS");
    expect_response("RSET tree-c three", "SUCCESS");
    expect_response("RSET tree-d four", "SUCCESS");
    expect_response("RSET tree-b duplicate", "FAILED");
    expect_response("RCOUNT", "4");
    expect_response("RDEL tree-b", "SUCCESS");
    expect_response("RGET tree-a", "one");
    expect_response("RGET tree-c", "three");
    expect_response("RDEL tree-a", "SUCCESS");
    expect_response("RDEL tree-c", "SUCCESS");
    expect_response("RDEL tree-d", "SUCCESS");
    expect_response("RCOUNT", "0");

    expect_response("HSET hash-key one", "SUCCESS");
    expect_response("HGET hash-key", "one");
    expect_response("HMOD hash-key two", "SUCCESS");
    expect_response("HGET hash-key", "two");
    expect_response("HDEL hash-key", "SUCCESS");
    expect_response("HCOUNT", "0");

    expect_response("GET", "ERROR wrong number of arguments");
    expect_response("SET only-key", "ERROR wrong number of arguments");
    expect_response("NOT_A_COMMAND", "ERROR unknown command");
    expect_response("   \r\n", "ERROR empty request");
    memset(oversized, 'x', sizeof(oversized) - 1U);
    oversized[sizeof(oversized) - 1U] = '\0';
    expect_response(oversized, "ERROR request too large");

    kvstore_engine_destroy();
    puts("test_kvstore: PASS");
    return 0;
}
