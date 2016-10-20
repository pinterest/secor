/* Simple thrift message used in Secor testing */

namespace java com.pinterest.secor.thrift

enum TestEnum {
    SOME_VALUE = 0,
    SOME_OTHER_VALUE = 1,
}

struct TestMessage {
    1: required i64 timestamp,
    2: required string requiredField,
    3: optional string optionalField,
    4: optional TestEnum enumField
}
