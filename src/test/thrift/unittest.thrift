/* Simple thrift message used in Secor unit testing */

namespace java com.pinterest.secor.thrift

enum UnitTestEnum {
    SOME_VALUE = 0,
    SOME_OTHER_VALUE = 1,
}

struct UnitTestMessage {
    1: required i64 timestamp,
    2: required string requiredField,
    3: required i32 timestampTwo,
    4: optional string optionalField,
    5: optional UnitTestEnum enumField,
    6: required i64 timestampThree
}

