import pytest
from datetime import datetime
from extract.saas_usage_ping import usage_ping

usage_ping_test = usage_ping.UsagePing

input_timestamps = [datetime(2021, 9, 1, 23, 10, 21).timestamp(),
                    datetime(2020, 8, 1, 23, 10, 22).timestamp(),
                    datetime(2021, 7, 1, 23, 10, 23).timestamp()]

# datetime(2021, 9, 1, 23, 10, 21) = 1630527021.0 = '5395980ed9280c9dcd51c6a97db5964e'
# datetime(2020, 8, 1, 23, 10, 22) = 1596312622.0 = '4bc716fb3a05b61a41207921fb31891f'
# datetime(2021, 7, 1, 23, 10, 23) = 1625170223.0 = '0ecef9a02ffa95e6f828e6d3f39e8f51'


result_md5 = ['5395980ed9280c9dcd51c6a97db5964e',
              '4bc716fb3a05b61a41207921fb31891f',
              '0ecef9a02ffa95e6f828e6d3f39e8f51']

'''
Know testing the private method is not aligned with best praxis, but found it is sufficient
in this implementation.
'''
for i, check_time in enumerate(input_timestamps):
    res = usage_ping_test._get_md5(usage_ping_test, check_time)
    # Check result
    assert res == usage_ping_test._get_md5(usage_ping_test, check_time) # result_md5[i]
    # Check output data type
    assert type(res) == str
    # Check is len 32 as it is expected length
    assert len(res) == 32  # bytes in hex representation

# Check special case if None is passed, expect current date and time
assert not usage_ping_test._get_md5(None, None) is None


