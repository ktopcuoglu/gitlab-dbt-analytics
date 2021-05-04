import pytest
from qualtrics.src.execute import chunk_list


def test_chunk_list():
    list_to_chunk = [n for n in range(10)]
    expected_chunks = [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9]]
    for chunked_list, expected_chunk in zip(
        chunk_list(list_to_chunk, 3), expected_chunks
    ):
        assert chunked_list == expected_chunk
