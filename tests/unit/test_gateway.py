from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from payments.gateway import Gateway


async def test_gateway_stable_outcome(monkeypatch: pytest.MonkeyPatch) -> None:
    sleep = AsyncMock()
    monkeypatch.setattr("payments.gateway.sleep", sleep)
    payment_id = UUID("f086a337-f537-4eab-843e-a53d8e64552e")
    outcomes = [await Gateway().process(payment_id) for _ in range(20)]
    assert set(outcomes) <= {"succeeded", "failed"}
    assert len(set(outcomes)) == 1
    assert sleep.await_count == 20
    assert all(2 <= call.args[0] <= 5 for call in sleep.await_args_list)


async def test_gateway_hash_distributes_results_independently_of_delay(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("payments.gateway.sleep", AsyncMock())
    outcomes = [await Gateway().process(UUID(int=index)) for index in range(10_000)]
    assert 850 <= outcomes.count("failed") <= 1150
    assert 8850 <= outcomes.count("succeeded") <= 9150


@pytest.mark.parametrize("delay", [2.0, 5.0])
async def test_gateway_delay_is_separate_from_outcome(
    monkeypatch: pytest.MonkeyPatch, delay: float
) -> None:
    sleep = AsyncMock()
    monkeypatch.setattr("payments.gateway.sleep", sleep)
    monkeypatch.setattr("payments.gateway.uniform", lambda low, high: delay)
    # These IDs exercise both business outcomes, irrespective of the random delay.
    outcomes = [await Gateway().process(UUID(int=index)) for index in range(100)]
    assert set(outcomes) == {"succeeded", "failed"}
    assert all(call.args == (delay,) for call in sleep.await_args_list)
