from batch_params import enable_calls, electrum_calls
from adex_calls import batch_request

batch_request("http://127.0.0.1:7783", "testuser", enable_calls)
batch_request("http://127.0.0.1:7783", "testuser", electrum_calls)
