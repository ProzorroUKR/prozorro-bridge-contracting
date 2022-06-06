from prozorro_bridge_contracting.settings import JOURNAL_PREFIX


def journal_context(record: dict = None, params: dict = None) -> dict:
    if record is None:
        record = {}
    if params is None:
        params = {}
    for k, v in params.items():
        record[JOURNAL_PREFIX + k] = v
    return record
