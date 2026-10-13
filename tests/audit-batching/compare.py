"""Compare same-workload evidence, without treating projections as savings."""
import json
import pathlib
import sys

root = pathlib.Path(sys.argv[1])


def read(name):
    records = {}
    for line in (root / (name + '.log')).read_text().splitlines():
        if line.startswith('AUDIT_PERFORMANCE_RESULT '):
            record = json.loads(line.split(' ', 1)[1])
            records[record['scenario']] = record
    assert set(records) == {'steady', 'burst', 'sustained'}, records
    return records


baseline, candidate = read('baseline'), read('candidate')
comparison = {}
for name in baseline:
    before, after = baseline[name], candidate[name]
    assert before['requests'] == after['requests'] and before['events'] == after['events']
    assert before['object_uploads'] > 0 and after['object_uploads'] > 0
    comparison[name] = {
        'baseline': before,
        'candidate': after,
        'new_part_reduction': 1 - after['new_parts'] / before['new_parts'],
        'object_upload_reduction': 1 - after['object_uploads'] / before['object_uploads'],
    }
(root / 'comparison.json').write_text(json.dumps(comparison, indent=2) + '\n')
print(json.dumps(comparison, indent=2))
# Record performance and cost proxies even when a target is missed. The rollout
# review must consider the measured result, not replace it with a passing ratio.
