"""Exercise host-budget guards without touching the host cgroup hierarchy."""
import fcntl
import importlib.machinery
import importlib.util
import os
import pathlib
import tempfile
import unittest
from unittest.mock import patch

loader = importlib.machinery.SourceFileLoader("budget", str(pathlib.Path(__file__).with_name("ctld-resource-cgroup-budget")))
spec = importlib.util.spec_from_loader(loader.name, loader)
budget = importlib.util.module_from_spec(spec)
loader.exec_module(budget)


class PhysicalBudgetTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = pathlib.Path(self.directory.name) / "cgroup"
        self.root.mkdir(mode=0o700)
        self.primary = self.root.parent / "primary.lock"
        self.runtime = {"resource_cpu_millicores": 14000, "resource_memory_bytes": 56 << 30,
                        "admission_cpu_millicores": 35000, "admission_memory_bytes": 70 << 30}
        self.limits = budget.requested_limits(self.runtime, 16, 64 << 30)
        for name, value in {"cpu.max": "max 100000", "memory.max": "max", "memory.swap.max": "max", "cgroup.procs": ""}.items():
            (self.root / name).write_text(value)

    def apply(self):
        budget.apply_limits(self.root, self.limits, os.geteuid(), self.primary)

    def test_physical_capacity_is_independent_of_overcommitted_admission(self):
        self.apply()
        self.assertEqual((self.root / "cpu.max").read_text(), "1400000 100000")
        self.assertEqual((self.root / "memory.max").read_text(), str(56 << 30))
        self.assertEqual((self.root / "memory.swap.max").read_text(), "0")

    def test_retained_empty_lease_child_prevents_budget_change(self):
        (self.root / "s0-retained").mkdir()
        with self.assertRaisesRegex(ValueError, "drained, empty"):
            self.apply()
        self.assertEqual((self.root / "memory.max").read_text(), "max")

    def test_processes_prevent_budget_change(self):
        (self.root / "cgroup.procs").write_text("123\n")
        with self.assertRaisesRegex(ValueError, "drained, empty"):
            self.apply()

    def test_idempotent_ab_restart_preserves_an_occupied_subtree(self):
        self.apply()
        (self.root / "s0-active").mkdir()
        with budget.private_lock(self.primary, os.geteuid()) as primary:
            fcntl.flock(primary.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.apply()
        self.assertTrue((self.root / "s0-active").is_dir())

    def test_primary_ownership_closes_the_empty_check_claim_race(self):
        with budget.private_lock(self.primary, os.geteuid()) as primary:
            fcntl.flock(primary.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            with self.assertRaisesRegex(ValueError, "both ctld instances stopped"):
                self.apply()
        self.assertEqual((self.root / "cpu.max").read_text(), "max 100000")

    def test_rejects_malformed_and_impossible_physical_capacity(self):
        for value in [True, 0, -1, 1.5, "missing", "${MISSING_BUDGET}", 17000]:
            with self.subTest(value=value), self.assertRaises(ValueError):
                budget.requested_limits({**self.runtime, "resource_cpu_millicores": value}, 16, 64 << 30)
        with self.assertRaisesRegex(ValueError, "exceeds the host"):
            budget.requested_limits(self.runtime, 16, 32 << 30)
        with patch.dict(os.environ, {"TEST_PHYSICAL_CPU": "14000"}):
            self.assertEqual(budget.requested_limits({**self.runtime, "resource_cpu_millicores": "${TEST_PHYSICAL_CPU}"}, 16, 64 << 30), self.limits)

    def test_rejects_symlinked_limit_and_primary_lock(self):
        target = self.root / "memory.max"
        target.unlink()
        target.symlink_to(self.root / "cgroup.procs")
        with self.assertRaisesRegex(ValueError, "regular cgroup"):
            self.apply()
        target.unlink()
        target.write_text("max")
        self.primary.symlink_to(self.root / "cgroup.procs")
        with self.assertRaises(OSError):
            self.apply()


if __name__ == "__main__":
    unittest.main()
