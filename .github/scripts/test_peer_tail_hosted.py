import unittest
from pathlib import Path
import tempfile
from unittest.mock import patch

from peer_tail_hosted import Experiment, SERVICES, cpu_delta, ownership, parse_stat, regression_signal, unattributed_task_cpu, verify_membership


def proc(pid, ppid=0, comm="other", **kwargs):
    return {"pid": pid, "ppid": ppid, "comm": comm, "kernel": False,
            "cgroup": "", "start": 100, "ticks": 0, **kwargs}


class HostedEvidenceTests(unittest.TestCase):
    def test_linux_stat_handles_spaces_and_parentheses_in_comm(self):
        fields = ["0"] * 20
        fields[0], fields[1], fields[6] = "R", "12", str(0x00200000)
        fields[11], fields[12], fields[19] = "20", "3", "400"
        parsed = parse_stat("123 (worker (test)) " + " ".join(fields))
        self.assertEqual((parsed["pid"], parsed["comm"], parsed["ppid"], parsed["ticks"], parsed["start"]),
                         (123, "worker (test)", 12, 23, 400))
        self.assertTrue(parsed["kernel"])

    def test_own_container_cgroup_and_ancestry_unknown_user_fails_closed(self):
        containers = [{"id": "abc123", "pid": 10}]
        procs = {1: proc(1), 10: proc(10, 1), 11: proc(11, 10),
                 12: proc(12, 1, cgroup="0::/docker/abc123"),
                 20: proc(20, 1), 21: proc(21, 20), 30: proc(30, 1, comm="unknown-work")}
        for pid in [10, 11, 12]:
            self.assertEqual(ownership(pid, procs, containers, 20), ("own_container", "abc123"))
        self.assertEqual(ownership(21, procs, containers, 20), ("harness", None))
        self.assertEqual(ownership(30, procs, containers, 20), ("external_user", None))

    def test_pid_reuse_and_new_process_cpu_accounting(self):
        previous = {10: proc(10, start=100, ticks=30)}
        self.assertEqual(cpu_delta(proc(10, start=100, ticks=45), previous, 200), 15)
        self.assertEqual(cpu_delta(proc(10, start=220, ticks=7), previous, 200), 7)
        self.assertEqual(cpu_delta(proc(11, start=220, ticks=8), previous, 200), 8)
        self.assertEqual(cpu_delta(proc(11, start=100, ticks=80), previous, 200), 0)

    def test_exited_cpu_burner_is_not_silently_discarded(self):
        # Five seconds, 100 Hz: a process that used one core then exited is absent
        # from the PID snapshot, but contributes 500 ticks to global user CPU.
        deltas = [500, 0, 0, 1500, 0, 25, 25, 0]
        self.assertEqual(unattributed_task_cpu(deltas, 0, 100, 5), 1)
        self.assertEqual(unattributed_task_cpu(deltas, 1, 100, 5), 0)
        self.assertEqual(unattributed_task_cpu(deltas, 1.1, 100, 5), 0)
        # IRQ/softirq aren't user-process evidence and aren't charged twice.
        self.assertEqual(unattributed_task_cpu([0, 0, 0, 1500, 0, 500, 500, 0], 0, 100, 5), 0)

    def test_every_runtime_init_and_child_must_be_visible(self):
        containers, procs = [], {}
        for index, service in enumerate(SERVICES):
            pid = index * 2 + 10
            containers.append({"id": str(index), "pid": pid, "service": service})
            procs[pid] = proc(pid, comm="docker-init")
            binary = "relaygate-echo-listener" if service.startswith("listener-") else "relaygate-server"
            procs[pid + 1] = proc(pid + 1, pid, binary[:15])
        owners = {pid: ownership(pid, procs, containers, 999) for pid in procs}
        verify_membership(procs, containers, owners)
        del procs[11]
        with self.assertRaisesRegex(RuntimeError, "runtime child"):
            verify_membership(procs, containers, owners)
        with self.assertRaisesRegex(RuntimeError, "runtime container absent"):
            verify_membership({}, [], {})

    def test_predeclared_paired_block_rule(self):
        criterion = {"p99_increase_percent": 10, "minimum_blocks_over_threshold": 4}
        self.assertTrue(regression_signal([20, 20, 20, 20, -20], criterion))
        self.assertFalse(regression_signal([20, 20, 20, -20, -20], criterion))
        self.assertFalse(regression_signal([10, 10, 10, 10, 10], criterion))

    def test_monitor_initialization_failure_aborts_immediately(self):
        with tempfile.TemporaryDirectory() as directory, patch("peer_tail_hosted.OUT", Path(directory)):
            experiment = Experiment()
            with patch.object(experiment, "monitor_loop", side_effect=OSError("proc unavailable")):
                experiment.monitor()
            self.assertTrue(experiment.abort.is_set())
            self.assertTrue(experiment.monitor_finished.is_set())
            self.assertIn("proc unavailable", experiment.reason)


if __name__ == "__main__":
    unittest.main()
