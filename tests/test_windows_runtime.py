from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from windows_runtime import WindowsStopEvent, request_stop


@unittest.skipUnless(sys.platform == "win32", "requires actual Windows kernel events")
class WindowsStopEventTests(unittest.TestCase):
    def test_other_process_can_signal_and_closed_events_do_not_survive_restart(self):
        root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        event = WindowsStopEvent(root)
        try:
            self.assertFalse(event.is_set())
            subprocess.run(
                [sys.executable, "-c", "from pathlib import Path; import sys; "
                 "from windows_runtime import request_stop; "
                 "sys.exit(0 if request_stop(Path(sys.argv[1])) else 1)", str(root)],
                cwd=Path(__file__).resolve().parent.parent, check=True, timeout=10,
            )
            self.assertTrue(event.is_set())
        finally:
            event.close()
        self.assertFalse(request_stop(root))
        restarted = WindowsStopEvent(root)
        try:
            self.assertFalse(restarted.is_set())
        finally:
            restarted.close()

    def test_a_different_installation_cannot_stop_this_instance(self):
        root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        first = WindowsStopEvent(root / "first")
        second = WindowsStopEvent(root / "second")
        try:
            self.assertTrue(request_stop(root / "second"))
            self.assertFalse(first.is_set())
            self.assertTrue(second.is_set())
        finally:
            first.close()
            second.close()
