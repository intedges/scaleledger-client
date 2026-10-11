from contextlib import chdir
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import paths
from paths import program_dir


class ProgramDirectoryTests(unittest.TestCase):
    def test_source_execution_uses_the_module_folder_from_another_working_directory(self):
        working_dir = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.enterContext(chdir(working_dir))
        with patch.object(paths.sys, "frozen", False, create=True):
            self.assertEqual(program_dir(), Path(paths.__file__).resolve().parent)
            self.assertNotEqual(program_dir(), working_dir.resolve())

    def test_frozen_execution_uses_the_executable_folder_instead_of_extracted_sources(self):
        root = Path(self.enterContext(tempfile.TemporaryDirectory())).resolve()
        executable = root / "프로그램 #1" / "client.exe"
        extracted_source = root / "extracted" / "paths.py"
        self.enterContext(chdir(root))
        with (
            patch.object(paths.sys, "frozen", True, create=True),
            patch.object(paths.sys, "executable", str(executable)),
            patch.object(paths, "__file__", str(extracted_source)),
        ):
            self.assertEqual(program_dir(), executable.parent)
