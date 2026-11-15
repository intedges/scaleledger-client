from dataclasses import FrozenInstanceError, replace
from datetime import UTC, datetime
import unittest

from device_configuration import DeviceConfiguration


def configuration_response(**changes):
    return {
        "auction_name": "동해수산물경매장",
        "vehicle_threshold": 75,
        "vehicle_ready_seconds": "1.5",
        "vehicle_empty_seconds": "2.5",
        "card_prompt_interval": "4.0",
        "completion_interval": "8.5",
        "print_copies": 2,
        "serial_read_timeout": "3.0",
        "serial_write_timeout": "4.5",
        "updated_at": "2026-11-15T10:00:00+09:00",
        **changes,
    }


class DeviceConfigurationTests(unittest.TestCase):
    def test_defaults_preserve_existing_operation_and_are_immutable(self):
        defaults = DeviceConfiguration()
        self.assertEqual(defaults.auction_name, "")
        self.assertEqual(defaults.vehicle_threshold, 50)
        self.assertEqual((defaults.vehicle_ready_seconds, defaults.vehicle_empty_seconds), (1.0, 1.0))
        self.assertEqual((defaults.card_prompt_interval, defaults.completion_interval), (5.0, 10.0))
        self.assertEqual(defaults.print_copies, 1)
        self.assertEqual((defaults.serial_read_timeout, defaults.serial_write_timeout), (2.0, 2.0))
        self.assertIsNone(defaults.updated_at)
        with self.assertRaises(FrozenInstanceError):
            defaults.print_copies = 2

    def test_server_configuration_parses_decimal_strings_and_separate_timestamp(self):
        configuration = DeviceConfiguration.from_response(configuration_response())
        self.assertEqual(configuration, DeviceConfiguration(
            auction_name="동해수산물경매장", vehicle_threshold=75,
            vehicle_ready_seconds=1.5, vehicle_empty_seconds=2.5,
            card_prompt_interval=4.0, completion_interval=8.5, print_copies=2,
            serial_read_timeout=3.0, serial_write_timeout=4.5,
            updated_at=datetime(2026, 11, 15, 1, tzinfo=UTC),
        ))

    def test_boundaries_empty_initial_name_and_unknown_fields(self):
        for seconds in ("0.1", "9999.9"):
            for threshold in (1, 2_147_483_647):
                for copies in (1, 9):
                    with self.subTest(seconds=seconds, threshold=threshold, copies=copies):
                        configuration = DeviceConfiguration.from_response(configuration_response(
                            auction_name="", vehicle_threshold=threshold, print_copies=copies,
                            vehicle_ready_seconds=seconds, vehicle_empty_seconds=seconds,
                            card_prompt_interval=seconds, completion_interval=seconds,
                            serial_read_timeout=seconds, serial_write_timeout=seconds,
                            future_option={"enabled": True},
                        ))
                        self.assertEqual(configuration.print_copies, copies)
                        self.assertEqual(configuration.vehicle_threshold, threshold)
                        self.assertEqual(configuration.vehicle_ready_seconds, float(seconds))
        self.assertEqual(
            DeviceConfiguration.from_response(configuration_response(auction_name="가" * 100)).auction_name,
            "가" * 100,
        )

    def test_missing_null_or_incorrect_configuration_shape_is_rejected(self):
        for value in (None, False, "", [], 1):
            with self.subTest(value=value), self.assertRaises(ValueError):
                DeviceConfiguration.from_response(value)
        for field in configuration_response():
            with self.subTest(missing=field), self.assertRaises(ValueError):
                data = configuration_response()
                del data[field]
                DeviceConfiguration.from_response(data)
            with self.subTest(null=field), self.assertRaises(ValueError):
                DeviceConfiguration.from_response(configuration_response(**{field: None}))

    def test_json_integer_fields_are_not_coerced_or_clamped(self):
        for field, maximum in (("vehicle_threshold", 2_147_483_647), ("print_copies", 9)):
            for value in (True, False, 0, -1, maximum + 1, 1.0, 1.5, "1", [], {}):
                with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                    DeviceConfiguration.from_response(configuration_response(**{field: value}))

    def test_time_fields_require_finite_in_range_one_decimal_strings(self):
        fields = (
            "vehicle_ready_seconds", "vehicle_empty_seconds", "card_prompt_interval",
            "completion_interval", "serial_read_timeout", "serial_write_timeout",
        )
        invalid = (
            True, 1, 1.0, [], {}, "0.0", "-1.0", "10000.0", "0.01", "1.00", "1",
            "NaN", "Infinity", "-Infinity", "1e1", " 1.0", "1.0 ", "abc", "１.０",
        )
        for field in fields:
            for value in invalid:
                with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                    DeviceConfiguration.from_response(configuration_response(**{field: value}))

    def test_name_and_timestamp_are_validated(self):
        for value in (1, False, [], {}, "가" * 101):
            with self.subTest(name=value), self.assertRaises(ValueError):
                DeviceConfiguration.from_response(configuration_response(auction_name=value))
        for value in (
            1, False, [], {}, "", "not-a-date", "2026-11-15", "2026-11-15T10:00:00",
            "2026-13-15T10:00:00+09:00", "2026-11-15T10:00:00+25:00",
        ):
            with self.subTest(timestamp=value), self.assertRaises(ValueError):
                DeviceConfiguration.from_response(configuration_response(updated_at=value))
        self.assertEqual(
            DeviceConfiguration.from_response(configuration_response(updated_at="2026-11-15T01:00:00Z")).updated_at,
            datetime(2026, 11, 15, 1, tzinfo=UTC),
        )

    def test_programmatic_invalid_values_cannot_enter_worker_configuration(self):
        defaults = DeviceConfiguration()
        for changes in (
            {"print_copies": True}, {"print_copies": 10}, {"vehicle_threshold": 0},
            {"serial_read_timeout": float("nan")}, {"serial_write_timeout": float("inf")},
            {"card_prompt_interval": 0.15}, {"completion_interval": True},
            {"updated_at": datetime(2026, 11, 15)},
        ):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                replace(defaults, **changes)


if __name__ == "__main__":
    unittest.main()
