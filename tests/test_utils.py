import socket
import unittest
from unittest.mock import MagicMock, patch

from utils import get_ip_address


class IPAddressTests(unittest.TestCase):
    def test_uses_the_resolved_ipv4_or_ipv6_interface(self):
        for host, family, remote, local in (
            ("server-pc", socket.AF_INET, ("192.168.0.10", 80), "192.168.0.20"),
            ("::1", socket.AF_INET6, ("::1", 80, 0, 0), "::1"),
            ("server-pc", socket.AF_INET6, ("fd00::10", 80, 0, 0), "fd00::20"),
        ):
            with self.subTest(host=host, family=family):
                sock = MagicMock()
                sock.__enter__.return_value = sock
                sock.getsockname.return_value = (local, 12345)
                candidate = (family, socket.SOCK_DGRAM, socket.IPPROTO_UDP, "", remote)
                with (
                    patch("utils.socket.getaddrinfo", return_value=[candidate]) as resolve,
                    patch("utils.socket.socket", return_value=sock) as create,
                ):
                    self.assertEqual(get_ip_address(host), local)
                resolve.assert_called_once_with(host, 80, type=socket.SOCK_DGRAM)
                create.assert_called_once_with(family, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
                sock.connect.assert_called_once_with(remote)

    def test_tries_the_next_address_when_a_route_is_unavailable(self):
        ipv6 = (socket.AF_INET6, socket.SOCK_DGRAM, socket.IPPROTO_UDP, "", ("fd00::10", 80, 0, 0))
        ipv4 = (socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP, "", ("192.168.0.10", 80))
        first, second = MagicMock(), MagicMock()
        first.__enter__.return_value = first
        second.__enter__.return_value = second
        first.connect.side_effect = OSError("IPv6 route unavailable")
        second.getsockname.return_value = ("192.168.0.20", 12345)
        with (
            patch("utils.socket.getaddrinfo", return_value=[ipv6, ipv4]),
            patch("utils.socket.socket", side_effect=[first, second]),
        ):
            self.assertEqual(get_ip_address("server-pc"), "192.168.0.20")
        first.__exit__.assert_called_once()
        second.connect.assert_called_once_with(ipv4[4])

    def test_unresolvable_host_keeps_the_loopback_fallback(self):
        with patch("utils.socket.getaddrinfo", side_effect=socket.gaierror("not found")):
            self.assertEqual(get_ip_address("missing-server"), "127.0.0.1")

    def test_unavailable_socket_keeps_the_loopback_fallback(self):
        candidate = (socket.AF_INET6, socket.SOCK_DGRAM, socket.IPPROTO_UDP, "", ("::1", 80, 0, 0))
        with (
            patch("utils.socket.getaddrinfo", return_value=[candidate]),
            patch("utils.socket.socket", side_effect=OSError("IPv6 unavailable")),
        ):
            self.assertEqual(get_ip_address("::1"), "127.0.0.1")
