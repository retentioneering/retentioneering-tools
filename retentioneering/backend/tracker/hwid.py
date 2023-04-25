# type: ignore
# @FIXME: All code about subprocesses and uuids return either str or bytes or something else. Vladimir Makhanov.

import platform
import subprocess
import uuid
from collections import defaultdict

UNDEFINED_PLATFORM_UUID = uuid.UUID("00000000-0000-0000-aaaa-eeeeeeeeeeee")
UNDEFINED_WINDOWS_PLATFORM_UUID = uuid.UUID("00000000-0000-0000-aaaa-aaaaaaaaaaaa")
UNDEFINED_LINUX_PLATFORM_UUID = uuid.UUID("00000000-0000-0000-aaaa-bbbbbbbbbbbb")
UNDEFINED_MAC_PLATFORM_UUID = uuid.UUID("00000000-0000-0000-aaaa-cccccccccccc")


def get_windows_hwid() -> str:
    try:
        current_machine_id = (
            str(subprocess.check_output("wmic csproduct get uuid", stderr=subprocess.DEVNULL), "utf-8")
            .split("\n")[1]
            .strip()
        )
        hwid = str(uuid.UUID(current_machine_id))
    except Exception:  # not sure if this is the right exception, but I don't know right one. Vladimir Makhanov.
        hwid = str(UNDEFINED_WINDOWS_PLATFORM_UUID)
    return hwid


def get_linux_hwid() -> str:
    try:
        hw = subprocess.check_output(["cat", "/etc/machine-id"], stderr=subprocess.DEVNULL)
        hw = hw.decode().strip()
        hwid = str(uuid.UUID(hw))
    except Exception:  # not sure if this is the right exception, but I don't know right one. Vladimir Makhanov.
        hwid = str(UNDEFINED_LINUX_PLATFORM_UUID)
    return hwid


def get_mac_hwid() -> str:
    try:
        hw = subprocess.check_output(["cat", "/etc/machine-id"], stderr=subprocess.DEVNULL)
        hw = hw.decode().strip()
        hwid = str(uuid.UUID(hw))
    except Exception:  # not sure if this is the right exception, but I don't know right one. Vladimir Makhanov.
        hwid = str(UNDEFINED_MAC_PLATFORM_UUID)
    return hwid


def undefined_platform() -> str:
    return str(UNDEFINED_PLATFORM_UUID)


__platforms_HWID = defaultdict(default_factory=undefined_platform)
__platforms_HWID["Windows"] = get_windows_hwid
__platforms_HWID["Linux"] = get_linux_hwid
__platforms_HWID["Darwin"] = get_mac_hwid


def get_hwid() -> str:
    try:
        os_name = platform.system()
        hwid = __platforms_HWID[os_name]()
    except Exception as e:
        hwid = UNDEFINED_PLATFORM_UUID
    return hwid


__all__ = ("get_hwid",)
