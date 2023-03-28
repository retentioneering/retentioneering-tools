# type: ignore
# @FIXME: All code about subprocesses and uuids return either str or bytes or something else. Vladimir Makhanov.

import platform
import subprocess
import uuid
from collections import defaultdict


def get_windows_hwid() -> str:
    current_machine_id = str(subprocess.check_output("wmic csproduct get uuid"), "utf-8").split("\n")[1].strip()
    hwid = str(uuid.UUID(current_machine_id))
    return hwid


def get_linux_hwid() -> str:
    hw = subprocess.check_output(["cat", "/etc/machine-id"])
    hw = hw.decode().strip()
    hwid = str(uuid.UUID(hw))
    return hwid


def get_mac_hwid() -> str:
    hw = subprocess.check_output(["cat", "/etc/machine-id"])
    hw = hw.decode().strip()
    hwid = str(uuid.UUID(hw))
    return hwid


def raise_not_implemented() -> str:
    os_name = platform.system()
    raise NotImplementedError(f"OS {os_name} is not supported")


__platforms_HWID = defaultdict(default_factory=raise_not_implemented)
__platforms_HWID["Windows"] = get_windows_hwid
__platforms_HWID["Linux"] = get_linux_hwid
__platforms_HWID["Darwin"] = get_mac_hwid


def get_hwid() -> str:
    os_name = platform.system()
    hwid = __platforms_HWID[os_name]()
    return hwid


__all__ = ("get_hwid",)
