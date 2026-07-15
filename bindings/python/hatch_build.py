"""Hatch hook marking the ctypes wheel as platform-specific.

The platform comes from the active build interpreter. Linux wheels initially
use a ``linux_*`` tag and are subsequently validated/retagged by auditwheel.
On macOS the interpreter tag may be older than the linked library's deployment
target, so delocate validates the Mach-O requirements and raises the final wheel
tag when required; the release workflow asserts the resulting macOS 15 tag.
"""

import sysconfig

from hatchling.builders.hooks.plugin.interface import BuildHookInterface


class CustomBuildHook(BuildHookInterface):
    def initialize(self, version: str, build_data: dict[str, object]) -> None:
        del version
        platform_tag = sysconfig.get_platform().replace("-", "_").replace(".", "_")
        build_data["pure_python"] = False
        # libmes is loaded through ctypes and does not use the CPython ABI.
        build_data["tag"] = f"py3-none-{platform_tag}"
