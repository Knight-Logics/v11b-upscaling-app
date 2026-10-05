"""Distribution channel flags for PixelForge AI.

STORE_BUILD is flipped to True only by build_store.ps1 when producing the
Microsoft Store (MSIX) package. As a safety net, any run inside an MSIX package
identity is also treated as a Store build, so the GitHub self-updater can never
run from C:\\Program Files\\WindowsApps even if the flag was not flipped.
"""

from __future__ import annotations

import sys

STORE_BUILD = False  # build_store.ps1 rewrites this line to True for MSIX builds


def _running_with_package_identity() -> bool:
    if sys.platform != "win32":
        return False
    try:
        import ctypes
        from ctypes import wintypes

        length = wintypes.UINT(0)
        # APPMODEL_ERROR_NO_PACKAGE (15700) means "not packaged".
        rc = ctypes.windll.kernel32.GetCurrentPackageFullName(ctypes.byref(length), None)
        return rc != 15700
    except Exception:
        return False


IS_STORE_BUILD: bool = STORE_BUILD or _running_with_package_identity()

# Store v1 ships without the in-app NVIDIA pack downloader (Store policy 10.2.2:
# no downloading executable code that extends functionality). The RTX profile
# still works if a pack is bundled in the package as a sidecar.
ALLOW_RUNTIME_ENGINE_DOWNLOADS: bool = not IS_STORE_BUILD

STORE_PURCHASE_NOTE = (
    "Purchases are processed by Knight Logics through Stripe, not by Microsoft. "
    "Credit packs are one-time payments; Microsoft Store refunds do not apply. "
    "Billing help: support@knightlogics.com"
)
