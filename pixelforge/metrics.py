"""Full-reference and no-reference metrics for the PixelForge quality gate.

Frame alignment is mandatory. A one-frame seek offset makes PSNR, SSIM, and VMAF
describe the wrong pair of pictures, which is how the v1.0.20–v1.0.22 video numbers
became unusable. LPIPS is computed only when the optional packages are installed.
"""

from __future__ import annotations

from typing import Sequence

import numpy as np
from numpy.lib.stride_tricks import sliding_window_view


def as_rgb_uint8(image) -> np.ndarray:
    array = np.asarray(image)
    if array.ndim == 2:
        array = np.repeat(array[:, :, None], 3, axis=2)
    if array.shape[2] > 3:
        array = array[:, :, :3]
    if array.dtype != np.uint8:
        array = np.clip(np.rint(array), 0, 255).astype(np.uint8)
    return np.ascontiguousarray(array)


def match_size(candidate, reference) -> tuple[np.ndarray, bool]:
    """Return ``candidate`` at ``reference``'s height and width.

    Scaling is Lanczos and is recorded so a metric never silently compares
    different pixel grids.
    """
    from PIL import Image

    candidate_rgb = as_rgb_uint8(candidate)
    reference_rgb = as_rgb_uint8(reference)
    if candidate_rgb.shape[:2] == reference_rgb.shape[:2]:
        return candidate_rgb, False
    resized = Image.fromarray(candidate_rgb, mode="RGB").resize(
        (reference_rgb.shape[1], reference_rgb.shape[0]),
        Image.Resampling.LANCZOS,
    )
    return np.asarray(resized, dtype=np.uint8), True


def luma(image: np.ndarray) -> np.ndarray:
    rgb = as_rgb_uint8(image).astype(np.float64)
    return rgb[:, :, 0] * 0.2126 + rgb[:, :, 1] * 0.7152 + rgb[:, :, 2] * 0.0722


def psnr(reference, candidate) -> float:
    left = as_rgb_uint8(reference).astype(np.float64)
    right, _scaled = match_size(candidate, left)
    mse = float(np.mean((left - right.astype(np.float64)) ** 2))
    if mse <= 1e-12:
        return 99.0
    return float(10.0 * np.log10((255.0 * 255.0) / mse))


def _box_mean(values: np.ndarray, size: int = 8) -> np.ndarray:
    windows = sliding_window_view(values, (size, size))
    return windows.mean(axis=(-1, -2))


def ssim(reference, candidate, window: int = 8) -> float:
    """Mean windowed SSIM on luma, after matching the candidate to the reference size."""
    left = luma(reference)
    right_rgb, _scaled = match_size(candidate, as_rgb_uint8(reference))
    right = luma(right_rgb)
    if min(left.shape) < window:
        return _global_ssim(left, right)
    mu_x = _box_mean(left, window)
    mu_y = _box_mean(right, window)
    sigma_x = np.maximum(_box_mean(left * left, window) - mu_x * mu_x, 0.0)
    sigma_y = np.maximum(_box_mean(right * right, window) - mu_y * mu_y, 0.0)
    sigma_xy = _box_mean(left * right, window) - mu_x * mu_y
    c1 = (0.01 * 255.0) ** 2
    c2 = (0.03 * 255.0) ** 2
    numerator = (2.0 * mu_x * mu_y + c1) * (2.0 * sigma_xy + c2)
    denominator = (mu_x * mu_x + mu_y * mu_y + c1) * (sigma_x + sigma_y + c2)
    return float(np.mean(numerator / denominator))


def _global_ssim(left: np.ndarray, right: np.ndarray) -> float:
    c1 = (0.01 * 255.0) ** 2
    c2 = (0.03 * 255.0) ** 2
    mu_x = float(left.mean())
    mu_y = float(right.mean())
    var_x = float(left.var())
    var_y = float(right.var())
    covariance = float(((left - mu_x) * (right - mu_y)).mean())
    return ((2 * mu_x * mu_y + c1) * (2 * covariance + c2)) / ((mu_x**2 + mu_y**2 + c1) * (var_x + var_y + c2))


def tenengrad(image) -> float:
    """No-reference sharpness: mean squared luma gradient (Tenengrad)."""
    y = luma(image)
    if y.shape[0] < 2 or y.shape[1] < 2:
        return 0.0
    gx = np.diff(y, axis=1)
    gy = np.diff(y, axis=0)
    return float(np.mean(gx * gx) + np.mean(gy * gy))


def mean_abs_diff(left, right) -> float:
    a = as_rgb_uint8(left).astype(np.float64)
    b, _scaled = match_size(right, a)
    return float(np.mean(np.abs(a - b.astype(np.float64))))


_LPIPS_MODEL = None
_LPIPS_UNAVAILABLE = False


def lpips_distance(reference, candidate) -> float | None:
    """Alex-net LPIPS when ``lpips`` and ``torch`` import. None when they do not.

    A missing package is not a score. Callers must record that LPIPS was not run.
    """
    global _LPIPS_MODEL, _LPIPS_UNAVAILABLE
    if _LPIPS_UNAVAILABLE:
        return None
    try:
        import lpips
        import torch
    except ImportError:
        _LPIPS_UNAVAILABLE = True
        return None
    if _LPIPS_MODEL is None:
        _LPIPS_MODEL = lpips.LPIPS(net="alex")
        _LPIPS_MODEL.eval()
    left = as_rgb_uint8(reference).astype(np.float32) / 255.0
    right, _scaled = match_size(candidate, left)
    right = right.astype(np.float32) / 255.0

    def _tensor(image: np.ndarray):
        chw = np.transpose(image, (2, 0, 1))[None, ...]
        return torch.from_numpy(chw * 2.0 - 1.0)

    with torch.no_grad():
        value = _LPIPS_MODEL(_tensor(left), _tensor(right))
    return float(value.item())


def score_pair(reference, candidate) -> dict[str, float | None]:
    matched, scaled = match_size(candidate, reference)
    distance = lpips_distance(reference, matched)
    return {
        "psnr": round(psnr(reference, matched), 4),
        "ssim": round(ssim(reference, matched), 6),
        "lpips": None if distance is None else round(distance, 6),
        "sharpness_tenengrad": round(tenengrad(matched), 4),
        "resized_to_reference": scaled,
    }


def best_frame_offset(
    reference_frames: Sequence,
    candidate_frames: Sequence,
    radius: int = 8,
    samples: int = 7,
) -> dict[str, float | int]:
    """Find the integer offset that maximizes windowed SSIM.

    ``offset`` is ``candidate_index - reference_index``. A candidate that dropped
    the first two source frames lines up at offset -2. Comparing at offset 0 in
    that case is the failure mode of the older benchmark.
    """
    if len(reference_frames) == 0 or len(candidate_frames) == 0:
        raise ValueError("Frame alignment needs at least one frame on each side")
    prepared_reference = [as_rgb_uint8(frame) for frame in reference_frames]
    prepared_candidate = []
    scaled_any = False
    for frame in candidate_frames:
        matched, scaled = match_size(frame, prepared_reference[0])
        prepared_candidate.append(matched)
        scaled_any = scaled_any or scaled

    best_offset = 0
    best_score = -1.0
    best_pairs = -1
    for offset in range(-int(radius), int(radius) + 1):
        pairs = _pair_indexes(len(prepared_reference), len(prepared_candidate), offset)
        if not pairs:
            continue
        chosen = _sample_pairs(pairs, samples)
        score = float(np.mean([ssim(prepared_reference[i], prepared_candidate[j]) for i, j in chosen]))
        # A circular shift can score 1.0 twice. Prefer the alignment that uses more
        # frames, then the offset closest to zero. The old benchmark kept offset 0.
        better_score = score > best_score + 1e-6
        same_score = abs(score - best_score) <= 1e-6
        better_overlap = same_score and (
            len(pairs) > best_pairs or (len(pairs) == best_pairs and abs(offset) < abs(best_offset))
        )
        if better_score or better_overlap:
            best_score = score
            best_offset = offset
            best_pairs = len(pairs)
    return {
        "offset": int(best_offset),
        "alignment_ssim": round(best_score, 6),
        "paired_frames": int(best_pairs),
        "candidate_resized": bool(scaled_any),
    }


def aligned_scores(
    reference_frames: Sequence,
    candidate_frames: Sequence,
    radius: int = 8,
) -> dict[str, float | int | bool | None]:
    """Score a candidate only after the best integer frame offset is applied."""
    alignment = best_frame_offset(reference_frames, candidate_frames, radius=radius)
    offset = int(alignment["offset"])
    prepared_reference = [as_rgb_uint8(frame) for frame in reference_frames]
    prepared_candidate = [match_size(frame, prepared_reference[0])[0] for frame in candidate_frames]
    pairs = _pair_indexes(len(prepared_reference), len(prepared_candidate), offset)
    psnr_values = [psnr(prepared_reference[i], prepared_candidate[j]) for i, j in pairs]
    ssim_values = [ssim(prepared_reference[i], prepared_candidate[j]) for i, j in pairs]
    sharp_values = [tenengrad(prepared_candidate[j]) for _i, j in pairs]
    lpips_values = [lpips_distance(prepared_reference[i], prepared_candidate[j]) for i, j in pairs]
    lpips_present = [value for value in lpips_values if value is not None]
    unaligned_pairs = _pair_indexes(len(prepared_reference), len(prepared_candidate), 0)
    unaligned_psnr = [psnr(prepared_reference[i], prepared_candidate[j]) for i, j in unaligned_pairs]
    return {
        **alignment,
        "psnr": round(float(np.mean(psnr_values)), 4),
        "ssim": round(float(np.mean(ssim_values)), 6),
        "lpips": None if not lpips_present else round(float(np.mean(lpips_present)), 6),
        "sharpness_tenengrad": round(float(np.mean(sharp_values)), 4),
        "unaligned_psnr": round(float(np.mean(unaligned_psnr)), 4) if unaligned_psnr else None,
    }


def _pair_indexes(reference_count: int, candidate_count: int, offset: int) -> list[tuple[int, int]]:
    pairs: list[tuple[int, int]] = []
    for index in range(reference_count):
        candidate_index = index + offset
        if 0 <= candidate_index < candidate_count:
            pairs.append((index, candidate_index))
    return pairs


def _sample_pairs(pairs: list[tuple[int, int]], samples: int) -> list[tuple[int, int]]:
    if len(pairs) <= samples:
        return pairs
    positions = np.linspace(0, len(pairs) - 1, samples)
    return [pairs[int(position)] for position in positions]
