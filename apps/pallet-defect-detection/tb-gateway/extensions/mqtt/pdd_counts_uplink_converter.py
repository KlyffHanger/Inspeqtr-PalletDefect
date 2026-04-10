import json
import math
import os
import threading
from time import time
from typing import Any

from thingsboard_gateway.connectors.mqtt.mqtt_uplink_converter import MqttUplinkConverter
from thingsboard_gateway.gateway.entities.converted_data import ConvertedData
from thingsboard_gateway.gateway.entities.telemetry_entry import TelemetryEntry


def _clamp01(v: float) -> float:
    return max(0.0, min(1.0, v))


def _bbox_from_det(det: dict[str, Any]) -> tuple[float, float, float, float] | None:
    bb = det.get("bounding_box")
    if not isinstance(bb, dict):
        return None
    try:
        x1 = float(bb["x_min"])
        y1 = float(bb["y_min"])
        x2 = float(bb["x_max"])
        y2 = float(bb["y_max"])
    except Exception:
        return None
    x1 = _clamp01(x1)
    y1 = _clamp01(y1)
    x2 = _clamp01(x2)
    y2 = _clamp01(y2)
    if x2 <= x1 or y2 <= y1:
        return None
    return (x1, y1, x2, y2)


def _iou(a: tuple[float, float, float, float], b: tuple[float, float, float, float]) -> float:
    ax1, ay1, ax2, ay2 = a
    bx1, by1, bx2, by2 = b
    ix1 = max(ax1, bx1)
    iy1 = max(ay1, by1)
    ix2 = min(ax2, bx2)
    iy2 = min(ay2, by2)
    iw = max(0.0, ix2 - ix1)
    ih = max(0.0, iy2 - iy1)
    inter = iw * ih
    if inter <= 0.0:
        return 0.0
    a_area = (ax2 - ax1) * (ay2 - ay1)
    b_area = (bx2 - bx1) * (by2 - by1)
    union = a_area + b_area - inter
    return inter / union if union > 0 else 0.0


def _center(b: tuple[float, float, float, float]) -> tuple[float, float]:
    x1, y1, x2, y2 = b
    return ((x1 + x2) / 2.0, (y1 + y2) / 2.0)


def _line_side(px: float, py: float, x1: float, y1: float, x2: float, y2: float) -> float:
    # Cross product sign: (x2-x1,y2-y1) x (px-x1,py-y1)
    return (x2 - x1) * (py - y1) - (y2 - y1) * (px - x1)


def _sign(v: float, eps: float = 1e-9) -> int:
    if v > eps:
        return 1
    if v < -eps:
        return -1
    return 0


class _Track:
    __slots__ = ("track_id", "bbox", "score", "last_frame_id", "lost", "counted", "defective", "last_side")

    def __init__(self, track_id: int, bbox: tuple[float, float, float, float], score: float, frame_id: int) -> None:
        self.track_id = track_id
        self.bbox = bbox
        self.score = score
        self.last_frame_id = frame_id
        self.lost = 0
        self.counted = False
        self.defective = False
        self.last_side: int | None = None


class _ByteTrackLite:
    """
    Lightweight ByteTrack-inspired tracker:
    - Match high-confidence detections first, then low-confidence.
    - Greedy IoU matching (fast, good enough for a small number of objects).
    """

    def __init__(
        self,
        *,
        iou_threshold: float,
        high_conf: float,
        low_conf: float,
        max_lost: int,
    ) -> None:
        self._iou_threshold = float(iou_threshold)
        self._high_conf = float(high_conf)
        self._low_conf = float(low_conf)
        self._max_lost = int(max_lost)
        self._next_id = 1
        self._tracks: dict[int, _Track] = {}

    @property
    def tracks(self) -> list[_Track]:
        return list(self._tracks.values())

    def update(self, detections: list[tuple[tuple[float, float, float, float], float]], frame_id: int) -> None:
        high = [(b, s) for (b, s) in detections if s >= self._high_conf]
        low = [(b, s) for (b, s) in detections if self._low_conf <= s < self._high_conf]

        active_ids = list(self._tracks.keys())
        matched: set[int] = set()

        def _match(cands: list[tuple[tuple[float, float, float, float], float]]) -> list[tuple[int, tuple[float, float, float, float], float]]:
            # Returns list of (track_id, bbox, score)
            results: list[tuple[int, tuple[float, float, float, float], float]] = []
            for bbox, score in cands:
                best_tid: int | None = None
                best_iou = 0.0
                for tid in active_ids:
                    if tid in matched:
                        continue
                    tr = self._tracks.get(tid)
                    if tr is None:
                        continue
                    i = _iou(tr.bbox, bbox)
                    if i >= self._iou_threshold and i > best_iou:
                        best_iou = i
                        best_tid = tid
                if best_tid is not None:
                    matched.add(best_tid)
                    results.append((best_tid, bbox, score))
            return results

        # Match high first, then low to remaining tracks.
        for tid, bbox, score in _match(high):
            tr = self._tracks[tid]
            tr.bbox = bbox
            tr.score = score
            tr.last_frame_id = frame_id
            tr.lost = 0

        for tid, bbox, score in _match(low):
            tr = self._tracks[tid]
            tr.bbox = bbox
            tr.score = score
            tr.last_frame_id = frame_id
            tr.lost = 0

        # Create new tracks for unmatched high detections.
        for bbox, score in high:
            if any(_iou(bbox, self._tracks[tid].bbox) >= self._iou_threshold for tid in matched):
                continue
            tid = self._next_id
            self._next_id += 1
            self._tracks[tid] = _Track(tid, bbox, score, frame_id)

        # Age out lost tracks.
        to_delete: list[int] = []
        for tid, tr in self._tracks.items():
            if tr.last_frame_id != frame_id:
                tr.lost += 1
                if tr.lost > self._max_lost:
                    to_delete.append(tid)
        for tid in to_delete:
            del self._tracks[tid]


class PddCountsUplinkConverter(MqttUplinkConverter):
    def __init__(self, config: dict[str, Any], logger) -> None:
        self._log = logger
        self.__config = (config or {}).get("converter", {}) or {}
        extension_config_key = (
            "extensionConfig" if self.__config.get("extensionConfig") is not None else "extension-config"
        )
        self.__ext_cfg = self.__config.get(extension_config_key, {}) or {}
        self.__n = 0
        self.__lock = threading.Lock()
        self.__persist_totals = bool(self.__ext_cfg.get("persistTotals", True))
        self.__reset_on_start = bool(self.__ext_cfg.get("resetOnStart", False))
        self.__save_every_n = int(self.__ext_cfg.get("saveEveryN", 10) or 10)
        self.__totals_path = self.__ext_cfg.get(
            "totalsPath",
            "/var/lib/thingsboard_gateway/data/pdd_counts_totals.json",
        )
        # Totals are now event-based to avoid 30fps duplication:
        # - `box_count`: number of unique boxes that crossed the line
        # - `defect_count`: number of unique defective boxes that crossed the line
        # - `detection_count`: number of unique box tracks created (unique boxes seen)
        self.__totals: dict[str, int] = {"defect": 0, "box": 0, "detection": 0}
        if self.__persist_totals and not self.__reset_on_start:
            self.__load_totals()

        # Tracker + line-crossing config (ByteTrack-lite).
        self.__iou_threshold = float(self.__ext_cfg.get("iouThreshold", 0.25) or 0.25)
        self.__high_conf = float(self.__ext_cfg.get("highConf", 0.6) or 0.6)
        self.__low_conf = float(self.__ext_cfg.get("lowConf", 0.1) or 0.1)
        self.__max_lost = int(self.__ext_cfg.get("maxLostFrames", 30) or 30)
        self.__defect_iou = float(self.__ext_cfg.get("defectIouThreshold", 0.1) or 0.1)
        self.__defect_conf = float(self.__ext_cfg.get("defectConfThreshold", 0.5) or 0.5)
        self.__count_direction = str(self.__ext_cfg.get("countDirection", "any") or "any").lower()

        line = self.__ext_cfg.get("countLine", None)
        if not isinstance(line, dict):
            line = {"x1": 0.0, "y1": 0.5, "x2": 1.0, "y2": 0.5}  # default horizontal mid-line
        self.__line_x1 = float(line.get("x1", 0.0) or 0.0)
        self.__line_y1 = float(line.get("y1", 0.5) or 0.5)
        self.__line_x2 = float(line.get("x2", 1.0) or 1.0)
        self.__line_y2 = float(line.get("y2", 0.5) or 0.5)

        self.__tracker = _ByteTrackLite(
            iou_threshold=self.__iou_threshold,
            high_conf=self.__high_conf,
            low_conf=self.__low_conf,
            max_lost=self.__max_lost,
        )

    @property
    def config(self) -> dict[str, Any]:
        return self.__config

    def __load_totals(self) -> None:
        try:
            if not self.__totals_path:
                return
            if not os.path.exists(self.__totals_path):
                return
            with open(self.__totals_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                self.__totals["defect"] = int(data.get("defect", 0) or 0)
                self.__totals["box"] = int(data.get("box", 0) or 0)
                self.__totals["detection"] = int(data.get("detection", 0) or 0)
        except Exception:
            self._log.exception("Failed to load totals from %s", self.__totals_path)

    def __save_totals(self) -> None:
        if not self.__persist_totals or not self.__totals_path:
            return
        try:
            os.makedirs(os.path.dirname(self.__totals_path), exist_ok=True)
            tmp_path = f"{self.__totals_path}.tmp"
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(self.__totals, f, separators=(",", ":"))
            os.replace(tmp_path, self.__totals_path)
        except Exception:
            self._log.exception("Failed to save totals to %s", self.__totals_path)

    def convert(self, topic: str, body):
        device_name = self.__ext_cfg.get("deviceName", "pallet-defect-detection-edge")
        device_type = self.__ext_cfg.get("deviceType", "default")
        converted = ConvertedData(device_name=device_name, device_type=device_type)
        try:
            current_n = self.__n + 1
            log_every_n = int(self.__ext_cfg.get("logEveryN", 0) or 0)
            log_tracks = bool(self.__ext_cfg.get("logTracks", False))
            should_log = log_every_n > 0 and current_n % log_every_n == 0

            if isinstance(body, dict):
                msg = body
            elif isinstance(body, (bytes, bytearray)):
                msg = json.loads(bytes(body).decode("utf-8", errors="strict"))
            elif isinstance(body, str):
                msg = json.loads(body)
            else:
                msg = json.loads(str(body))

            metadata = msg.get("metadata") if isinstance(msg, dict) else None
            objects = []
            if isinstance(metadata, dict):
                maybe_objects = metadata.get("objects")
                if isinstance(maybe_objects, list):
                    objects = maybe_objects

            # Extract detections.
            box_dets: list[tuple[tuple[float, float, float, float], float]] = []
            defect_dets: list[tuple[tuple[float, float, float, float], float]] = []
            detection_count_frame = 0

            for obj in objects:
                if not isinstance(obj, dict):
                    continue
                det = obj.get("detection")
                if not isinstance(det, dict):
                    continue
                detection_count_frame += 1
                bbox = _bbox_from_det(det)
                if bbox is None:
                    continue
                conf = det.get("confidence")
                score = float(conf) if isinstance(conf, (int, float)) else 0.0
                label = det.get("label")
                if label == "box":
                    box_dets.append((bbox, score))
                elif label == "defect":
                    defect_dets.append((bbox, score))

            frame_id = 0
            if isinstance(metadata, dict) and isinstance(metadata.get("frame_id"), int):
                frame_id = int(metadata["frame_id"])

            ts = int(time() * 1000)
            track_logs: list[tuple[int, int, float, float, int, bool, bool]] = []

            with self.__lock:
                # Update tracker.
                prev_track_ids = {t.track_id for t in self.__tracker.tracks}
                self.__tracker.update(box_dets, frame_id=frame_id)
                new_track_ids = {t.track_id for t in self.__tracker.tracks}
                created = len(new_track_ids - prev_track_ids)
                if created:
                    self.__totals["detection"] += created

                # Associate defect detections to box tracks (mark track defective).
                for tr in self.__tracker.tracks:
                    for bbox, score in defect_dets:
                        if score < self.__defect_conf:
                            continue
                        if _iou(tr.bbox, bbox) >= self.__defect_iou:
                            tr.defective = True
                            break

                # Line crossing events.
                box_cross_frame = 0
                defect_box_cross_frame = 0
                for tr in self.__tracker.tracks:
                    cx, cy = _center(tr.bbox)
                    if should_log and log_tracks:
                        track_logs.append((frame_id, tr.track_id, cx, cy, tr.lost, tr.counted, tr.defective))
                    side_val = _line_side(cx, cy, self.__line_x1, self.__line_y1, self.__line_x2, self.__line_y2)
                    side = _sign(side_val)
                    if tr.last_side is None:
                        tr.last_side = side
                        continue
                    if side == 0 or tr.last_side == 0:
                        tr.last_side = side
                        continue
                    crossed = (side != tr.last_side)
                    if crossed and not tr.counted:
                        # Direction filter (relative to line orientation).
                        if self.__count_direction in ("any", ""):
                            allowed = True
                        elif self.__count_direction in ("pos_to_neg", "a_to_b"):
                            allowed = tr.last_side > 0 and side < 0
                        elif self.__count_direction in ("neg_to_pos", "b_to_a"):
                            allowed = tr.last_side < 0 and side > 0
                        else:
                            allowed = True
                        if allowed:
                            tr.counted = True
                            box_cross_frame += 1
                            self.__totals["box"] += 1
                            if tr.defective:
                                defect_box_cross_frame += 1
                                self.__totals["defect"] += 1
                    tr.last_side = side

                telemetry: dict[str, Any] = {
                    # Cumulative, de-duplicated by tracking + line crossing.
                    "box_count": self.__totals["box"],
                    "defect_count": self.__totals["defect"],
                    "detection_count": self.__totals["detection"],
                    "defect_percentage": ((self.__totals["defect"] / self.__totals["box"]) * 100.0) if self.__totals["box"] > 0 else 0.0,
                    # Per-frame for debugging.
                    "box_cross_frame": box_cross_frame,
                    "defect_box_cross_frame": defect_box_cross_frame,
                    "detections_in_frame": detection_count_frame,
                    "active_box_tracks": len(self.__tracker.tracks),
                }

                if self.__persist_totals and self.__save_every_n > 0 and (self.__totals["box"] + self.__totals["defect"] + self.__totals["detection"]) % self.__save_every_n == 0:
                    self.__save_totals()

            if isinstance(metadata, dict):
                if isinstance(metadata.get("frame_id"), int):
                    telemetry["frame_id"] = metadata["frame_id"]
                if isinstance(metadata.get("width"), int):
                    telemetry["frame_width"] = metadata["width"]
                if isinstance(metadata.get("height"), int):
                    telemetry["frame_height"] = metadata["height"]

            converted.add_to_telemetry(TelemetryEntry(telemetry, ts=ts))
            self.__n = current_n
            if should_log:
                self._log.info(
                    "pdd_counts device=%s defect=%d box=%d total=%d",
                    device_name,
                    telemetry.get("defect_count", 0),
                    telemetry.get("box_count", 0),
                    telemetry.get("detection_count", 0),
                )
                if log_tracks:
                    for fid, tid, cx, cy, lost, counted, defective in track_logs:
                        self._log.info(
                            "[Frame %s] ID=%s center=(%.2f,%.2f) lost=%s counted=%s defective=%s",
                            fid,
                            tid,
                            cx,
                            cy,
                            lost,
                            counted,
                            defective,
                        )
            return converted
        except Exception:
            self._log.exception("PddCountsUplinkConverter failed for topic=%s", topic)
            return converted
