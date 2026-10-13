import sys
import os
import json
import re
import shutil
import hashlib
import subprocess
import traceback
import uuid
import webbrowser
import ctypes
from dataclasses import dataclass, asdict, fields
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, List, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from PyQt6.QtCore import Qt, QSize, QObject, pyqtSignal, QRunnable, QThreadPool, QTimer, QEvent
from PyQt6.QtGui import QAction, QColor, QIcon, QKeySequence, QPalette, QPixmap
from PyQt6.QtWidgets import (
    QApplication,
    QFileDialog,
    QHBoxLayout,
    QInputDialog,
    QLabel,
    QListWidget,
    QListWidgetItem,
    QMainWindow,
    QMenu,
    QMessageBox,
    QPushButton,
    QLineEdit,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

try:
    import pygame
except Exception:
    pygame = None

APP_NAME = "Sage Game Launcher"
APP_VERSION = "4.3"

# Portable mode: all state lives next to this script/executable so the whole
# folder can be moved or run from removable media. When frozen (PyInstaller
# etc.), anchor to the exe location rather than the bundled temp dir.
if getattr(sys, "frozen", False):
    BASE_DIR = Path(sys.executable).resolve().parent
else:
    BASE_DIR = Path(__file__).resolve().parent

CACHE_DIR = BASE_DIR / "launcher_cache"
DATA_FILE = BASE_DIR / "games_data.json"
SETTINGS_FILE = BASE_DIR / "launcher_settings.json"
PLACEHOLDER_FILE = BASE_DIR / "placeholder.png"

CARD_WIDTH = 300
CARD_HEIGHT = 170
GRID_EXTRA_HEIGHT = 58
GRID_SPACING = 12
MAX_WORKERS = 4
SAVE_DEBOUNCE_MS = 500

SUPPORTED_DROPS = {".exe", ".lnk", ".bat", ".cmd", ".url"}


def atomic_write_json(path: Path, payload) -> None:
    """Write JSON via temp file + os.replace so a crash can't corrupt the file."""
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2, ensure_ascii=False)
    os.replace(tmp, path)


@dataclass
class GameEntry:
    """Stores persistent information for one game entry."""

    game_id: str = ""
    name: str = "Unknown Game"
    path: str = ""
    image: str = ""
    steam_id: str = ""
    release_date: str = ""
    favorite: bool = False
    metadata_status: str = "pending"
    image_source: str = "placeholder"
    added_at: str = ""
    last_played: str = ""
    parameters: str = ""
    steam_rating: str = ""

    @classmethod
    def from_dict(cls, data: dict) -> "GameEntry":
        known = {f.name for f in fields(cls)}
        filtered = {k: v for k, v in data.items() if k in known}
        game = cls(**filtered)
        if not game.game_id:
            game.game_id = uuid.uuid4().hex
        return game


class GameRepository:
    """Handles saving/loading library data and settings."""

    def __init__(self, data_file: Path, settings_file: Path, cache_dir: Path, placeholder_file: Path):
        self.data_file = data_file
        self.settings_file = settings_file
        self.cache_dir = cache_dir
        self.placeholder_file = placeholder_file
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.corrupt_backup: Optional[Path] = None

    def load_games(self) -> Dict[str, GameEntry]:
        if not self.data_file.exists():
            return {}

        try:
            with self.data_file.open("r", encoding="utf-8") as fh:
                raw = json.load(fh)
        except Exception as exc:
            # Never silently discard: back up the unreadable file so it isn't
            # overwritten by the next save.
            backup = self.data_file.with_suffix(".json.bak")
            try:
                shutil.copy2(self.data_file, backup)
                self.corrupt_backup = backup
            except Exception:
                pass
            print(f"Failed to load games data ({exc}); backed up to {backup}", file=sys.stderr)
            return {}

        games = {}
        for game_id, payload in raw.items():
            payload.setdefault("game_id", game_id)
            game = GameEntry.from_dict(payload)
            if not game.image:
                game.image = str(self.placeholder_file)
            games[game.game_id] = game
        return games

    def save_games(self, games: Dict[str, GameEntry]) -> None:
        serializable = {gid: asdict(game) for gid, game in games.items()}
        atomic_write_json(self.data_file, serializable)

    def load_settings(self) -> dict:
        default = {
            "window_width": 1280,
            "window_height": 760,
            "controller_enabled": True,
        }
        if not self.settings_file.exists():
            return default
        try:
            with self.settings_file.open("r", encoding="utf-8") as fh:
                data = json.load(fh)
            default.update(data)
            return default
        except Exception:
            return default

    def save_settings(self, settings: dict) -> None:
        atomic_write_json(self.settings_file, settings)

    def cleanup_unused_cache(self, games: Dict[str, GameEntry]) -> List[str]:
        referenced = {
            str(Path(game.image).resolve())
            for game in games.values()
            if game.image and Path(game.image).exists()
        }
        placeholder_resolved = str(self.placeholder_file.resolve()) if self.placeholder_file.exists() else ""
        removed = []

        for file in self.cache_dir.glob("*"):
            try:
                file_resolved = str(file.resolve())
                if file_resolved == placeholder_resolved:
                    continue
                if file_resolved not in referenced and file.is_file():
                    file.unlink()
                    removed.append(file.name)
            except Exception:
                continue
        return removed


class MetadataSignals(QObject):
    finished = pyqtSignal(str, dict)
    failed = pyqtSignal(str, str)


class MetadataFetchWorker(QRunnable):
    """Fetches Steam metadata and artwork in a background thread."""

    def __init__(self, game_id: str, game_name: str, game_path: str, cache_dir: Path):
        super().__init__()
        self.game_id = game_id
        self.game_name = game_name
        self.game_path = game_path
        self.cache_dir = cache_dir
        self.signals = MetadataSignals()

    @staticmethod
    def sanitize_filename(value: str) -> str:
        return re.sub(r"[^A-Za-z0-9._-]", "_", value)[:100] or "game"

    @staticmethod
    def normalize_release_date(raw_date: str) -> str:
        if not raw_date:
            return ""
        raw_date = raw_date.strip()
        formats = [
            "%d %b, %Y",
            "%b %d, %Y",
            "%d %B, %Y",
            "%B %d, %Y",
            "%Y-%m-%d",
            "%b %Y",
            "%B %Y",
            "%Y",
        ]
        for fmt in formats:
            try:
                dt = datetime.strptime(raw_date, fmt)
                return str(dt.year)
            except ValueError:
                continue
        return ""

    @staticmethod
    def _name_similarity(a: str, b: str) -> float:
        """0-100 similarity. Uses rapidfuzz if available, else difflib."""
        try:
            from rapidfuzz import fuzz
            return float(fuzz.ratio(a.lower(), b.lower()))
        except ImportError:
            import difflib
            return difflib.SequenceMatcher(None, a.lower(), b.lower()).ratio() * 100.0

    def _pick_best_match(self, items: list) -> Optional[dict]:
        """Score storesearch results against the entry name; reject poor matches."""
        best_item, best_score = None, 0.0
        for item in items[:8]:
            candidate_name = str(item.get("name", ""))
            if not candidate_name:
                continue
            score = self._name_similarity(self.game_name, candidate_name)
            if score > best_score:
                best_item, best_score = item, score
        if best_item is not None and best_score >= 80.0:
            return best_item
        # Single-result searches are usually intentional exact-ish hits; accept
        # a lone result at a lower bar than a crowded result list.
        if len(items) == 1 and best_item is not None and best_score >= 60.0:
            return best_item
        return None

    def _build_session(self) -> requests.Session:
        session = requests.Session()
        session.headers.update({"User-Agent": f"SageGameLauncher/{APP_VERSION}"})
        retries = Retry(total=2, backoff_factor=0.5, status_forcelist=(429, 500, 502, 503, 504))
        session.mount("https://", HTTPAdapter(max_retries=retries))
        return session

    def run(self) -> None:
        try:
            with self._build_session() as session:
                search_res = session.get(
                    "https://store.steampowered.com/api/storesearch/",
                    params={"term": self.game_name, "l": "english", "cc": "US"},
                    timeout=10,
                )
                search_res.raise_for_status()
                search_json = search_res.json()
                items = search_json.get("items", []) if isinstance(search_json, dict) else []

                match = self._pick_best_match(items) if items else None

                if not match:
                    self.signals.finished.emit(
                        self.game_id,
                        {
                            "path": self.game_path,
                            "image": "",
                            "steam_id": "",
                            "release_date": "",
                            "steam_rating": "",
                            "metadata_status": "not_found",
                            "image_source": "placeholder",
                        },
                    )
                    return

                app_id = str(match.get("id", ""))

                details_res = session.get(
                    "https://store.steampowered.com/api/appdetails",
                    params={"appids": app_id, "l": "english", "cc": "US"},
                    timeout=10,
                )
                details_res.raise_for_status()
                details_json = details_res.json()

                steam_rating = ""
                if app_id:
                    try:
                        rev_res = session.get(
                            f"https://store.steampowered.com/appreviews/{app_id}",
                            params={"json": "1", "language": "all"},
                            timeout=10,
                        )
                        if rev_res.ok:
                            steam_rating = rev_res.json().get("query_summary", {}).get("review_score_desc", "")
                    except Exception:
                        pass

                release_date = ""
                if details_json.get(app_id, {}).get("success"):
                    raw_rel = details_json[app_id].get("data", {}).get("release_date", {}).get("date", "")
                    release_date = self.normalize_release_date(raw_rel)

                image_path = ""
                image_source = "placeholder"

                if app_id:
                    # cdn.steamstatic.com is the current domain; the old
                    # cdn.akamai.steamstatic.com endpoint is retired.
                    img_url = f"https://cdn.steamstatic.com/steam/apps/{app_id}/header.jpg"
                    img_res = session.get(img_url, timeout=10)
                    if img_res.ok and img_res.content:
                        safe_name = self.sanitize_filename(f"{self.game_name}_{app_id}")
                        image_file = self.cache_dir / f"{safe_name}.jpg"
                        image_file.write_bytes(img_res.content)
                        image_path = str(image_file)
                        image_source = "steam"

                self.signals.finished.emit(
                    self.game_id,
                    {
                        "path": self.game_path,
                        "image": image_path,
                        "steam_id": app_id,
                        "release_date": release_date,
                        "steam_rating": steam_rating,
                        "metadata_status": "ready" if app_id else "not_found",
                        "image_source": image_source,
                    },
                )
        except Exception as exc:
            self.signals.failed.emit(self.game_id, f"Metadata fetch failed for '{self.game_name}': {exc}")


class GameLauncherList(QListWidget):
    """Tile grid widget for games."""

    def __init__(self, parent=None):
        super().__init__(parent)

        self.log_callback = None
        self.games: Dict[str, GameEntry] = {}
        self.placeholder_path = str(PLACEHOLDER_FILE)
        self._search_query = ""
        self._icon_cache: Dict[Tuple[str, int], QPixmap] = {}

        self.setViewMode(QListWidget.ViewMode.IconMode)
        self.setFlow(QListWidget.Flow.LeftToRight)
        self.setWrapping(True)
        self.setResizeMode(QListWidget.ResizeMode.Adjust)
        self.setMovement(QListWidget.Movement.Static)
        self.setUniformItemSizes(True)
        self.setWordWrap(True)
        self.setSelectionMode(QListWidget.SelectionMode.SingleSelection)
        self.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)

        self.setAcceptDrops(True)
        self.setDragDropMode(QListWidget.DragDropMode.DropOnly)

        self.setIconSize(QSize(CARD_WIDTH, CARD_HEIGHT))
        self.setGridSize(QSize(CARD_WIDTH + 24, CARD_HEIGHT + GRID_EXTRA_HEIGHT))
        self.setSpacing(GRID_SPACING)
        self.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        self.setVerticalScrollMode(QListWidget.ScrollMode.ScrollPerPixel)
        self.setHorizontalScrollMode(QListWidget.ScrollMode.ScrollPerPixel)
        self.setLayoutMode(QListWidget.LayoutMode.Batched)
        self.setBatchSize(64)

        self.setStyleSheet("""
            QListWidget {
                background-color: #1e1e1e;
                border: 1px solid #7A0000;
            }
            QListWidget::item {
                margin: 6px;
                padding: 4px;
                border: 2px solid transparent;
                border-radius: 6px;
            }
            QListWidget::item:selected {
                background: transparent;
                color: white;
                border: 3px solid red;
            }
            QListWidget::item:hover {
                border: 2px solid #666666;
            }
        """)

        self.itemDoubleClicked.connect(self._on_item_double_clicked)

    def set_logger(self, callback):
        self.log_callback = callback

    def log(self, message: str) -> None:
        if self.log_callback:
            self.log_callback(message)

    def set_search_query(self, query: str) -> None:
        self._search_query = query

    def bind_games(self, games: Dict[str, GameEntry]) -> None:
        self.games = games
        self.refresh()

    def resizeEvent(self, event):
        super().resizeEvent(event)
        self.setWrapping(True)
        self.setResizeMode(QListWidget.ResizeMode.Adjust)
        self.scheduleDelayedItemsLayout()
        self.viewport().update()

    def dragEnterEvent(self, event):
        if event.mimeData().hasUrls():
            event.setDropAction(Qt.DropAction.CopyAction)
            event.acceptProposedAction()
        else:
            event.ignore()

    def dragMoveEvent(self, event):
        if event.mimeData().hasUrls():
            event.setDropAction(Qt.DropAction.CopyAction)
            event.acceptProposedAction()
        else:
            event.ignore()

    def dropEvent(self, event):
        parent = self.window()
        accepted = False

        if event.mimeData().hasUrls():
            for url in event.mimeData().urls():
                file_path = url.toLocalFile()
                if file_path and Path(file_path).suffix.lower() in SUPPORTED_DROPS:
                    if hasattr(parent, "add_new_game"):
                        parent.add_new_game(file_path)
                        accepted = True

        if accepted:
            event.setDropAction(Qt.DropAction.CopyAction)
            event.acceptProposedAction()
        else:
            event.ignore()

    def _on_item_double_clicked(self, item: QListWidgetItem) -> None:
        parent = self.window()
        if hasattr(parent, "launch_item"):
            parent.launch_item(item)

    def current_game_id(self) -> Optional[str]:
        item = self.currentItem()
        return item.data(Qt.ItemDataRole.UserRole) if item else None

    def column_count(self) -> int:
        grid_width = max(1, self.gridSize().width())
        viewport_width = max(1, self.viewport().width())
        return max(1, viewport_width // grid_width)

    def move_selection_grid(self, dx: int = 0, dy: int = 0) -> None:
        count = self.count()
        if count == 0:
            return

        current = self.currentRow()
        if current < 0:
            self.setCurrentRow(0)
            return

        columns = self.column_count()
        new_index = current + dx + (dy * columns)
        new_index = max(0, min(count - 1, new_index))
        self.setCurrentRow(new_index)

        current_item = self.currentItem()
        if current_item:
            self.scrollToItem(current_item)

    def sorted_games(self) -> List[GameEntry]:
        games = list(self.games.values())
        if self._search_query:
            games = [g for g in games if self._search_query in g.name.lower()]
        games.sort(key=lambda g: (not g.favorite, g.name.lower()))
        return games

    def build_item_text(self, game: GameEntry) -> str:
        tags = []
        if game.favorite:
            tags.append("★ Favorite")

        rating = game.steam_rating
        # Normalise legacy "YYYY-MM-DD" stored dates to year-only
        rd = re.sub(r"^(\d{4})-\d{2}-\d{2}$", r"\1", game.release_date)
        date_parts = [p for p in (rating, rd) if p]
        if date_parts:
            tags.append(" • ".join(date_parts))

        if game.metadata_status == "loading":
            tags.append("Loading metadata")
        elif game.metadata_status == "not_found":
            tags.append("No Steam match")
        elif game.metadata_status == "error":
            tags.append("Metadata error")

        return f"{game.name}\n{' • '.join(tags)}" if tags else game.name

    def _load_scaled_pixmap(self, path: Path) -> QPixmap:
        """Pixmap cache keyed by (path, mtime); center-crop for custom images."""
        try:
            mtime = int(path.stat().st_mtime)
        except OSError:
            mtime = 0
        key = (str(path), mtime)
        cached = self._icon_cache.get(key)
        if cached is not None:
            return cached

        pixmap = QPixmap(str(path))
        if not pixmap.isNull():
            if abs(pixmap.width() / max(1, pixmap.height()) - CARD_WIDTH / CARD_HEIGHT) > 0.05:
                pixmap = pixmap.scaled(
                    CARD_WIDTH,
                    CARD_HEIGHT,
                    Qt.AspectRatioMode.KeepAspectRatioByExpanding,
                    Qt.TransformationMode.SmoothTransformation,
                )
                x = max(0, (pixmap.width() - CARD_WIDTH) // 2)
                y = max(0, (pixmap.height() - CARD_HEIGHT) // 2)
                pixmap = pixmap.copy(x, y, CARD_WIDTH, CARD_HEIGHT)
            else:
                pixmap = pixmap.scaled(
                    CARD_WIDTH,
                    CARD_HEIGHT,
                    Qt.AspectRatioMode.IgnoreAspectRatio,
                    Qt.TransformationMode.SmoothTransformation,
                )
        if len(self._icon_cache) > 512:
            self._icon_cache.clear()
        self._icon_cache[key] = pixmap
        return pixmap

    def invalidate_icon(self, path: str) -> None:
        self._icon_cache = {k: v for k, v in self._icon_cache.items() if k[0] != path}

    def icon_for_game(self, game: GameEntry) -> QIcon:
        image_path = Path(game.image) if game.image else Path(self.placeholder_path)
        if not image_path.exists():
            image_path = Path(self.placeholder_path)

        pixmap = self._load_scaled_pixmap(image_path)

        if pixmap.isNull() and Path(self.placeholder_path).exists():
            pixmap = self._load_scaled_pixmap(Path(self.placeholder_path))

        if pixmap.isNull():
            pixmap = QPixmap(CARD_WIDTH, CARD_HEIGHT)
            pixmap.fill(QColor("#303030"))

        return QIcon(pixmap)

    def refresh(self) -> None:
        selected_id = self.current_game_id()
        self.clear()

        for game in self.sorted_games():
            item = QListWidgetItem(self.icon_for_game(game), self.build_item_text(game))
            item.setData(Qt.ItemDataRole.UserRole, game.game_id)
            item.setSizeHint(QSize(CARD_WIDTH + 18, CARD_HEIGHT + GRID_EXTRA_HEIGHT))
            item.setTextAlignment(Qt.AlignmentFlag.AlignHCenter | Qt.AlignmentFlag.AlignTop)
            self.addItem(item)

            if selected_id and game.game_id == selected_id:
                self.setCurrentItem(item)

        if self.count() and self.currentRow() < 0:
            self.setCurrentRow(0)

        self.scheduleDelayedItemsLayout()
        self.viewport().update()


class MainWindow(QMainWindow):
    """Main app window."""

    def __init__(self):
        super().__init__()
        self.setWindowTitle(APP_NAME)
        self.setAcceptDrops(True)
        self._closing = False

        self.thread_pool = QThreadPool.globalInstance()
        self.thread_pool.setMaxThreadCount(MAX_WORKERS)

        self.repo = GameRepository(DATA_FILE, SETTINGS_FILE, CACHE_DIR, PLACEHOLDER_FILE)
        self.settings = self.repo.load_settings()
        self.games = self.repo.load_games()

        self.active_workers: List[MetadataFetchWorker] = []

        self.controller = None
        self.controller_enabled = bool(self.settings.get("controller_enabled", True))
        self.controller_ready = False
        self.last_hat = (0, 0)
        self.last_buttons = {}
        self.axis_latch_x = 0
        self.axis_latch_y = 0

        self._save_timer = QTimer(self)
        self._save_timer.setSingleShot(True)
        self._save_timer.timeout.connect(self._do_save)

        self.setup_ui()
        self.apply_dark_theme()
        self.game_list.bind_games(self.games)

        # Honor saved geometry unless it was never customized.
        self.resize(
            int(self.settings.get("window_width", 1280)),
            int(self.settings.get("window_height", 760)),
        )

        self.init_controller()

        if self.repo.corrupt_backup:
            self.log(
                f"Warning: games data was corrupt and backed up to {self.repo.corrupt_backup}. "
                "Library was reset."
            )
        QTimer.singleShot(0, lambda: self.log(f"Launcher ready — {len(self.games)} game(s) loaded."))

    def setup_ui(self):
        central = QWidget()
        central.setAcceptDrops(True)
        self.setCentralWidget(central)

        outer = QVBoxLayout(central)
        header = QHBoxLayout()

        self.title_label = QLabel("Drop .exe, .lnk, .bat, .cmd, or .url files here")
        self.title_label.setStyleSheet("font-size: 16px; font-weight: 600;")
        header.addWidget(self.title_label)

        self.add_button = QPushButton("Add Game")
        self.add_button.clicked.connect(self.pick_game_file)
        header.addWidget(self.add_button)

        self.launch_button = QPushButton("Launch Selected")
        self.launch_button.clicked.connect(self.launch_selected)
        header.addWidget(self.launch_button)

        outer.addLayout(header)

        self._search_query = ""
        search_layout = QHBoxLayout()
        self.search_box = QLineEdit()
        self.search_box.setPlaceholderText("Search games…")
        self.search_box.setClearButtonEnabled(True)
        self.search_box.setStyleSheet(
            "QLineEdit {"
            " background-color: #2a2a2a;"
            " border: 1px solid #555;"
            " border-radius: 6px;"
            " padding: 6px 10px;"
            " font-size: 14px;"
            " color: #e0e0e0;"
            "}"
            "QLineEdit:focus {"
            " border: 1px solid #9A3030;"
            "}"
        )
        self.search_box.textChanged.connect(self._on_search_changed)
        search_layout.addWidget(self.search_box)
        outer.addLayout(search_layout)

        self.game_list = GameLauncherList()
        self.game_list.set_logger(self.log)
        self.game_list.customContextMenuRequested.connect(self.show_context_menu)
        outer.addWidget(self.game_list, 1)

        self.log_box = QTextEdit()
        self.log_box.setReadOnly(True)
        self.log_box.setFixedHeight(66)
        outer.addWidget(self.log_box)

        self.build_actions()

    def build_actions(self):
        launch_action = QAction("Launch Selected", self)
        launch_action.setShortcut("Return")
        launch_action.triggered.connect(self.launch_selected)
        self.addAction(launch_action)

        fav_action = QAction("Toggle Favorite", self)
        fav_action.setShortcut("Ctrl+Shift+F")
        fav_action.triggered.connect(self.toggle_favorite_selected)
        self.addAction(fav_action)

        search_focus_action = QAction("Focus Search", self)
        search_focus_action.setShortcut("Ctrl+F")
        search_focus_action.triggered.connect(lambda: self.search_box.setFocus())
        self.addAction(search_focus_action)

        refresh_action = QAction("Refresh Layout", self)
        refresh_action.setShortcut("F5")
        refresh_action.triggered.connect(self.refresh_ui)
        self.addAction(refresh_action)

        delete_action = QAction("Delete Selected", self)
        delete_action.setShortcut(QKeySequence(QKeySequence.StandardKey.Delete))
        delete_action.triggered.connect(self.delete_selected)
        self.addAction(delete_action)

    def log(self, message: str) -> None:
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.log_box.append(f"[{timestamp}] {message}")

    def apply_dark_theme(self) -> None:
        app = QApplication.instance()
        if not app:
            return

        palette = QPalette()
        palette.setColor(QPalette.ColorRole.Window, QColor(30, 30, 30))
        palette.setColor(QPalette.ColorRole.WindowText, QColor(220, 220, 220))
        palette.setColor(QPalette.ColorRole.Base, QColor(20, 20, 20))
        palette.setColor(QPalette.ColorRole.AlternateBase, QColor(35, 35, 35))
        palette.setColor(QPalette.ColorRole.Text, QColor(230, 230, 230))
        palette.setColor(QPalette.ColorRole.Button, QColor(45, 45, 45))
        palette.setColor(QPalette.ColorRole.ButtonText, QColor(230, 230, 230))
        palette.setColor(QPalette.ColorRole.Highlight, QColor(80, 120, 200))
        palette.setColor(QPalette.ColorRole.HighlightedText, QColor(255, 255, 255))
        app.setPalette(palette)

    def normalize_name_from_path(self, file_path: str) -> str:
        stem = Path(file_path).stem
        stem = stem.replace("_", " ").replace("-", " ")
        stem = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", " ", stem)
        stem = re.sub(r"\s+", " ", stem).strip()
        return stem or "Unknown Game"

    def find_game_by_path(self, file_path: str) -> Optional[GameEntry]:
        resolved = str(Path(file_path).resolve()).lower()
        for game in self.games.values():
            try:
                if str(Path(game.path).resolve()).lower() == resolved:
                    return game
            except Exception:
                continue
        return None

    def ensure_placeholder(self) -> bool:
        if PLACEHOLDER_FILE.exists():
            return True
        QMessageBox.warning(
            self,
            "Missing placeholder.png",
            f"Place a file named placeholder.png next to this script:\n{PLACEHOLDER_FILE}",
        )
        self.log("placeholder.png is missing; fallback visuals may be blank.")
        return False

    def dragEnterEvent(self, event):
        if event.mimeData().hasUrls():
            event.setDropAction(Qt.DropAction.CopyAction)
            event.acceptProposedAction()
        else:
            event.ignore()

    def dragMoveEvent(self, event):
        if event.mimeData().hasUrls():
            event.setDropAction(Qt.DropAction.CopyAction)
            event.acceptProposedAction()
        else:
            event.ignore()

    def dropEvent(self, event):
        accepted = False
        if event.mimeData().hasUrls():
            for url in event.mimeData().urls():
                file_path = url.toLocalFile()
                if file_path and Path(file_path).suffix.lower() in SUPPORTED_DROPS:
                    self.add_new_game(file_path)
                    accepted = True

        if accepted:
            event.setDropAction(Qt.DropAction.CopyAction)
            event.acceptProposedAction()
        else:
            event.ignore()

    def pick_game_file(self):
        files, _ = QFileDialog.getOpenFileNames(
            self,
            "Choose game files",
            str(Path.home()),
            "Launch Files (*.exe *.lnk *.bat *.cmd *.url)",
        )
        for file_path in files:
            self.add_new_game(file_path)

    def add_new_game(self, file_path: str):
        file_path = str(Path(file_path))
        suffix = Path(file_path).suffix.lower()

        if suffix not in SUPPORTED_DROPS:
            self.log(f"Unsupported file type skipped: {file_path}")
            return

        parameters = ""
        if suffix == ".lnk":
            target, args, _workdir = self._resolve_lnk(Path(file_path))
            if target:
                file_path = target
                parameters = args
                self.log(f"Resolved shortcut to: {file_path}")

        existing = self.find_game_by_path(file_path)
        if existing:
            self.log(f"Game already exists: {existing.name}")
            return

        entry = GameEntry(
            game_id=uuid.uuid4().hex,
            name=self.normalize_name_from_path(file_path),
            path=file_path,
            image=str(PLACEHOLDER_FILE),
            metadata_status="loading",
            image_source="placeholder",
            added_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            parameters=parameters,
        )

        self.games[entry.game_id] = entry
        self.save_all()
        self.refresh_ui()
        self.log(f"Added game: {entry.name}")
        self.log(f"Total games in launcher: {len(self.games)}")
        self.fetch_metadata_for_game(entry.game_id)

    def fetch_metadata_for_game(self, game_id: str):
        game = self.games.get(game_id)
        if not game:
            return

        game.metadata_status = "loading"
        self.refresh_ui()

        worker = MetadataFetchWorker(game_id, game.name, game.path, CACHE_DIR)
        worker.signals.finished.connect(self.on_metadata_finished)
        worker.signals.failed.connect(self.on_metadata_failed)

        self.active_workers.append(worker)
        self.thread_pool.start(worker)
        self.log(f"Fetching metadata for {game.name}...")

    def remove_finished_worker(self, game_id: str):
        self.active_workers = [w for w in self.active_workers if getattr(w, "game_id", None) != game_id]

    def on_metadata_finished(self, game_id: str, payload: dict):
        self.remove_finished_worker(game_id)
        if self._closing:
            return

        game = self.games.get(game_id)
        if not game:
            return

        game.path = payload.get("path", game.path)
        game.steam_id = payload.get("steam_id", game.steam_id)
        game.release_date = payload.get("release_date", game.release_date)
        game.steam_rating = payload.get("steam_rating", game.steam_rating)
        game.metadata_status = payload.get("metadata_status", game.metadata_status)

        image_path = payload.get("image", "")
        if game.image_source == "custom":
            pass  # Never overwrite a user-set custom image during metadata refresh
        elif image_path and Path(image_path).exists():
            if game.image != image_path:
                self.game_list.invalidate_icon(game.image)
            game.image = image_path
            game.image_source = payload.get("image_source", "steam")
        else:
            game.image = str(PLACEHOLDER_FILE)
            game.image_source = "placeholder"

        self.save_all()
        self.refresh_ui()
        self.log(f"Metadata updated for {game.name} ({game.metadata_status}).")

    def on_metadata_failed(self, game_id: str, error_message: str):
        self.remove_finished_worker(game_id)
        if self._closing:
            return

        game = self.games.get(game_id)
        if game:
            game.metadata_status = "error"
            game.image = str(PLACEHOLDER_FILE)
            game.image_source = "placeholder"

        self.save_all()
        self.refresh_ui()
        self.log(error_message)

    def save_all(self):
        """Debounced save; cache cleanup only happens on exit."""
        self.settings["window_width"] = self.width()
        self.settings["window_height"] = self.height()
        self.settings["controller_enabled"] = self.controller_enabled
        self._save_timer.start(SAVE_DEBOUNCE_MS)

    def _do_save(self):
        self.repo.save_games(self.games)
        self.repo.save_settings(self.settings)

    def _on_search_changed(self, text: str) -> None:
        self._search_query = text.strip().lower()
        self.game_list.set_search_query(self._search_query)
        self.refresh_ui()

    def refresh_ui(self):
        self.game_list.bind_games(self.games)
        query = self._search_query if hasattr(self, "_search_query") else ""
        total = len(self.games)
        visible = sum(1 for g in self.games.values() if not query or query in g.name.lower())
        if query:
            self.title_label.setText(f'{visible} of {total} games • "{query}"')
        else:
            self.title_label.setText(f"{total} games • Favorites first • A-Z")

    def launch_item(self, item: QListWidgetItem):
        if not item:
            return
        game_id = item.data(Qt.ItemDataRole.UserRole)
        self.launch_game_by_id(game_id)

    def launch_selected(self):
        item = self.game_list.currentItem()
        if not item:
            self.log("No game selected.")
            return
        self.launch_item(item)

    def _resolve_lnk(self, path: Path) -> Tuple[str, str, str]:
        """Resolve a .lnk shortcut via Windows Script Host COM."""
        try:
            import win32com.client
            shell = win32com.client.Dispatch("WScript.Shell")
            shortcut = shell.CreateShortCut(str(path))
            return (
                str(shortcut.Targetpath or ""),
                str(shortcut.Arguments or ""),
                str(shortcut.WorkingDirectory or ""),
            )
        except Exception:
            return "", "", ""

    def _shell_execute_open(self, file_path: str, parameters: str = "",
                            working_dir: Optional[str] = None, verb: str = "open") -> bool:
        """Launch through ShellExecuteW (handles shell associations and elevation)."""
        try:
            result = ctypes.windll.shell32.ShellExecuteW(
                None,
                verb,
                file_path,
                parameters if parameters else None,
                working_dir if working_dir else None,
                1,
            )
            return result > 32
        except Exception:
            return False

    def _launch_windows_target(self, original_path: Path, game_params: str = "") -> bool:
        """Windows-aware launcher for .lnk/.url/.bat/.cmd/.exe with elevation fallback."""
        suffix = original_path.suffix.lower()

        if suffix == ".lnk":
            target, args, workdir = self._resolve_lnk(original_path)
            if not target:
                return self._shell_execute_open(str(original_path))
            target_path = Path(target)
            working_dir = workdir or str(target_path.parent)
            if args:
                # Raw arg string: ShellExecute passes it verbatim, avoiding
                # tokenization bugs across different game launchers.
                return self._shell_execute_open(target, args, working_dir, "open")
            try:
                subprocess.Popen([target], cwd=working_dir, shell=False)
                return True
            except OSError as exc:
                verb = "runas" if getattr(exc, "winerror", None) == 740 else "open"
                return self._shell_execute_open(target, args, working_dir, verb)
            except Exception:
                return self._shell_execute_open(target, args, working_dir, "open")

        if suffix == ".url":
            return self._shell_execute_open(str(original_path))

        if suffix in {".bat", ".cmd"}:
            try:
                subprocess.Popen(
                    ["cmd.exe", "/c", str(original_path)],
                    cwd=str(original_path.parent),
                    shell=False,
                )
                return True
            except OSError as exc:
                verb = "runas" if getattr(exc, "winerror", None) == 740 else "open"
                return self._shell_execute_open(
                    str(original_path), working_dir=str(original_path.parent), verb=verb
                )

        # Standard executable. Windows command lines are not POSIX shell
        # strings — shlex would mangle quoting/backslashes, so tokenize with
        # Windows rules and fall back to ShellExecute with the raw string.
        if game_params:
            try:
                import shlex
                cmd = [str(original_path)] + shlex.split(game_params, posix=False)
                cmd = [c.strip('"') for c in cmd]
                subprocess.Popen(cmd, cwd=str(original_path.parent), shell=False)
                return True
            except OSError as exc:
                verb = "runas" if getattr(exc, "winerror", None) == 740 else "open"
                return self._shell_execute_open(
                    str(original_path), parameters=game_params,
                    working_dir=str(original_path.parent), verb=verb,
                )
            except Exception:
                return self._shell_execute_open(
                    str(original_path), parameters=game_params,
                    working_dir=str(original_path.parent), verb="open",
                )
        try:
            subprocess.Popen([str(original_path)], cwd=str(original_path.parent), shell=False)
            return True
        except OSError as exc:
            verb = "runas" if getattr(exc, "winerror", None) == 740 else "open"
            return self._shell_execute_open(
                str(original_path), working_dir=str(original_path.parent), verb=verb
            )
        except Exception:
            return self._shell_execute_open(
                str(original_path), working_dir=str(original_path.parent), verb="open"
            )

    def launch_game_by_id(self, game_id: str):
        game = self.games.get(game_id)
        if not game:
            return

        path = Path(game.path)
        if not path.exists():
            QMessageBox.warning(self, "Missing game", f"Launch target not found:\n{game.path}")
            self.log(f"Launch failed; file missing: {game.path}")
            return

        try:
            launched = self._launch_windows_target(path, game.parameters)
            if not launched:
                raise RuntimeError("ShellExecute/subprocess launch returned failure.")

            game.last_played = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.save_all()
            self.refresh_ui()

            if hasattr(self, "controller_timer") and self.controller_timer.isActive():
                self.controller_timer.stop()
                self.log("Controller input paused.")

            self.log(f"Launched: {game.name}")
        except Exception as exc:
            self.log(f"Launch failed for {game.name}: {exc}")
            QMessageBox.critical(self, "Launch failed", f"Could not launch {game.name}.\n\n{exc}")

    def browse_to_path(self, game: GameEntry):
        """Open Explorer and select the game's launch target."""
        if not game or not game.path:
            self.log("No path available for selected game.")
            return

        target = Path(game.path)
        if not target.exists():
            self.log(f"Browse failed; file missing: {game.path}")
            QMessageBox.warning(self, "Path Missing", f"Launch target not found:\n{game.path}")
            return

        try:
            subprocess.Popen(["explorer", "/select,", str(target)])
            self.log(f"Opened Explorer for {game.name}")
        except Exception as exc:
            self.log(f"Failed to open Explorer for {game.name}: {exc}")
            QMessageBox.warning(
                self,
                "Browse to Path Failed",
                f"Could not open Explorer for:\n{game.path}\n\n{exc}",
            )

    def open_steam_page(self, game: GameEntry):
        if not game or not game.steam_id:
            self.log(f"No Steam page available for {game.name if game else 'selected game'}.")
            return

        url = f"https://store.steampowered.com/app/{game.steam_id}/"
        try:
            webbrowser.open(url)
            self.log(f"Opened Steam page for {game.name}")
        except Exception as exc:
            self.log(f"Failed to open Steam page for {game.name}: {exc}")
            QMessageBox.warning(self, "Open Steam Page Failed", f"Could not open:\n{url}\n\n{exc}")

    def get_selected_game(self) -> Optional[GameEntry]:
        game_id = self.game_list.current_game_id()
        return self.games.get(game_id) if game_id else None

    def show_context_menu(self, pos):
        item = self.game_list.itemAt(pos)
        if item:
            self.game_list.setCurrentItem(item)

        game = self.get_selected_game()
        if not game:
            return

        menu = QMenu(self)
        launch_act = menu.addAction("Launch")
        browse_act = menu.addAction("Browse to Path")
        steam_act = menu.addAction("Open Steam Page")
        rename_act = menu.addAction("Rename")
        favorite_act = menu.addAction("Unfavorite" if game.favorite else "Favorite")
        redo_act = menu.addAction("Redo Metadata")
        placeholder_act = menu.addAction("Assign placeholder.png")
        image_act = menu.addAction("Set Custom Image")
        params_act = menu.addAction("Set Launch Parameters")
        delete_act = menu.addAction("Delete")

        if not game.steam_id:
            steam_act.setEnabled(False)

        chosen = menu.exec(self.game_list.mapToGlobal(pos))

        if chosen == launch_act:
            self.launch_selected()
        elif chosen == browse_act:
            self.browse_to_path(game)
        elif chosen == steam_act:
            self.open_steam_page(game)
        elif chosen == rename_act:
            self.rename_selected()
        elif chosen == favorite_act:
            self.toggle_favorite_selected()
        elif chosen == redo_act:
            self.fetch_metadata_for_game(game.game_id)
        elif chosen == placeholder_act:
            self.game_list.invalidate_icon(game.image)
            game.image = str(PLACEHOLDER_FILE)
            game.image_source = "placeholder"
            self.save_all()
            self.refresh_ui()
            self.log(f"Assigned placeholder.png to {game.name}")
        elif chosen == image_act:
            self.set_custom_image_selected()
        elif chosen == params_act:
            self.set_launch_parameters_selected(game)
        elif chosen == delete_act:
            self.delete_selected()

    def rename_selected(self):
        game = self.get_selected_game()
        if not game:
            return

        new_name, ok = QInputDialog.getText(self, "Rename Game", "New game name:", text=game.name)
        if not ok:
            return

        new_name = re.sub(r"\s+", " ", new_name).strip()
        if not new_name:
            self.log("Rename cancelled: empty name.")
            return

        game.name = new_name
        self.save_all()
        self.refresh_ui()
        self.log(f"Renamed game to: {new_name}")

    def toggle_favorite_selected(self):
        game = self.get_selected_game()
        if not game:
            return

        game.favorite = not game.favorite
        self.save_all()
        self.refresh_ui()
        self.log(f"{'Favorited' if game.favorite else 'Unfavorited'}: {game.name}")

    def set_custom_image_selected(self):
        game = self.get_selected_game()
        if not game:
            return

        file_path, _ = QFileDialog.getOpenFileName(
            self,
            "Choose image",
            str(Path.home()),
            "Images (*.png *.jpg *.jpeg *.bmp *.webp)",
        )
        if not file_path or not Path(file_path).exists():
            if file_path:
                self.log("Custom image not found.")
            return

        # Copy into launcher cache so removing the source doesn't break it.
        safe_id = hashlib.md5(game.game_id.encode()).hexdigest()[:16]
        cached_img = self.repo.cache_dir / f"custom_{safe_id}{Path(file_path).suffix.lower()}"
        old_image = game.image
        try:
            shutil.copy2(file_path, cached_img)
            game.image = str(cached_img)
        except Exception as exc:
            self.log(f"Could not cache custom image, using source path: {exc}")
            game.image = file_path
        self.game_list.invalidate_icon(old_image)
        game.image_source = "custom"
        self.save_all()
        self.refresh_ui()
        self.log(f"Custom image assigned to {game.name}")

    def set_launch_parameters_selected(self, game: GameEntry):
        text, ok = QInputDialog.getText(
            self,
            "Set Launch Parameters",
            f"Enter launch parameters for {game.name}:\n(e.g., -directx11 -fullscreen)",
            text=game.parameters,
        )
        if ok:
            game.parameters = text.strip()
            self.save_all()
            self.log(f"Updated parameters for {game.name}: {game.parameters}")

    def delete_selected(self):
        game = self.get_selected_game()
        if not game:
            return

        answer = QMessageBox.question(self, "Delete Game", f"Remove {game.name} from the launcher?")
        if answer != QMessageBox.StandardButton.Yes:
            return

        removed_image = game.image
        del self.games[game.game_id]

        try:
            if (
                removed_image
                and Path(removed_image).exists()
                and Path(removed_image).resolve().parent == CACHE_DIR.resolve()
            ):
                Path(removed_image).unlink(missing_ok=True)
        except Exception:
            pass

        self.save_all()
        self.refresh_ui()
        self.log(f"Deleted: {game.name}")
        self.log(f"Total games in launcher: {len(self.games)}")

    def init_controller(self):
        if not self.controller_enabled:
            self.log("Controller support disabled in settings.")
            return
        if pygame is None:
            self.log("pygame not installed; controller support unavailable.")
            return

        try:
            pygame.init()
            pygame.joystick.init()

            self.controller_timer = QTimer(self)
            self.controller_timer.timeout.connect(self.poll_controller)
            self.controller_timer.start(120)

            self._try_connect_controller()
        except Exception as exc:
            self.log(f"Controller init failed: {exc}")

    def _try_connect_controller(self) -> bool:
        """(Re)connect to the first available joystick; supports hot-plug."""
        try:
            pygame.joystick.quit()
            pygame.joystick.init()
            if pygame.joystick.get_count() < 1:
                if self.controller_ready:
                    self.log("Controller disconnected.")
                self.controller = None
                self.controller_ready = False
                return False
            self.controller = pygame.joystick.Joystick(0)
            self.controller.init()
            if not self.controller_ready:
                self.log(f"Controller connected: {self.controller.get_name()}")
            self.controller_ready = True
            self.last_hat = (0, 0)
            self.axis_latch_x = 0
            self.axis_latch_y = 0
            self.last_buttons = {}
            return True
        except Exception as exc:
            self.log(f"Controller connect failed: {exc}")
            self.controller = None
            self.controller_ready = False
            return False

    def _controller_should_process(self) -> bool:
        """Only process input when the launcher is active, visible, not minimized."""
        if not self.isVisible():
            return False
        if self.isMinimized():
            return False
        if not self.isActiveWindow():
            return False
        return True

    def poll_controller(self):
        if pygame is None:
            return

        try:
            pygame.event.pump()
        except Exception as exc:
            self.log(f"Controller poll error: {exc}")
            self.controller_ready = False
            return

        # Hot-plug: re-enumerate if no pad is currently usable.
        if not self.controller_ready or self.controller is None:
            self._try_connect_controller()
            if not self.controller_ready:
                return

        if not self._controller_should_process():
            # Reset latches so holding a stick while alt-tabbed doesn't fire
            # a move the instant focus returns.
            self.last_hat = (0, 0)
            self.axis_latch_x = 0
            self.axis_latch_y = 0
            self.last_buttons = {}
            return

        try:
            hat = self.controller.get_hat(0) if self.controller.get_numhats() > 0 else (0, 0)
            if hat != self.last_hat:
                if hat[0] == 1:
                    self.game_list.move_selection_grid(dx=1, dy=0)
                elif hat[0] == -1:
                    self.game_list.move_selection_grid(dx=-1, dy=0)
                elif hat[1] == 1:
                    self.game_list.move_selection_grid(dx=0, dy=-1)
                elif hat[1] == -1:
                    self.game_list.move_selection_grid(dx=0, dy=1)
                self.last_hat = hat

            axis_x = self.controller.get_axis(0) if self.controller.get_numaxes() > 0 else 0.0
            axis_y = self.controller.get_axis(1) if self.controller.get_numaxes() > 1 else 0.0

            if axis_x > 0.75 and self.axis_latch_x != 1:
                self.game_list.move_selection_grid(dx=1, dy=0)
                self.axis_latch_x = 1
            elif axis_x < -0.75 and self.axis_latch_x != -1:
                self.game_list.move_selection_grid(dx=-1, dy=0)
                self.axis_latch_x = -1
            elif -0.35 < axis_x < 0.35:
                self.axis_latch_x = 0

            if axis_y > 0.75 and self.axis_latch_y != 1:
                self.game_list.move_selection_grid(dx=0, dy=1)
                self.axis_latch_y = 1
            elif axis_y < -0.75 and self.axis_latch_y != -1:
                self.game_list.move_selection_grid(dx=0, dy=-1)
                self.axis_latch_y = -1
            elif -0.35 < axis_y < 0.35:
                self.axis_latch_y = 0

            mappings = {
                0: self.launch_selected,          # A
                1: lambda: None,                  # B
                2: self.toggle_favorite_selected, # X
            }
            for button_idx, action in mappings.items():
                if button_idx < self.controller.get_numbuttons():
                    pressed = self.controller.get_button(button_idx)
                    last = self.last_buttons.get(button_idx, 0)
                    if pressed and not last:
                        action()
                    self.last_buttons[button_idx] = pressed
        except Exception as exc:
            self.log(f"Controller poll error: {exc}")
            self.controller_ready = False

    def changeEvent(self, event):
        """Restart controller polling when the window regains focus."""
        if event.type() == QEvent.Type.ActivationChange:
            if self.isActiveWindow():
                if getattr(self, "controller_ready", False):
                    if hasattr(self, "controller_timer") and not self.controller_timer.isActive():
                        self.last_hat = (0, 0)
                        self.axis_latch_x = 0
                        self.axis_latch_y = 0
                        self.last_buttons = {}
                        self.controller_timer.start(120)
                        self.log("Controller input resumed.")
        super().changeEvent(event)

    def closeEvent(self, event):
        self._closing = True
        if self._save_timer.isActive():
            self._save_timer.stop()
        self._do_save()

        removed = self.repo.cleanup_unused_cache(self.games)
        if removed:
            self.log(f"Removed {len(removed)} unused cached image(s).")

        # Give in-flight metadata workers a moment to finish rather than
        # emitting signals into a dying window.
        self.thread_pool.waitForDone(1500)

        if pygame is not None:
            try:
                pygame.quit()
            except Exception:
                pass

        super().closeEvent(event)


def main():
    app = QApplication(sys.argv)
    window = MainWindow()
    window.ensure_placeholder()
    window.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    try:
        main()
    except Exception:
        traceback.print_exc()
