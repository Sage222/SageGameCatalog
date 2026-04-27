import sys
import json
import re
import subprocess
import traceback
import webbrowser
import logging
import time
from collections import deque
from dataclasses import dataclass, asdict, field
from datetime import datetime
from pathlib import Path
from threading import Lock
from typing import Optional, Dict, List, Tuple

import requests
from bs4 import BeautifulSoup

from PyQt6.QtCore import Qt, QSize, QObject, pyqtSignal, QRunnable, QThreadPool, QTimer
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
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

APP_NAME = "Sage Game Archive Catalog 1.2"
BASE_DIR = Path(__file__).resolve().parent
CACHE_DIR = BASE_DIR / "launcher_cache"
DATA_FILE = BASE_DIR / "games_data.json"
SETTINGS_FILE = BASE_DIR / "launcher_settings.json"
PLACEHOLDER_FILE = BASE_DIR / "placeholder.png"
LOGS_DIR = BASE_DIR / "logs"
LOG_FILE = LOGS_DIR / "archive_catalog.log"

CARD_WIDTH = 300
CARD_HEIGHT = 170
GRID_EXTRA_HEIGHT = 104
GRID_SPACING = 12
MAX_WORKERS = 3
REQUEST_INTERVAL_SECONDS = 1.4


def setup_logging() -> logging.Logger:
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("sage_archive_catalog")
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    return logger


LOGGER = setup_logging()


class SteamRateLimiter:
    def __init__(self, min_interval: float = REQUEST_INTERVAL_SECONDS):
        self.min_interval = float(min_interval)
        self.lock = Lock()
        self.last_request_at = 0.0

    def wait(self):
        with self.lock:
            now = time.monotonic()
            elapsed = now - self.last_request_at
            if elapsed < self.min_interval:
                time.sleep(self.min_interval - elapsed)
            self.last_request_at = time.monotonic()


STEAM_LIMITER = SteamRateLimiter()


@dataclass
class GameEntry:
    game_id: str
    name: str
    path: str
    image: str
    steam_id: str = ""
    release_date: str = ""
    tags: List[str] = field(default_factory=list)
    summary: str = ""
    recent_reviews: str = ""
    all_reviews: str = ""
    favorite: bool = False
    metadata_status: str = "pending"
    image_source: str = "placeholder"
    added_at: str = ""
    modified_at: str = ""

    @classmethod
    def from_dict(cls, data: dict) -> "GameEntry":
        return cls(
            game_id=str(data.get("game_id", "")),
            name=str(data.get("name", "Unknown Game")),
            path=str(data.get("path", "")),
            image=str(data.get("image", str(PLACEHOLDER_FILE))),
            steam_id=str(data.get("steam_id", "")),
            release_date=str(data.get("release_date", "")),
            tags=list(data.get("tags", []) or []),
            summary=str(data.get("summary", "")),
            recent_reviews=str(data.get("recent_reviews", "")),
            all_reviews=str(data.get("all_reviews", "")),
            favorite=bool(data.get("favorite", False)),
            metadata_status=str(data.get("metadata_status", "pending")),
            image_source=str(data.get("image_source", "placeholder")),
            added_at=str(data.get("added_at", "")),
            modified_at=str(data.get("modified_at", "")),
        )


class GameRepository:
    def __init__(self, data_file: Path, settings_file: Path, cache_dir: Path, placeholder_file: Path):
        self.data_file = data_file
        self.settings_file = settings_file
        self.cache_dir = cache_dir
        self.placeholder_file = placeholder_file
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def load_games(self) -> Dict[str, GameEntry]:
        if not self.data_file.exists():
            return {}
        try:
            with self.data_file.open("r", encoding="utf-8") as fh:
                raw = json.load(fh)
            games = {}
            for game_id, payload in raw.items():
                payload.setdefault("game_id", game_id)
                game = GameEntry.from_dict(payload)
                if not game.image:
                    game.image = str(self.placeholder_file)
                games[game.game_id] = game
            return games
        except Exception:
            LOGGER.exception("Failed to load games data")
            return {}

    def save_games(self, games: Dict[str, GameEntry]) -> None:
        serializable = {gid: asdict(game) for gid, game in games.items()}
        with self.data_file.open("w", encoding="utf-8") as fh:
            json.dump(serializable, fh, indent=2, ensure_ascii=False)

    def load_settings(self) -> dict:
        default = {
            "window_width": 1280,
            "window_height": 760,
            "last_library_folder": str(Path.home()),
            "auto_fetch_on_import": True,
        }
        if not self.settings_file.exists():
            return default
        try:
            with self.settings_file.open("r", encoding="utf-8") as fh:
                data = json.load(fh)
            default.update(data)
            return default
        except Exception:
            LOGGER.exception("Failed to load settings")
            return default

    def save_settings(self, settings: dict) -> None:
        with self.settings_file.open("w", encoding="utf-8") as fh:
            json.dump(settings, fh, indent=2, ensure_ascii=False)

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
                LOGGER.exception("Failed to clean cache file %s", file)
        return removed


class MetadataSignals(QObject):
    finished = pyqtSignal(str, dict)
    failed = pyqtSignal(str, str)


class MetadataFetchWorker(QRunnable):
    def __init__(self, game_id: str, game_name: str, folder_path: str, cache_dir: Path):
        super().__init__()
        self.game_id = game_id
        self.game_name = game_name
        self.folder_path = folder_path
        self.cache_dir = cache_dir
        self.signals = MetadataSignals()

    def sanitize_filename(self, value: str) -> str:
        return re.sub(r"[^A-Za-z0-9._-]", "_", value)[:100] or "game"

    def normalize_release_date(self, raw_date: str) -> str:
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
                if fmt == "%Y":
                    return f"{dt.year:04d}-01-01"
                if fmt in {"%b %Y", "%B %Y"}:
                    return f"{dt.year:04d}-{dt.month:02d}-01"
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                continue
        return raw_date

    def steam_get(self, session: requests.Session, url: str, **kwargs):
        STEAM_LIMITER.wait()
        LOGGER.info("Steam request: %s %s", url, kwargs.get("params", {}))
        response = session.get(url, timeout=15, **kwargs)
        response.raise_for_status()
        return response

    def extract_tags(self, html: str) -> List[str]:
        soup = BeautifulSoup(html, "html.parser")
        tags = []
        for element in soup.select("a.app_tag"):
            value = re.sub(r"\s+", " ", element.get_text(" ", strip=True)).strip()
            if value and value not in tags:
                tags.append(value)
        return tags[:8]

    def extract_summary(self, soup: BeautifulSoup) -> str:
        node = soup.select_one("div.game_description_snippet")
        if not node:
            return ""
        return re.sub(r"\s+", " ", node.get_text(" ", strip=True)).strip()

    def extract_reviews(self, soup: BeautifulSoup) -> Tuple[str, str]:
        recent_reviews = ""
        all_reviews = ""

        rows = soup.select("a.user_reviews_summary_row")
        for row in rows:
            subtitle = row.select_one("div.subtitle")
            summary = row.select_one("span.game_review_summary")
            count = row.select_one("span.responsive_hidden")

            if not subtitle or not summary:
                continue

            label = re.sub(r"\s+", " ", subtitle.get_text(" ", strip=True)).strip().rstrip(":")
            rating = re.sub(r"\s+", " ", summary.get_text(" ", strip=True)).strip()
            review_count = re.sub(r"\s+", " ", count.get_text(" ", strip=True)).strip() if count else ""

            combined = label + ":"
            if rating:
                combined += f"\n{rating}"
            if review_count:
                combined += f" {review_count}"

            lowered = label.lower()
            if lowered.startswith("recent"):
                recent_reviews = combined
            elif "english" in lowered or "all" in lowered or "overall" in lowered:
                if not all_reviews:
                    all_reviews = combined

        return recent_reviews, all_reviews

    def run(self) -> None:
        try:
            with requests.Session() as session:
                session.headers.update({
                    "User-Agent": "SageGameArchiveCatalog/1.2",
                    "Accept-Language": "en-US,en;q=0.9",
                })

                search_res = self.steam_get(
                    session,
                    "https://store.steampowered.com/api/storesearch/",
                    params={"term": self.game_name, "l": "english", "cc": "US"},
                )
                search_json = search_res.json()
                items = search_json.get("items", []) if isinstance(search_json, dict) else []

                if not items:
                    self.signals.finished.emit(
                        self.game_id,
                        {
                            "path": self.folder_path,
                            "image": "",
                            "steam_id": "",
                            "release_date": "",
                            "tags": [],
                            "summary": "",
                            "recent_reviews": "",
                            "all_reviews": "",
                            "metadata_status": "not_found",
                            "image_source": "placeholder",
                        },
                    )
                    return

                first = items[0]
                app_id = str(first.get("id", ""))

                details_res = self.steam_get(
                    session,
                    "https://store.steampowered.com/api/appdetails",
                    params={"appids": app_id, "l": "english", "cc": "US"},
                )
                details_json = details_res.json()

                release_date = ""
                if details_json.get(app_id, {}).get("success"):
                    raw_rel = details_json[app_id].get("data", {}).get("release_date", {}).get("date", "")
                    release_date = self.normalize_release_date(raw_rel)

                page_res = self.steam_get(
                    session,
                    f"https://store.steampowered.com/app/{app_id}/",
                    params={"l": "english", "cc": "US"},
                )
                soup = BeautifulSoup(page_res.text, "html.parser")
                tags = self.extract_tags(page_res.text)
                summary = self.extract_summary(soup)
                recent_reviews, all_reviews = self.extract_reviews(soup)

                image_path = ""
                image_source = "placeholder"
                if app_id:
                    img_url = f"https://cdn.akamai.steamstatic.com/steam/apps/{app_id}/header.jpg"
                    img_res = self.steam_get(session, img_url)
                    if img_res.ok and img_res.content:
                        safe_name = self.sanitize_filename(f"{self.game_name}_{app_id}")
                        image_file = self.cache_dir / f"{safe_name}.jpg"
                        image_file.write_bytes(img_res.content)
                        image_path = str(image_file)
                        image_source = "steam"

                self.signals.finished.emit(
                    self.game_id,
                    {
                        "path": self.folder_path,
                        "image": image_path,
                        "steam_id": app_id,
                        "release_date": release_date,
                        "tags": tags,
                        "summary": summary,
                        "recent_reviews": recent_reviews,
                        "all_reviews": all_reviews,
                        "metadata_status": "ready" if app_id else "not_found",
                        "image_source": image_source,
                    },
                )
        except Exception as exc:
            LOGGER.exception("Metadata fetch failed for %s", self.game_name)
            self.signals.failed.emit(self.game_id, f"Metadata fetch failed for '{self.game_name}': {exc}")


class GameCatalogList(QListWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.log_callback = None
        self.games: Dict[str, GameEntry] = {}
        self.placeholder_path = str(PLACEHOLDER_FILE)

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
                folder_path = url.toLocalFile()
                if folder_path and Path(folder_path).is_dir() and hasattr(parent, "import_library_folder"):
                    parent.import_library_folder(folder_path)
                    accepted = True
        if accepted:
            event.setDropAction(Qt.DropAction.CopyAction)
            event.acceptProposedAction()
        else:
            event.ignore()

    def _on_item_double_clicked(self, item: QListWidgetItem) -> None:
        parent = self.window()
        if hasattr(parent, "open_selected_folder"):
            parent.open_selected_folder()

    def current_game_id(self) -> Optional[str]:
        item = self.currentItem()
        return item.data(Qt.ItemDataRole.UserRole) if item else None

    def sorted_games(self) -> List[GameEntry]:
        games = list(self.games.values())
        games.sort(key=lambda g: (not g.favorite, g.name.lower()))
        return games

    def build_item_text(self, game: GameEntry) -> str:
        lines = []
        if game.favorite:
            lines.append("★ Favorite")
        if game.release_date:
            lines.append(game.release_date)
        if game.tags:
            lines.append(", ".join(game.tags[:3]))
        if game.recent_reviews:
            compact = game.recent_reviews.replace("\n", " ")
            lines.append(compact)
        if game.metadata_status == "loading":
            lines.append("Loading metadata")
        elif game.metadata_status == "queued":
            lines.append("Queued")
        elif game.metadata_status == "not_found":
            lines.append("No Steam match")
        elif game.metadata_status == "error":
            lines.append("Metadata error")
        return f"{game.name}\n{' • '.join(lines)}" if lines else game.name

    def icon_for_game(self, game: GameEntry) -> QIcon:
        image_path = Path(game.image) if game.image else Path(self.placeholder_path)
        if not image_path.exists():
            image_path = Path(self.placeholder_path)
        pixmap = QPixmap(str(image_path))
        if pixmap.isNull() and Path(self.placeholder_path).exists():
            pixmap = QPixmap(str(Path(self.placeholder_path)))
        if pixmap.isNull():
            pixmap = QPixmap(CARD_WIDTH, CARD_HEIGHT)
            pixmap.fill(QColor("#303030"))
        scaled = pixmap.scaled(
            CARD_WIDTH,
            CARD_HEIGHT,
            Qt.AspectRatioMode.IgnoreAspectRatio,
            Qt.TransformationMode.SmoothTransformation,
        )
        return QIcon(scaled)

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
    def __init__(self):
        super().__init__()
        self.setWindowTitle(APP_NAME)
        self.setAcceptDrops(True)

        self.thread_pool = QThreadPool.globalInstance()
        self.thread_pool.setMaxThreadCount(MAX_WORKERS)

        self.repo = GameRepository(DATA_FILE, SETTINGS_FILE, CACHE_DIR, PLACEHOLDER_FILE)
        self.settings = self.repo.load_settings()
        self.games = self.repo.load_games()

        self.active_workers: List[MetadataFetchWorker] = []
        self.pending_fetch_queue = deque()

        self.setup_ui()
        self.apply_dark_theme()
        self.game_list.bind_games(self.games)

        self.resize(
            int(self.settings.get("window_width", 1280)),
            int(self.settings.get("window_height", 760)),
        )

        self.refresh_timer = QTimer(self)
        self.refresh_timer.setSingleShot(True)
        self.refresh_timer.timeout.connect(self.start_pending_fetches)
        self.log("Archive catalog ready.")

    def setup_ui(self):
        central = QWidget()
        central.setAcceptDrops(True)
        self.setCentralWidget(central)

        outer = QVBoxLayout(central)
        header = QHBoxLayout()

        self.title_label = QLabel("Choose or drop a parent folder that contains archived game folders")
        self.title_label.setStyleSheet("font-size: 16px; font-weight: 600;")
        header.addWidget(self.title_label)

        self.add_button = QPushButton("Add Archive Folder")
        self.add_button.clicked.connect(self.pick_library_folder)
        header.addWidget(self.add_button)

        self.open_button = QPushButton("Open Selected Folder")
        self.open_button.clicked.connect(self.open_selected_folder)
        header.addWidget(self.open_button)

        outer.addLayout(header)

        self.game_list = GameCatalogList()
        self.game_list.set_logger(self.log)
        self.game_list.customContextMenuRequested.connect(self.show_context_menu)
        outer.addWidget(self.game_list, 1)

        self.log_box = QTextEdit()
        self.log_box.setReadOnly(True)
        self.log_box.setFixedHeight(120)
        outer.addWidget(self.log_box)

        self.build_actions()

    def build_actions(self):
        details_action = QAction("View Details", self)
        details_action.setShortcut("Ctrl+D")
        details_action.triggered.connect(self.show_details_selected)
        self.addAction(details_action)

        open_action = QAction("Open Selected Folder", self)
        open_action.setShortcut("Return")
        open_action.triggered.connect(self.open_selected_folder)
        self.addAction(open_action)

        fav_action = QAction("Toggle Favorite", self)
        fav_action.setShortcut("Ctrl+F")
        fav_action.triggered.connect(self.toggle_favorite_selected)
        self.addAction(fav_action)

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
        LOGGER.info(message)

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

    def normalize_name_from_folder(self, folder_name: str) -> str:
        clean = re.sub(r"\[[^\]]*\]", "", folder_name)
        clean = re.sub(r"-.*$", "", clean)
        clean = clean.replace("_", " ")
        clean = re.sub(r"\s+", " ", clean).strip(" .-_")
        return clean or folder_name.strip() or "Unknown Game"

    def generate_game_id(self, folder_path: str) -> str:
        return str(Path(folder_path).resolve()).lower()

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
                folder_path = url.toLocalFile()
                if folder_path and Path(folder_path).is_dir():
                    self.import_library_folder(folder_path)
                    accepted = True
        if accepted:
            event.setDropAction(Qt.DropAction.CopyAction)
            event.acceptProposedAction()
        else:
            event.ignore()

    def pick_library_folder(self):
        folder = QFileDialog.getExistingDirectory(
            self,
            "Choose archive parent folder",
            self.settings.get("last_library_folder", str(Path.home())),
        )
        if not folder:
            return
        self.settings["last_library_folder"] = folder
        self.save_all()
        self.import_library_folder(folder)

    def import_library_folder(self, folder_path: str):
        root = Path(folder_path)
        if not root.exists() or not root.is_dir():
            self.log(f"Invalid folder ignored: {folder_path}")
            return

        added = 0
        updated = 0
        skipped = 0

        for child in sorted(root.iterdir(), key=lambda p: p.name.lower()):
            if not child.is_dir():
                continue

            game_id = self.generate_game_id(str(child))
            clean_name = self.normalize_name_from_folder(child.name)
            existing = self.games.get(game_id)

            if existing:
                existing.name = clean_name
                existing.path = str(child)
                existing.modified_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                updated += 1
                self.log(f"Updated existing entry: {clean_name}")
            else:
                self.games[game_id] = GameEntry(
                    game_id=game_id,
                    name=clean_name,
                    path=str(child),
                    image=str(PLACEHOLDER_FILE),
                    tags=[],
                    summary="",
                    recent_reviews="",
                    all_reviews="",
                    metadata_status="queued",
                    image_source="placeholder",
                    added_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    modified_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                )
                self.pending_fetch_queue.append(game_id)
                added += 1
                self.log(f"Added archive folder: {clean_name}")

        if added == 0 and updated == 0:
            skipped += 1

        self.save_all()
        self.refresh_ui()
        self.log(f"Import complete. Added {added}, updated {updated}, skipped {skipped}.")

        if self.settings.get("auto_fetch_on_import", True):
            self.refresh_timer.start(100)

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

    def start_pending_fetches(self):
        while self.pending_fetch_queue and len(self.active_workers) < MAX_WORKERS:
            game_id = self.pending_fetch_queue.popleft()
            game = self.games.get(game_id)
            if not game:
                continue
            if game.metadata_status == "loading":
                continue
            self.fetch_metadata_for_game(game_id)

    def remove_finished_worker(self, game_id: str):
        self.active_workers = [w for w in self.active_workers if getattr(w, "game_id", None) != game_id]
        self.start_pending_fetches()

    def on_metadata_finished(self, game_id: str, payload: dict):
        self.remove_finished_worker(game_id)
        game = self.games.get(game_id)
        if not game:
            return

        game.path = payload.get("path", game.path)
        game.steam_id = payload.get("steam_id", game.steam_id)
        game.release_date = payload.get("release_date", game.release_date)
        game.tags = list(payload.get("tags", game.tags) or [])
        game.summary = payload.get("summary", game.summary)
        game.recent_reviews = payload.get("recent_reviews", game.recent_reviews)
        game.all_reviews = payload.get("all_reviews", game.all_reviews)
        game.metadata_status = payload.get("metadata_status", game.metadata_status)
        game.modified_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        image_path = payload.get("image", "")
        if image_path and Path(image_path).exists():
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
        game = self.games.get(game_id)
        if game:
            game.metadata_status = "error"
            game.summary = ""
            game.recent_reviews = ""
            game.all_reviews = ""
            game.image = str(PLACEHOLDER_FILE)
            game.image_source = "placeholder"
            game.modified_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.save_all()
        self.refresh_ui()
        self.log(error_message)

    def save_all(self):
        self.settings["window_width"] = self.width()
        self.settings["window_height"] = self.height()
        self.repo.save_games(self.games)
        self.repo.save_settings(self.settings)
        removed = self.repo.cleanup_unused_cache(self.games)
        if removed:
            self.log(f"Removed {len(removed)} unused cached image(s).")

    def refresh_ui(self):
        self.game_list.bind_games(self.games)
        self.title_label.setText(f"{len(self.games)} archived games • Favorites first • A-Z")

    def get_selected_game(self) -> Optional[GameEntry]:
        game_id = self.game_list.current_game_id()
        return self.games.get(game_id) if game_id else None

    def open_folder(self, game: GameEntry):
        if not game or not game.path:
            self.log("No folder available for selected game.")
            return
        target = Path(game.path)
        if not target.exists() or not target.is_dir():
            self.log(f"Open folder failed; folder missing: {game.path}")
            QMessageBox.warning(self, "Folder Missing", f"Folder not found:\n{game.path}")
            return
        try:
            subprocess.Popen(["explorer", str(target)])
            self.log(f"Opened folder for {game.name}")
        except Exception as exc:
            self.log(f"Failed to open folder for {game.name}: {exc}")
            QMessageBox.warning(self, "Open Folder Failed", f"Could not open:\n{game.path}\n\n{exc}")

    def open_selected_folder(self):
        game = self.get_selected_game()
        if not game:
            self.log("No game selected.")
            return
        self.open_folder(game)

    def browse_to_path(self, game: GameEntry):
        if not game or not game.path:
            self.log("No path available for selected game.")
            return
        target = Path(game.path)
        if not target.exists():
            self.log(f"Browse failed; folder missing: {game.path}")
            QMessageBox.warning(self, "Path Missing", f"Folder not found:\n{game.path}")
            return
        try:
            subprocess.Popen(["explorer", "/select,", str(target)])
            self.log(f"Opened Explorer for {game.name}")
        except Exception as exc:
            self.log(f"Failed to open Explorer for {game.name}: {exc}")
            QMessageBox.warning(self, "Browse to Path Failed", f"Could not open Explorer for:\n{game.path}\n\n{exc}")

    def show_details_selected(self):
        game = self.get_selected_game()
        if not game:
            return

        parts = [game.name]
        if game.release_date:
            parts.append(f"Release Date: {game.release_date}")
        if game.recent_reviews:
            parts.append(game.recent_reviews)
        if game.all_reviews:
            parts.append(game.all_reviews)
        if game.tags:
            parts.append("Tags: " + ", ".join(game.tags))
        if game.summary:
            parts.append("")
            parts.append(game.summary)
        parts.append("")
        parts.append(f"Folder: {game.path}")

        QMessageBox.information(self, "Game Details", "\n\n".join(parts))

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

    def show_context_menu(self, pos):
        item = self.game_list.itemAt(pos)
        if item:
            self.game_list.setCurrentItem(item)

        game = self.get_selected_game()
        if not game:
            return

        menu = QMenu(self)
        open_act = menu.addAction("Open Folder")
        browse_act = menu.addAction("Browse to Path")
        details_act = menu.addAction("View Details")
        steam_act = menu.addAction("Open Steam Page")
        rename_act = menu.addAction("Rename/Search Title")
        favorite_act = menu.addAction("Unfavorite" if game.favorite else "Favorite")
        redo_act = menu.addAction("Redo Search")
        placeholder_act = menu.addAction("Assign placeholder.png")
        image_act = menu.addAction("Set Custom Image")
        delete_act = menu.addAction("Delete")

        if not game.steam_id:
            steam_act.setEnabled(False)

        chosen = menu.exec(self.game_list.mapToGlobal(pos))

        if chosen == open_act:
            self.open_selected_folder()
        elif chosen == browse_act:
            self.browse_to_path(game)
        elif chosen == details_act:
            self.show_details_selected()
        elif chosen == steam_act:
            self.open_steam_page(game)
        elif chosen == rename_act:
            self.rename_selected()
        elif chosen == favorite_act:
            self.toggle_favorite_selected()
        elif chosen == redo_act:
            game.steam_id = ""
            game.release_date = ""
            game.tags = []
            game.summary = ""
            game.recent_reviews = ""
            game.all_reviews = ""
            game.metadata_status = "queued"
            game.image = str(PLACEHOLDER_FILE)
            game.image_source = "placeholder"
            self.pending_fetch_queue.appendleft(game.game_id)
            self.save_all()
            self.refresh_ui()
            self.start_pending_fetches()
            self.log(f"Requeued metadata search for {game.name}")
        elif chosen == placeholder_act:
            game.image = str(PLACEHOLDER_FILE)
            game.image_source = "placeholder"
            self.save_all()
            self.refresh_ui()
            self.log(f"Assigned placeholder.png to {game.name}")
        elif chosen == image_act:
            self.set_custom_image_selected()
        elif chosen == delete_act:
            self.delete_selected()

    def rename_selected(self):
        game = self.get_selected_game()
        if not game:
            return

        new_name, ok = QInputDialog.getText(self, "Rename/Search Title", "New game title for Steam search:", text=game.name)
        if not ok:
            return

        new_name = re.sub(r"\s+", " ", new_name).strip()
        if not new_name:
            self.log("Rename cancelled: empty name.")
            return

        game.name = new_name
        game.modified_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.save_all()
        self.refresh_ui()
        self.log(f"Renamed game to: {new_name}")

    def toggle_favorite_selected(self):
        game = self.get_selected_game()
        if not game:
            return
        game.favorite = not game.favorite
        game.modified_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
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

        if not file_path:
            return

        if not Path(file_path).exists():
            self.log("Custom image not found.")
            return

        game.image = file_path
        game.image_source = "custom"
        game.modified_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.save_all()
        self.refresh_ui()
        self.log(f"Custom image assigned to {game.name}")

    def delete_selected(self):
        game = self.get_selected_game()
        if not game:
            return

        answer = QMessageBox.question(self, "Delete Game", f"Remove {game.name} from the catalog?")
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
            LOGGER.exception("Failed to delete cached image %s", removed_image)

        self.save_all()
        self.refresh_ui()
        self.log(f"Deleted: {game.name}")

    def closeEvent(self, event):
        self.save_all()
        super().closeEvent(event)


def main():
    app = QApplication(sys.argv)
    window = MainWindow()
    window.ensure_placeholder()
    window.showMaximized()
    sys.exit(app.exec())


if __name__ == "__main__":
    try:
        main()
    except Exception:
        LOGGER.exception("Fatal archive catalog error")
        traceback.print_exc()