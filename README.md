# SageGameCatalog
A python script that uses folder names of DRM Free installation game game titles (Like from GOG) and obtains the screenshots, reviews and other details from Steam.


A Windows PyQt6 game archive browser that scans a parent folder of game folders, turns each child folder into a catalog entry, cleans the folder name for a nicer title, then looks up Steam metadata and artwork for each game. It stores each entry locally with the folder path, Steam app id, release date, image, and other metadata so the library persists between runs.

What it does
Imports a parent folder and reads each immediate child folder as one game entry.

Cleans folder names by removing anything inside square brackets and anything from the first - onward before searching Steam.

Fetches Steam metadata including header image, release date, tags, summary, and review summaries, then shows them in the UI and details view.

Lets you right-click to rename/search again, redo metadata lookup, open the Steam page, set a custom image, assign placeholder.png, favorite entries, view details, or delete entries.

Opens the selected game’s folder in Explorer rather than launching an .exe, because this version is built as an archive catalog.

Data and logging
The script saves its catalog and settings as JSON files beside the script and caches downloaded Steam images in a local cache folder so artwork does not need to be re-downloaded every time. It also writes timestamped log output to the UI and to a log file, and includes request pacing so Steam lookups are spaced out to reduce the chance of blocking.

Prereqs
You need Python 3 on Windows plus these packages: PyQt6, requests, and beautifulsoup4 (bs4). You also need a placeholder.png file in the same folder as the script, because that image is used whenever a game has no Steam match, a lookup fails, or you explicitly assign the placeholder.

Notes
It is designed for Windows because it uses Explorer integration to open folders and browse paths. Re-scanning the same parent folder does not create duplicates; existing entries are matched by resolved folder path and updated in place, while only new folders are added.
