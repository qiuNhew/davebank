"""Single-file entry point for building a standalone executable.

Building this with PyInstaller produces ``project.exe`` which can be placed at
``C:\\temp\\project\\project.exe`` on the CSB/Ashby lab machines to bypass the
Windows firewall popup (which cannot be accepted without an admin account).

    pyinstaller --onefile --name project davebank_main.py

Run it exactly like ``python -m davebank`` -- the same flags apply, e.g.:

    C:\\temp\\project\\project.exe --nick alice --port 50000
"""

from davebank.__main__ import main

if __name__ == "__main__":
    main()
