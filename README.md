# invoice-collection
Hướng dẫn sử dụng
 1. Install unrar
    Download the UnRAR command-line tool from the RARLab website: https://www.rarlab.com/rar_add.htm
    Extract the downloaded file to a location on your computer (e.g., C:\Program Files\UnRAR)
    Add the UnRAR directory to your PATH:

    Search for "Environment Variables" in Windows
    Click "Edit the system environment variables"
    Click the "Environment Variables" button
    Under "System variables", find "Path" and click "Edit"
    Click "New" and add the path to the UnRAR folder
    Click "OK" on all dialogs
 2. Install pdf2image, truy cập: https://github.com/oschwartz10612/poppler-windows/releases/
    Download the latest Poppler for Windows from here
    Extract the ZIP file to a location on your computer (e.g., C:\Program Files\poppler)
    Add the bin directory to your PATH:

    Search for "Environment Variables" in Windows
    Click "Edit the system environment variables"
    Click the "Environment Variables" button
    Under "System variables", find "Path" and click "Edit"
    Click "New" and add the path to the Poppler bin folder (e.g., C:\Program Files\poppler\bin)
    Click "OK" on all dialogs
  3. Tạo reports
      For Single Company Reports
      bashpython main_report.py --entity "COMPANY_NAME" 
      For Mass Generation (All Companies)
      bashpython main_report.py --mass-generation

    # Add optional drive-link argument
    parser.add_argument(
        "--drive-link",
        help="Google Drive folder link where files will be organized",
        default=config.DRIVE,
    )

    # Add time filter argument
    parser.add_argument(
        "--time",
        help="Time filter for emails (e.g., '1d' for 1 day, '2h' for 2 hours, '30m' for 30 minutes)",
        default="1d",
        choices=["30m", "1h", "2h", "3h", "6h", "12h", "1d", "2d", "3d", "7d"],
    )