"""TODO: Add description."""

from crawler.binance import download_all

if __name__ == "__main__":
    DESTINATION_DIR: str = "/home/nacho/data"

    try:
        download_all(DESTINATION_DIR)
    except KeyboardInterrupt:
        print("\nDownload interrupted by user")
