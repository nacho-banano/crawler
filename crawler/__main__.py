"""TODO: Add description."""

from crawler.binance import download_all

if __name__ == "__main__":
    try:
        download_all()
    except KeyboardInterrupt:
        print("\nDownload interrupted by user")
