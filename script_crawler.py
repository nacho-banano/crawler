"""TODO: Add description."""

from crawler.binance.web_crawler import get_list_of_prefixes

lof = get_list_of_prefixes()

with open("list_of_resources", "w", encoding="UTF-8") as file:
    for resource in lof:
        file.write(resource + "\n")
