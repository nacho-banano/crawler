from crawler.binance.web_crawler import list_of_files

lof = list_of_files()

with open("list_of_resources", "w", encoding="UTF-8") as file:
    for resource in lof:
        file.write(resource)
