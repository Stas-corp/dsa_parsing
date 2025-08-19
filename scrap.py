import zipfile
import shutil
from pathlib import Path

import requests
from bs4 import BeautifulSoup

import csv_proccesing

BASE_URL = "https://dsa.court.gov.ua"
START_URL = f"{BASE_URL}/dsa/inshe/oddata/532/?page=1"


def get_archives():
    page = 3
    urls_zip = []
    date_zip = []
    while True:
        url = f"{BASE_URL}/dsa/inshe/oddata/532/?page={page}"
        resp = requests.get(url)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        items = soup.find_all("div", class_="allFiles")
        # print(f"items -> {items}")
        if not items:
            break

        stop = False
        splash = False
        for link in items:
            item = link.find("a")
            text = item.get("download")
            href = item.get("href")
            # print(f"{text}\n{href}")
            if not href or not href.endswith(".zip"):
                continue
            if "2025" in text:
                urls_zip.append(href)
                date_zip.append(text.split('від')[-1].strip())
                # if splash:
                stop = True
            elif "2024" in text:
                splash = True
                continue
        # print(result)
        if stop:
            break
        page += 1
    return list(zip(urls_zip, date_zip))


def download_archive(url: str, date: str):
    print(f"Downloading: {url}")
    if not tmpdir.exists():
        tmpdir.mkdir()
    local_zip:Path = tmpdir / date

    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_zip, "wb") as f:
            shutil.copyfileobj(r.raw, f)
            extract_archive(local_zip, date)
        local_zip.unlink()


def extract_archive(path: Path, date: str):
    with zipfile.ZipFile(path, "r") as zf:
        target_path:Path = tmpdir / str(date[:-4]+'_unpack')
        zf.extractall(target_path)
        csv_proccesing.csv_proccesing(target_path)
        shutil.rmtree(target_path)


def main():
    archives = get_archives()
    print(f"Found {len(archives)} archives")
    # for url, date in archives:
    #     download_archive(url, date)
    url, date = archives[0]
    download_archive(url, date)
        

if __name__ == "__main__":
    tmpdir = Path('zip_dir')
    if not tmpdir.exists():
        tmpdir.mkdir()
    main()
