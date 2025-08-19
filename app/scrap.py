import zipfile
import shutil
from pathlib import Path
from multiprocessing import Manager, Queue, Process
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup

from db.csv_proccesing import csv_proccesing, writer_task
from config.logger import logger

BASE_URL = "https://dsa.court.gov.ua"
START_URL = f"{BASE_URL}/dsa/inshe/oddata/532/?page=1"


def get_archives() -> list[tuple[str, str]]:
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
                date_zip.append(text.split("від")[-1].strip())
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


def download_archive(url: str, date: str, dir: Path):
    logger.info(f"Downloading: {url}")
    local_zip: Path = dir / date

    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_zip, "wb") as f:
            shutil.copyfileobj(r.raw, f)
            # extract_archive(local_zip, date)
        # local_zip.unlink()
    logger.info(f"Downloaded: {date}")
    return local_zip


def extract_archive(path: Path, date: str, queue: Queue):
    tmpdir = Path("zip_dir")
    with zipfile.ZipFile(path, "r") as zf:
        target_path: Path = tmpdir / str(date[:-4]+"_unpack")
        zf.extractall(target_path)
        csv_proccesing(target_path, queue)
        shutil.rmtree(target_path)
    path.unlink()


def main():
    zipdir = Path("zip_dir")
    if not zipdir.exists():
        zipdir.mkdir()
    
    mng = Manager()
    queue = mng.Queue()
    writer = Process(target=writer_task, args=(queue,))
    writer.start()
    logger.info("Start get archives...")
    archives = get_archives()
    logger.info(f"Found {len(archives)} archives")
    
    with ThreadPoolExecutor(max_workers=5) as download_executor, ProcessPoolExecutor(max_workers=4) as process_executor:
        future_to_archive = {
            download_executor.submit(download_archive, url, date, zipdir): date for url, date in archives
        }

        process_futures = []
        
        for future in as_completed(future_to_archive):
            date = future_to_archive[future]
            try:
                logger.info(f"Start extract {date}")
                local_zip = future.result()
                # extract_archive(local_zip, date, queue)
                process_futures.append(process_executor.submit(extract_archive, local_zip, date, queue))
            except Exception as e:
                logger.error(f"Error downloading or processing archive {date}: {e}")
        
        for process_future in as_completed(process_futures):
            logger.info(f"Future {process_future}")
            try:
                process_future.result()
                logger.info(f"Sucsses {process_future}")
            except Exception as e:
                logger.error(f"Error processing archive: {e}")
        
    queue.put(None)
    writer.join()


if __name__ == "__main__":
    main()
