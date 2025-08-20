import logging

log_file = "scrap.log"
logger = logging.getLogger("dsa_scraper")
logger.setLevel(logging.INFO)

fh = logging.FileHandler(log_file, encoding="utf-8")
fh.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S")
fh.setFormatter(formatter)

ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch.setFormatter(formatter)

logger.addHandler(fh)
logger.addHandler(ch)