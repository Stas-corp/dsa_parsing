from pathlib import Path

import pandas as pd
from sqlalchemy import text

from db.model import Case
from db.db import Session_maker
from config.logger import logger


def writer_task(data: dict):
    session = Session_maker()
    try:
        session.bulk_insert_mappings(Case, data)
        session.commit()
    except Exception as e:
        session.rollback()
        logger.error(f"Error writing to database -> {e}")
    session.close()


def csv_proccesing(path: Path) -> dict:
    for csv_file in path.rglob("*.csv"):
        try:
            df = pd.read_csv(
                    csv_file,
                    sep='\t',
                    quotechar='"',
                    on_bad_lines='skip'
                )
            logger.info(f"read {csv_file.name}")
        except Exception as e:
            logger.error(f"Error read csv -> {e}")
            raise

        for col in ["registration_date", "stage_date"]:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce", dayfirst=True)
                df[col] = df[col].apply(lambda x: x if pd.notnull(x) else None)

        df = df.astype(object).where(pd.notnull(df), None)

        data = []
        for _, row in df.iterrows():
            case_data = {
                "court_name": row.get("court_name"),
                "case_number": row.get("case_number"),
                "case_proc": row.get("case_proc"),
                "registration_date": row.get("registration_date"),
                "judge": row.get("judge"),
                "judges": row.get("judges"),
                "participants": row.get("participants"),
                "stage_date": row.get("stage_date"),
                "stage_name": row.get("stage_name"),
                "cause_result": row.get("cause_result"),
                "cause_dep": row.get("cause_dep"),
                "type": row.get("type"),
                "description": row.get("description"),
            }
            data.append(case_data)
            
        logger.info(f"{csv_file.name} len data -> {len(data)}")
        writer_task(data)
        logger.info(f"{csv_file.name} commit sucsses.")


def remove_full_duplicates():
    logger.info(f"Start remove_full_duplicates")
    session = Session_maker()
    session.execute(text(
    """
        WITH cte AS (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY court_name, case_number, case_proc, registration_date,
                        judge, judges, participants, stage_date, stage_name,
                        cause_result, cause_dep, type, description
                    ORDER BY id
                ) AS rn
            FROM cases
        )
        DELETE FROM cte WHERE rn > 1;
    """
    ))
    session.commit()
    logger.info(f"Sucsses remove_full_duplicates")
    session.close()


def main():
    from concurrent.futures import ProcessPoolExecutor, as_completed
    paths = [
        Path('zip_dir\\19.08.2025_unpack'),
        Path('zip_dir\\21.08.2025_unpack'),
        Path('zip_dir\\22.08.2025_unpack')]
    
    with ProcessPoolExecutor(max_workers=3) as process_executor:
        process_futures = []
        for p in paths:
            process_futures.append(process_executor.submit(csv_proccesing, p))
        
        
        for process_future in as_completed(process_futures):
            logger.info(f"Future {process_future}")
            try:
                process_future.result()
                logger.info(f"Sucsses {process_future}")
            except Exception as e:
                logger.error(f"Error processing archive: {e}")
                
    
    remove_full_duplicates()
    # csv_proccesing(Path('zip_dir\\22.08.2025_unpack'))

if __name__ == "__main__":
    csv_proccesing(Path('zip_dir\22.08.2025_unpack'))