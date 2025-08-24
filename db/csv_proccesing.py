from pathlib import Path

import pandas as pd
from sqlalchemy import text
from sqlalchemy.orm import Session

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
        logger.error(f"Error writing to DB -> {e}")
    finally:
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
    try:
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
    finally:
        logger.info(f"Sucsses remove_full_duplicates")
        session.close()


def get_cases_by_numbers(session: Session, case_numbers: list[str]) -> list[Case]:
    logger.info(f"Query in DB {len(case_numbers)} number case...")
    query = session.query(Case).filter(Case.case_number.in_(case_numbers))
    results = query.all()
    logger.info(f"Find {len(results)} records in DB.")
    return results


def get_cases(input_file: str):
    input_file: Path = Path(input_file)
    if not input_file.exists():
        logger.error(f"Input file not found: {input_file}")
        return

    try:
        df_input = pd.read_csv(input_file)
        case_numbers = df_input.iloc[:, 0].astype(str).tolist()
        if not case_numbers:
            logger.warning("Іnput CSV file is empty or not have case numbers.")
            return
        logger.info(f"{len(case_numbers)} case numbers read from {input_file.name}")
    except Exception as e:
        logger.error(f"Error read CSV file: {e}")
        return

    session = Session_maker()
    try:
        db_cases = get_cases_by_numbers(session, case_numbers)
    finally:
        session.close()

    if not db_cases:
        logger.info("Nothing found in the database.")
        return

    data_to_export = [
        {column.name: getattr(case, column.name) for column in Case.__table__.columns}
        for case in db_cases
    ]
    
    df_output = pd.DataFrame(data_to_export)
    df_output.to_csv("output.csv", index=False, encoding="utf-8")
    logger.info("Result save in output.csv.")


def test_main():
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