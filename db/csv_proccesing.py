from pathlib import Path
from multiprocessing import Queue

import pandas as pd
from sqlalchemy import text
from sqlalchemy.orm import Session

from db.model import Case, Session_maker
from config.logger import logger


def writer_task(queue: Queue, batch_size: int = 15_000):
    session = Session_maker()
    buffer = []
    
    while True:
        case_data = queue.get()
        if case_data is None:
            break
        
        buffer.append(Case(**case_data))
        if len(buffer) >= batch_size:
            try:
                session.add_all(buffer)
                session.commit()
                buffer.clear()
                logger.info(f"In queue -> {queue.qsize()}")
            except Exception as e:
                session.rollback()
                logger.error(f"Error writing to database -> {e}")
            logger.info("Remove full duplicates")
            
    remove_full_duplicates(session)
                
    if buffer:
        try:
            session.add_all(buffer)
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Error writing final batch to database -> {e}")
            
    session.close()


def csv_proccesing(path: Path, queue: Queue):
    for csv_file in path.rglob("*.csv"):
        try:
            df = pd.read_csv(
                    csv_file,
                    sep='\t',
                    quotechar='"',
                    on_bad_lines='skip'
                )
            # print(f"readed {csv_file}")
        except Exception as e:
            logger.error(f"Error read csv -> {e}")
            raise

        for col in ["registration_date", "stage_date"]:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce", dayfirst=True)
                df[col] = df[col].apply(lambda x: x if pd.notnull(x) else None)

        df = df.astype(object).where(pd.notnull(df), None)

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
            queue.put(case_data)

        # try:
        #     session.commit()
        # except Exception as e:
        #     session.rollback()
        #     print(f"Error proccesing csv -> {csv_file}: {e}")
    
    # remove_full_duplicates(session)
    # session.close()


def remove_full_duplicates(session: Session):
    session.execute(text(
    """
        DELETE FROM cases
        WHERE id NOT IN (
            SELECT MIN(id)
            FROM cases
            GROUP BY court_name, case_number, case_proc, registration_date,
                judge, judges, participants, stage_date, stage_name,
                cause_result, cause_dep, type, description
        )
    """
    ))
    session.commit()


if __name__ == "__main__":
    csv_proccesing(Path('zip_dir\16.07.2025_unpack'))