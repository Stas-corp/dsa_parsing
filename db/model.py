from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.orm import declarative_base

from db.db import engine

Base = declarative_base()

class Case(Base):
    __tablename__ = "cases"

    id = Column(Integer, primary_key=True, autoincrement=True)
    court_name = Column(String(500))
    case_number = Column(String(255), index=True)
    case_proc = Column(String(255))
    registration_date = Column(DateTime)
    judge = Column(String(255))
    judges = Column(String(1000))
    participants = Column(String(2000))
    stage_date = Column(DateTime)
    stage_name = Column(String(500))
    cause_result = Column(String(500))
    cause_dep = Column(String(500))
    type = Column(String(500))
    description = Column(String(2000))

Base.metadata.create_all(engine)