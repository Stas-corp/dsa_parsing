from sqlalchemy import create_engine, Column, Integer, String, DateTime
from sqlalchemy.orm import declarative_base, sessionmaker

Base = declarative_base()

class Case(Base):
    __tablename__ = "cases"

    id = Column(Integer, primary_key=True, autoincrement=True)
    court_name = Column(String)
    case_number = Column(String, index=True)
    case_proc = Column(String)
    registration_date = Column(DateTime)
    judge = Column(String)
    judges = Column(String)
    participants = Column(String)
    stage_date = Column(DateTime)
    stage_name = Column(String)
    cause_result = Column(String)
    cause_dep = Column(String)
    type = Column(String)
    description = Column(String)

engine = create_engine("sqlite:///cases.db")
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()