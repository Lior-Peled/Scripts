from sqlalchemy.orm import sessionmaker
from datetime import datetime
from utils.common import get_logger, get_db_schema, get_db
import pandas as pd
from os.path import basename
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib


logger = get_logger(__name__)


def execute_step(engine, schema, tables_to_run, step_type):
    session_maker = sessionmaker(bind=engine)
    conn = engine.connect()
    session = session_maker()
    for module in tables_to_run:
        try:
            mod = __import__(f'queries.{step_type}.' + module, fromlist=["query"])
            queries_to_run = getattr(mod, "queries_to_run")

            for query in queries_to_run:
                session.execute(query.format(schema=schema))

            session.commit()
            result = pd.DataFrame({'StoredProcedureName': [f"{module}"], 'ExecutionStatus': ['Success'],
                                   'ExecutionDateTime': [datetime.today()]})
            result.to_sql("MNG_SP_Executions", engine, schema=schema, if_exists='append', index=False)
            logger.info(f"{module} Commit")
        except:
            session.rollback()
            result = pd.DataFrame({'StoredProcedureName': [f"{module}"], 'ExecutionStatus': ['Failed'],
                                   'ExecutionDateTime': [datetime.today()]})
            result.to_sql("MNG_SP_Executions", engine, schema=schema, if_exists='append', index=False)
            logger.info(f"{module} Failed")
            raise
        finally:
            session.close()
    conn.close()


def send_email(source, to, subj, text, smtp='smtp-relay.gmail.com:587', files=None):
    # Create message container - the correct MIME type is multipart/alternative.
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subj
    msg['From'] = source
    msg['To'] = ",".join(to) if isinstance(to, list) else to

    msg.attach(MIMEText(text, 'plain'))

    for f in files or []:
        with open(f, "rb") as fil:
            part = MIMEApplication(
                fil.read(),
                Name=basename(f)
            )
        # After the file is closed
        part['Content-Disposition'] = 'attachment; filename="%s"' % basename(f)
        msg.attach(part)

    # Send the message via local SMTP server.
    smtp = smtplib.SMTP(smtp)
    smtp.ehlo()
    smtp.starttls()

    # sendmail function takes 3 arguments: sender's address, recipient's address
    # and message to send - here it is sent as one string.
    smtp.sendmail(source, to, msg.as_string())
    smtp.close()
