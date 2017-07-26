import pyodbc
import json

class Logger:
    def __init__(self, name):
        self.curs = pyodbc.connect("DRIVER={ODBC Driver 11 for SQL Server};SERVER=alborz;DATABASE=Leftovers;Trusted_Connection=yes;").cursor()
        self.curs.execute("EXEC dbo.Plog_start '{name}'".format(name=name))
        row = self.curs.fetchone()
        self.curs.commit()
        self.id = row.id

    def log(self, descr):
        self.curs.execute("EXEC dbo.Plog_log {id}, '{descr}'".format(id=self.id, descr=descr))
        self.curs.commit()

    def end(self):
        self.curs.execute("EXEC dbo.Plog_end {id}".format(id=self.id))
        self.curs.commit()

    def error(self,error):
        self.curs.execute("EXEC dbo.Plog_error {id}, '{descr}'".format(id=self.id, descr = error))
        self.curs.commit()


def main():
    with open("config.json") as f_file:
        cfg = json.load(f_file)
    logger = Logger("some_name")
    try:
        for o in cfg:
            curs = pyodbc.connect(o["connection"]).cursor()

            if "exec" in o:
                logger.log(o["exec"])
                with open(o["exec"]) as f_exec:
                    str = f_exec.read()
                    curs.execute(str)
                    curs.commit()
        logger.end()
    except Exception as e:
        logger.error(e)

if __name__ == "__main__":
    main()
