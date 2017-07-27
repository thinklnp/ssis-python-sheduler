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

logger = Logger("test_1")

class Connection:
    def __init__(self, o):
        self.conn = pyodbc.connect(o["connection"])
        self.is_ended = False
        self.is_wait = False
        self.jobs = o["jobs"]
        self.params = {}

    def run_inner(self):
        pass

    def run(self, w):
        if not self.jobs:
            self.is_ended = True
            self.is_wait = False
        else:
            j = self.jobs.pop(0)
            if "to" in j:
                wt = w.get(j["to"], (None,None))
                w.update({j["to"]: (wt[0],self)})
                self.is_wait = True
            elif "from" in j:
                wt = w.get(j["from"], (None, None))
                w.update({j["from"]: (self, wt[1])})
                self.is_wait = True
            else:
                sql_str = ""
                if "exec_sql" in j:
                    try:
                        sql_str = open(j["exec_sql"]).read()
                    except Exception as e:
                        logger.error(e)
                elif "exec_str" in j:
                    sql_str = j["exec_str"]
                try:
                    sql_str.format(**self.params)
                except Exception as e:
                    logger.error(e)



    def run_with(self, c2):
        pass


def main():
    with open("config.json") as f_file:
        cfg = json.load(f_file)

    cns = []
    for o in cfg:
        cns.append(Connection(o))
    waits = {}
    while all([x.is_ended for x in cns]):
        cr = [cn for cn in cns if not cn.is_ended and not cn.is_wait][0]
        cr.run(waits)
        for wtn, wts in waits.items():
            if wts[0] and wts[1]:
                wts[0].run_with(wts[1], logger)


    try:
        for o in cfg:

            conn = pyodbc.connect(o["connection"])
            for j in cfg["jobs"]:
                curs = conn.cursor()



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
