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

    def from_exec(self,d):
        sql_str = ""
        if "exec_sql" in d:
            try:
                sql_str = open(d["exec_sql"]).read()
            except Exception as e:
                logger.error(e)
                raise
        elif "exec_str" in d:
            sql_str = d["exec_str"]
        try:
            sql_str.format(**self.params)
        except Exception as e:
            logger.error(e)
            raise
        return sql_str

    def run_par(self, sql_str):
        try:
            curs = self.conn.cursor()
            curs.execute(sql_str)
            rs = curs.fetchone()
            res = {}
            i = 0
            if rs:
                for r in curs.description:
                    res.update({r[0]: rs[i]})
                    i += 1
            return rs
        except Exception as e:
            logger.error(e)
            raise


    def run(self, w):
        if not self.jobs:
            self.is_ended = True
            self.is_wait = False
        else:
            j = self.jobs.pop(0)
            res = {}
            if "to" in j:
                wt = w.get(j["to"], (None,None))
                w.update({j["to"]: (wt[0],self)})
                self.is_wait = True
            elif "from" in j:
                wt = w.get(j["from"], (None, None))
                w.update({j["from"]: (self, wt[1])})
                self.is_wait = True
            else:
                logger.log(str(j))
                sql_str = self.from_exec(j)
                if sql_str:
                    res = self.run_par(sql_str)
                if "output" in j:
                    for r, rn in res.items():
                        if r in j["output"]:
                            self.params = self.params.update({r: rn})


    def run_with(self, c2):
        j_from = self.jobs.pop(0)
        j_to = self.jobs.pop(0)
        if "into" in j_to:
            sql1 = self.from_exec(j_from)
            if sql1:
                logger.log(str(j_from) + "->" + str(j_to))
                try:
                    curs = self.conn.cursor()
                    curs.execute(sql1)
                    curs.commit()
                    par = ", ".join(["?" for r in curs.description])
                    sql2 = " INSERT INTO {0} VALUES ({1})".format(j_to["into"],par)
                    curs2 = c2.conn.cursor()
                    curs2.executemany(sql2, curs.fetchmany())
                    curs2.commit()
                except Exception as e:
                    logger.error(e)
                    raise
        self.is_wait = False
        c2.is_wait = False


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
                wts[0].run_with(wts[1])


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
        raise

if __name__ == "__main__":
    main()
