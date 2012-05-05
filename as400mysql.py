#!/usr/local/bin/python

import sys

try:
    import string
    import time
    import os
    import os.path
    import ConfigParser
    import md5
    import MySQLdb
except:
    print 'Error, not all modules could be imported'
    sys.exit()
    
class DBHandler:

    """ class to handle db actions """

    def __init__(self, ctsr):
        self.__ctsr = self.AnalyseConnectString(ctsr)
        self.__dbType = self.__ctsr[0]
        self.__dbUser = self.__ctsr[1]
        self.__dbPwd = self.__ctsr[2]
        self.__dbHost = self.__ctsr[3]
        self.__dbName = self.__ctsr[4]
    
    def AnalyseConnectString(self, cstr):
        """ analizes connection string """
        try:
            a, b = string.split(cstr, '@')
            dbType, dbUser, dbPwd = string.split(a, ':')
            dbHost, dbName = string.split(b, ':')
        except:
            print 'ERROR WITH CONNECT STRING: ', str(cstr)
        return (dbType, dbUser, dbPwd, dbHost, dbName)

    def Connect(self):
        """ connect to database """
        try:
            self.connection = MySQLdb.connect(host = self.__dbHost, db = self.__dbName,
                                              user = self.__dbUser, passwd = self.__dbPwd)
        except MySQLdb.OperationalError, msg:
            self.Exception(msg)

    def Commit(self):
        self.connection.commit()

    def Close(self):
        """ close connectio to database """
        self.connection.close()

    def SetCursor(self):
        self.__cursor = self.connection.cursor()

    def GetCursor(self):
        return self.__cursor

    def Execute(self, stmt):
        self.__cursor.execute(stmt)

    def GetResult(self):
        return self.__cursor.fetchall()

    def GetFields(self, table):
        fields = []
        self.__cursor.execute('DESCRIBE %s' % table)
        r = self.__cursor.fetchall()
        for i in r:
            fields.append(i[0])
        return fields

class Parser:

    """ class to parse line an get information from export to import """
    """ data format is <table>#<todo flag>#<value 1>;<value 2>;..... """
    """ example: kso#N#1;2;3;4;safddsa;;                             """

    def __init__(self):
        self.__data = None
        self.__line = ''

    def SetLine(self, value):
        self.__line = value

    def SplitLine(self):
        tmp = string.split(string.strip(self.__line), '#', 2)
        tmp2 = []
        tmp2.append(tmp[0])
        tmp2.append(tmp[1])
        tmp2.append(string.split(string.strip(tmp[2]), ';'))
        self.__data = tmp2

    def GetParsed(self):
        if self.__data:
            return self.__data

class File:

    """ class to handle files """

    def __init__(self):
        self.__file = None
        self.__filename = ''
        self.__content = ''
        self.__method = ''

    def SetFileName(self, value):
        self.__filename = value

    def SetMethod(self, value):
        """ open file method r for read, w for write """
        self.__method = value

    def Open(self):
        self.__file = open('%s' % self.__filename , '%s' % self.__method)

    def Read(self):
        return self.__file.readlines()

    def AddLine(self, value):
	#print "File.AddLine()"
        self.__content = self.__content + str(value) + '\n'

    def ClearLine(self):
        self.__content = ''
    
    def Write(self):
	#print "File.Write()"
        if self.__content != '':
            self.__file.write(str(self.__content))
        else:
            self.Delete()

    def Close(self):
	#print "File.Close()"
        self.__file.close()

    def Delete(self):
	#print "File.Delete()"
        os.system('rm %s' %(self.__filename))
    

class Statement:

    """ class to build and handle statements """

    def __init__(self):
        self.__type = ''
        self.__stmt = ''
        self.__data = []

    def SetData(self, data):
        self.__data = data

    def GetType(self, type):
        type = string.upper(type)
        if type == 'N':
            return type
        if type == 'D':
            return type
        if type == 'U':
            return type

    def Insert(self, table, fields, data):
        """ Insert new data into database if count of fields """
        """ is the same as count of data                     """
        if len(fields) == len(data):
            stmt = "INSERT INTO %s ( " % table
            """ add field list to statement """
            for i in range(len(fields)):
                if i == len(fields) - 1:
                    stmt = stmt + fields[i]
                else:
                    stmt = stmt + fields[i] + ','
            stmt = stmt + ') VALUES ( '
            """ add value list to statement """
            for i in range(len(data)):
                if i == len(data) - 1:
                    stmt = stmt + "'" + data[i] + "'"
                else:
                    stmt = stmt + "'" + data[i] + "'" + ','
            stmt = stmt + ')'
            return stmt
        else:
            return None

    def Delete(self, table, fields, data, indexes):
        """ delete from table where field = data """
        if len(fields) == len(data):
            stmt = "DELETE FROM %s WHERE " % table
            for i in range(len(indexes)):
                if i == len(indexes) - 1:
                    stmt = stmt + "%s = '%s' " %(fields[int(indexes[i]) - 1], data[int(indexes[i]) - 1])
                else:
                    stmt = stmt + "%s = '%s' AND " %(fields[int(indexes[i]) -1], data[int(indexes[i]) - 1])
            return stmt
        else:
            return None

    def Update(self, table, fields, data, indexes):
        if len(fields) == len(data):
            stmt = "UPDATE %s SET " % table
            for i in range(len(fields)):
                if i == len(fields) - 1:
                    stmt = stmt + "%s = '%s' " % (fields[i], data[i])
                else:
                    stmt = stmt + "%s = '%s', " % (fields[i], data[i])
            stmt = stmt + "WHERE "
            for i in range(len(indexes)):
                if i == len(indexes) - 1:
                    stmt = stmt + "%s = '%s' " % (str(fields[int(indexes[i]) - 1]) ,
                                                  str(data[int(indexes[i] ) - 1]))
                else:
                    stmt = stmt + "%s = '%s' AND " % (str(fields[int(indexes[i]) - 1]) ,
                                                      str(data[int(indexes[i]) - 1]))
                    
            return stmt
        else:
            return None

class MD5:

    """ class to handel md5 checksum """

    def __init__(self):
        self.__md5 = md5.new()

    def Update(self, valStr):
        self.__md5.update(valStr)

    def GetDigest(self):
        return self.__md5.digest()

class Fields:

    """ class to handle fields """

    def __init__(self, db):
        self.__fields = {}
        self.__db = db

    def AddFields(self, table, field):
        self.__fields[table] = field

    def GetFields(self, table):
        if not self.__fields.has_key(table):
            self.AddFields(table, self.__db.GetFields(table))
        return self.__fields[table]

class Tables:
    
    def __init__(self):
        self.__tables = {}

    def CheckTable(self, table):
        if not self.__tables.has_key(table):
            tablename, indexes = getIndexes(checkTable(table))
            self.__tables[table] = []
            self.__tables[table].append(tablename)
            self.__tables[table].append(indexes)
        return self.__tables[table][0], self.__tables[table][1]

#
# Temporary file for GetData()
#
getDataTempFile = File()
getDataTempFile.SetFileName("%s.tmp" % time.time())
getDataTempFile.SetMethod('w')
getDataTempFile.Open()

def t():
    """ return local iso time string """
    return time.strftime('%a %b %d %X %Z %Y ', time.localtime(time.time()))
        
def check(host):
    d = {}
    try:
        cp = ConfigParser.ConfigParser()
        #cp.read('/usr/local/as400import/as400mysql.cfg')
        #cp.read('./as400mysql.cfg')
        cp.read('/export/home/as400/as400mysql.cfg')
        d['hosts'] = cp.get('hosts', '%s' % host)
        d['restzeit'] = cp.get('%s' % host, 'restzeit' )
        d['datanormzeit'] = cp.get('%s' % host, 'datanormzeit')
        d['ctsr'] = cp.get('%s' % host, 'ctsr')
        d['tmpdir'] = cp.get('%s' % host, 'tmpdir')
        return d
    except:
        print 'ERROR: COULD NOT READ ./as400mysql.cfg'
        sys.exit()
        
def checkTable(table):
    d = {}
    try:
        table = string.upper(table)
        cp = ConfigParser.ConfigParser()
        #cp.read('/usr/local/as400import/as400mysql.cfg')
        #cp.read('./as400mysql.cfg')
        cp.read('/export/home/as400/as400mysql.cfg')
        d['table'] = cp.get('table', '%s' % table)
        return d['table']
    except:
        return None

def replaceNewline(s):
    s = string.replace(s, '\r\n', ' ')
    s = string.replace(s, '\n', ' ')
    return s

def replaceSemicolon(s):
    s = string.replace(s, ';', '')
    return s

def GetData(db, type,  firma = 0, artnr = '', ring = ''):
    """ get data out of table """
    t = time.localtime(time.time())
    if type == 1:
        stmt = "SELECT w.firma, wz.objekt, w.kdnr, wz.lieferart, wz.auftragnr, w.pos, wz.kommission, " \
               "       wz.lieferdatum, wz.angebot, wz.angebotken, wz.lieferadr, wz.zusatztext, w.artnr, w.menge, " \
               "       w.ring, w.preis, w.pd, w.id " \
               "  FROM warenkorb as w, warenkorbzusatz as wz " \
               " WHERE wz.lieferadrnr = 0 " \
               "   AND wz.auftragnr = w.auftragnr " \
               "   AND wz.kdnr = w.kdnr " \
               "   AND w.status = 1" \
               "   AND (wz.lieferadr != '' " \
               "    OR  wz.lieferadr != ' ')"
        
        db.Execute(stmt)
        tmp = db.GetResult()
        result = ""
        if len(tmp) > 0:
            for data in tmp:
                for k in range(len(data)):
                    d = replaceNewline(str(data[k]))
                    if k == 3:
                        result = result + '%02i:%02i;%02i.%02i.%04i;' % (t[3], t[4], t[2], t[1], t[0])
                        result = result + replaceSemicolon(d) + ';'
                    elif k == 10 or k == 12: # 9 or 11
                        result = result + d + ';'
                    elif k == len(data) - 2:
                        result = result + replaceSemicolon(d)
                        #result = result + str(data[k])
                    elif k == len(data) - 1:
                        pass # id
                    else:
                        result = result + replaceSemicolon(d) + ';'
                        #result = result + str(data[k]) + ';'
		#print "GetData type=1: result=" + result
                #result = result + '\n'
		WriteResultToFile(result)
		result = ""
                MoveToFiling(db, data[17])

    if type == 2:
        stmt = "SELECT w.firma, wz.objekt, w.kdnr, wz.lieferart, wz.auftragnr, w.pos, wz.kommission, " \
               "       wz.lieferdatum, wz.angebot, wz.angebotken, wz.lieferadr, wz.zusatztext, w.artnr, w.menge, " \
               "       w.ring, w.preis, w.pd, w.id " \
               "  FROM warenkorb as w, warenkorbzusatz as wz " \
               " WHERE wz.lieferadrnr = 0 " \
               "   AND wz.auftragnr = w.auftragnr " \
               "   AND wz.kdnr = w.kdnr " \
               "   AND w.status = 1" \
               "   AND (wz.lieferadr = '' " \
               "    OR  wz.lieferadr = ' ')"
        
        db.Execute(stmt)
        tmp = db.GetResult()
        result = ""
        if len(tmp) > 0:
            for data in tmp:
                for k in range(len(data)):
                    d = replaceNewline(str(data[k]))
                    if k == 3:
                        result = result + '%02i:%02i;%02i.%02i.%04i;' % (t[3], t[4], t[2], t[1], t[0])
                        result = result + replaceSemicolon(d) + ';'
                    elif k == 9: # 8, 11
                        ## alt result = result + ';;;'
                        ## alt result = result + replaceSemicolon(d) + ';'
                        ## zeilen wurden getauscht!
                        result = result + replaceSemicolon(d) + ';'
                        result = result + ';;;'
                    elif k == len(data) - 2:
                        #result = result + string.replace(str(data[k]), 'L', '')
                        result = result + replaceSemicolon(d)
                    elif k == len(data) - 1:
                        pass # id
                    else:
                        #result = result + string.replace(str(data[k]), 'L', '') + ';'
                        result = result + replaceSemicolon(d) + ';'
		#print "GetData type=2: result=" + result
                #result = result + '\n'
		WriteResultToFile(result)
		result = ""
                MoveToFiling(db, data[17])

    if type == 3:
        stmt = "SELECT w.firma, wz.objekt, w.kdnr, wz.lieferart, wz.auftragnr, w.pos, wz.kommission, " \
               "       wz.lieferdatum, wz.angebot, wz.angebotken, l.name, l.strasse, "\
               "       l.plz, l.ort, wz.zusatztext, w.artnr, w.menge, " \
               "       w.ring, w.preis, w.pd, w.id " \
               "  FROM warenkorb as w, warenkorbzusatz as wz, lieferadresse as l " \
               " WHERE wz.lieferadrnr != 0 " \
               "   AND l.kdnr = w. kdnr " \
               "   AND l.lieferadrnr = wz.lieferadrnr " \
               "   AND wz.auftragnr = w.auftragnr " \
               "   AND wz.kdnr = w.kdnr " \
               "   AND w.status = 1" \
               "   AND (wz.lieferadr = '' " \
               "    OR  wz.lieferadr = ' ')"
        
        db.Execute(stmt)
        tmp = db.GetResult()
        result = ""
        if len(tmp) > 0:
            for data in tmp:
                for k in range(len(data)):
                    d = replaceNewline(str(data[k]))
                    if k == 3:
                        result = result + '%02i:%02i;%02i.%02i.%04i;' % (t[3], t[4], t[2], t[1], t[0])
                        result = result + replaceSemicolon(d) + ';'
                    elif k == len(data) - 2:
                        #result = result + string.replace(str(data[k]), 'L', '')
                        #result = result + string.replace(str(data[k]), '\n', '')
                        result = result + replaceSemicolon(d)
                    elif k == len(data) - 1:
                        pass # id
                    else:
                        #result = result + string.replace(str(data[k]), 'L', '') + ';'
                        #result = result + string.replace(str(data[k]), '\n','') + ';'
                        result = result + replaceSemicolon(d) + ';'
		#print "GetData type=3: result=" + result
                #result = result + '\n'
		WriteResultToFile(result)
		result = ""
                MoveToFiling(db, data[20])

    if type == 4:
        stmt = "SELECT firma, kdnr, artnr, ring " \
               "  FROM warenkorb " \
               " WHERE firma = '%s' " % firma + \
               "   AND artnr = '%s' " % artnr + \
               "   AND status < 2 " \
               "   AND ring = '%s'" % ring        
        db.Execute(stmt)
        result =  db.GetResult()[0]

    return result

#
# Erzeugten Datensatz in eine Datei schreiben
#
def WriteResultToFile(result):
    getDataTempFile.AddLine(result)

#
# Datensatz in Tabelle warenkorbablage schreiben und status auf 2 setzen
#
def MoveToFiling(db, id):
    #print "MoveToFiling: move id #%i to filing" % id
    t = time.localtime(time.time())
    zeit = "%02i:%02i:%02i" % (t[3], t[4], t[5])
    datum = "%04i-%02i-%02i" % (t[0], t[1], t[2])
    stmt = "INSERT INTO warenkorbablage SELECT * FROM warenkorb WHERE id = %i" % id
    #print "MoveToFiling: " + stmt
    db.Execute(stmt)
    stmt = "UPDATE warenkorbablage SET zeit = '%s' AND datum = '%s', status = 2 WHERE id = %i" % (zeit,datum,id)
    #print "MoveToFiling: " + stmt
    db.Execute(stmt)
    stmt = "DELETE FROM warenkorb WHERE id = %i" % id
    #print "MoveToFiling: " + stmt
    db.Execute(stmt)

#
# alt und gefaehrlich - NICHT nutzen
#
def UpdateWK(db):
    t = time.localtime(time.time())
    stmt = "UPDATE warenkorb " \
           "   SET zeit = '%02i:%02i:%02i', " % (t[3], t[4], t[5]) + \
           "       datum = '%04i-%02i-%02i', " % (t[0], t[1], t[2]) + \
           "       status = 2" \
           " WHERE status = 1"
    db.Execute(stmt)

def CalcTimeStr(t = 0, timestamp = 1):

    """ converts a timestring from format YYYYMMDD into timestamp """
    """ and timestamp into format YYYYMMDD """

    timestr = ""
    if timestamp == 1:
        t = string.replace(t,'-','')
        return time.mktime(int(t[0:4]), int(t[4:6]), int(t[6:8]), 0, 0, 0, 0, 0, 0)
    if timestamp == 0:
        if t == 0:
            tmp = time.localtime(time.time())
        if t != 0:
            tmp = time.localtime(t)
        timestr = '%04i%02i%02i' % (int(tmp[0]), int(tmp[1]), int(tmp[2]))
        return  timestr

def getIndexes(value):
    table = ''
    indexes = []
    if string.find(value, ';'):
        a = string.split(value, ';')
        table = str(a[0])
        for i in range(1, len(a)):
            indexes.append(string.strip(a[i]))
        return table, indexes
    else:
        return value, None

def DeleteRest(db, conf):
    """ set artikel status 0 if restzeit has reached """
    print t(), "UPDATING WARENKORB"
    restzeit = conf['restzeit']
    datanormzeit = conf['datanormzeit']
    stmt = "SELECT firma, kdnr, artnr, menge, preis, pd, zeit, datum, auftragnr, ring, status " \
           "  FROM warenkorb " \
           " WHERE ring != 'O' " \
           " AND   ring != 'S'"
    db.Execute(stmt)
    result = db.GetResult()
    fields = Fields(db)
    changed = 0
    print t(), "FOUND %i ELEMENTS TO CHECK FOR UPDATING" % len(result)
    for i in result:
        print i[7]
        if time.time() - CalcTimeStr(i[7], 1) > float(restzeit):
            stmt = "UPDATE artikel " \
                   "   SET status = '0' " \
                   " WHERE firma = '%s' " % i[0] + \
                   "   AND artnr = '%s'" % string.replace(str(i[2]), 'L', '')
            db.Execute(stmt)
            stmt = "DELETE FROM warenkorb " \
                   " WHERE firma = '%s' " % i[0] + \
                   "   AND kdnr = '%s' " % string.replace(str(i[1]), 'L', '') + \
                   "   AND artnr = '%s'" % string.replace(str(i[2]), 'L', '')
            changed = changed + 1
            db.Execute(stmt)
    print t(), "CHANGED %i ELEMENTS IN WARENKORB" % changed
    files = os.listdir(conf['tmpdir'])
    if len(files) > 0:
        print t(), "FOUND %i FILES IN DIRECTORY '%s' TO CHECK" % (len(files), conf['tmpdir'])
        delfile = 0
        for i in files:
            if os.stat(conf['tmpdir'] + '/' + i)[8] < (float(time.time()) - float(datanormzeit)):
                print t(), "FILE %s IS MORE THAN 11 DAYS OLD" % i
                kdnr = string.split(string.split(i, 'KD')[1],'.')[0]
                stmt = "DELETE FROM datanorm " \
                       "      WHERE kdnr = '%s'" % str(kdnr)
                print stmt
                print t(), "DATANORM DATA FOR kdnr = %s DELETED" % str(kdnr)
                os.system('rm %s/%s' % (conf['tmpdir'], i))
                print t(), "DELETED FILE %s/%s" % (conf['tmpdir'], i)
                delfile = delfile + 1
        print t(), "DELETED %i FILES AND DATA" % delfile
    else:
        print t(), "NO FILES FOUND IN %s" % conf['tmpdir']
    print t(), "STOP DELETING PROCESS"

def Export(db, conf):
    print t(), 'START EXPORT PROCESS'
    export = ''
    if GetData(db, 1) != '':
        export = export + GetData(db, 1)
    #print "Export type=1: export=" + export
    if GetData(db, 2) != '':
        export = export + GetData(db, 2)   # wird bei typ 2 direkt in datei geschrieben
    #print "Export type=2: export=" + export
    if GetData(db, 3) != '':
        export = export + GetData(db, 3)
    #print "Export type=3: export=" + export
    #UpdateWK(db) RB 20030712: soll nun aus GetData() fuer jeden datensatz einzeln aufgerufen werden
    getDataTempFile.Write()
    getDataTempFile.Close()
    #
    f = File()
    file = "%s.exp" % str(int(time.time()))
    f.SetFileName(file)
    f.SetMethod('w')
    f.Open()
    f.AddLine(export)
    f.Write()
    f.Close()
    print t(),'EXPORTED DATA WRITTEN INTO FILE: %s  ' % file
    print t(), 'EXIT'

def Import(db, conf, file):
    print t(), 'START IMPORT PROCESS'
    print
    p = Parser()
    print t(), 'READING FILE ', file
    a = open(file, 'r').readlines()
    print t(), 'READING COMPLETE'
    s = Statement()
    error = File()
    error.SetFileName('%s_import_errors.log' % file)
    error.SetMethod('a')
    error.Open()
    error.AddLine('%s' % CopyRight())
    error.Write()
    error.ClearLine()
    error.AddLine('%sBEGINNING OF ERROR LOG' % t())
    error.Write()
    error.ClearLine()
    lines = 1
    fields = Fields(db)
    ta = Tables()
    for i in a:
        p.SetLine(i)
        p.SplitLine()
        parsed = p.GetParsed()
        table, indexes = ta.CheckTable(parsed[0])
        if table:
            fieldlist = fields.GetFields(table)
            if parsed[1] == 'N':
                stmt = s.Insert(table, fieldlist, parsed[2])
                if stmt:
                    try:
                        db.Execute(stmt)
                    except (MySQLdb.Warning, MySQLdb.Error, MySQLdb.MySQLError), msg:
                        error.AddLine('%sDUPLICATE ENTRY, DATA OF LINE NO. %i IS NOT INSERTED' % (t(),lines))
                        error.Write()
                        error.ClearLine()
                        error.AddLine('%sMySQL ERROR -> %s' % (t(),msg))
                        error.Write()
                        error.ClearLine()
                        error.AddLine('%sFOLLOWING LINE CONTAINS THE ERROR: %s' % (t(), i))
                        error.Write()
                        error.ClearLine()
                else:
                    error.AddLine('%sERROR CREATING INSERT STATEMENT FOR LINE NO. %i' %(t(), lines))
                    error.Write()
                    error.ClearLine()
                    error.AddLine('%sFOLLOWING LINE CONTAINS THE ERROR: %s' % (t(), i))
                    error.Write()
                    error.ClearLine()
            if parsed[1] == 'U':
                stmt = s.Update(table, fieldlist, parsed[2], indexes)
                if stmt:
                    if table == 'artikel' and parsed[2][37] != 'O' and parsed[2][37] != 'S' and parsed[2][38] == 1:
                        data = GetData(db, 4, parsed[2][0], parsed[2][1], parsed[2][37])
                        if len(data) > 0:
                            print t(), 'FOUND REST IN warenkorb IN LINE % i' % lines
                            tmpstmt = "INSERT INTO temp_rest VALUES " \
                                      "            ( '%s', " % data[0] + \
                                      "              '%s', " % data[1] + \
                                      "              '%s', " % data[2] + \
                                      "              '%s') " % data[3]
                            db.Execute(tmpstmt)
                    try:
                        db.Execute(stmt)
                    except (MySQLdb.Warning, MySQLdb.Error, MySQLdb.MySQLError), msg:
                        error.AddLine('%sNO ENTRY FOUND FOR DATA IN LINE NO. %i. NOT UPDATED' % (t(), lines))
                        error.Write()
                        error.ClearLine()
                        error.AddLine('%sMySQL ERROR MESSAGE -> %s' % (t(), msg))
                        error.Write()
                        error.ClearLine()
                        error.AddLine('%s Line -> %s' % (t(), i))
                        error.Write()
                        error.ClearLine()
                        error.AddLine('%s SQL STATEMENT -> %s' % (t(), stmt))
                        error.Write()
                        error.ClearLine()
                else:
                    error.AddLine('%sDATA AND NO OF FIELDS ARE NOT OF THE SAME LENGHT IN LINE NO. %i' % (t(),lines))
                    error.Write()
                    error.ClearLine()
            if parsed[1] == 'D':
                stmt = s.Delete(table, fieldlist, parsed[2], indexes)
                if stmt:
                    if table == 'artikel' and parsed[2][37] != 'O' and parsed[2][37] != 'S' and parsed[2][38] == 1:
                        data = GetData(db, 4, parsed[2][0], parsed[2][1], parsed[2][37])
                        if len(data) > 0:
                            print t(), 'FOUND REST IN warenkorb AT LINE % i' % lines
                            tmpstmt = "INSERT INTO temp_rest VALUES " \
                                      "            ( '%s', " % data[0] + \
                                      "              '%s', " % data[1] + \
                                      "              '%s', " % data[2] + \
                                      "              '%s') " % data[3]
                            db.Execute(tmpstmt)
                    try:
                        db.Execute(stmt)
                    except (MySQLdb.Warning, MySQLdb.Error, MySQLdb.MySQLError), msg:
                        error.AddLine('%sNO SUCH ENTRY TO DELETE IN LINE NO. %i' % (t(),lines))
                        error.Write()
                        error.ClearLine()
                        error.AddLine('%sMySQL ERROR -> %s' % (t(), msg))
                        error.Write()
                        error.ClearLine()
                        error.AddLine('%sLINE -> %s' % (t(), i))
                        error.Write()
                        error.ClearLine()
                        error.AddLine('%sSQL STATEMENT -> %s' % (t(), stmt))
                        error.Write()
                        error.ClearLine()
                else:
                    error.AddLine('%sCOUNT OF FIELDS ARE NOT THE SAME AS DATA IN LINE NO. %i' % (t(),lines))
                    error.Write()
                    error.ClearLine()
        else:
            error.AddLine('%sTABLE SYNONYM ', parsed[0] , ' IN LINE NO. %i NOT FOUND IN CONFIGURATION FILE' % (t(),lines))
            error.ClearLine()
        lines = lines + 1
    error.AddLine('%sEND OF ERROR LOG' % t())
    error.Close()
    print t(), 'DELETING SOURCE FILE %s' % file
    os.system('rm %s' % file)
    print t(),'FINISHED WITH IMPORTING DATA'
    print t(),'ERRORS ARE WRITTEN IN FILE %s_import_error.log' % file
    
def howto():
    msg = "Wrong use of %s\n\n" % sys.argv[0]
    msg = "Correct use of %s\n" % sys.argv[0]
    msg = msg + "python %s type=<import/export> file=<location of file> project=<projectname>\n" % sys.argv[0]
    return msg

def checkOptions(options):
    o = {}
    for i in options:
        try:
            o[string.split(i, '=')[0]] = string.split(i, '=')[1]
        except:
            print howto()
    return o

def CopyRight():
    
    VERSION = '2.4.0'
    COPYRIGHT_SCRIPT = '#\n# 1Ci(R) GmbH, http://www.1ci.de\n' \
                       '# dmerce(R) %s\n' \
                       '# Copyright 2000-2004\n#\n' % VERSION
    return COPYRIGHT_SCRIPT
    
if __name__=='__main__':

    print CopyRight()
    print

    options = []

    """ check options given with program call """
    for i in range(1, len(sys.argv)):
        options.append(sys.argv[i])
    options = checkOptions(options)

    """ initialize needed variables """
    type = None
    file = None
    table = None
    dbname = None
    
    if options.has_key('type'):
        type = options['type']
    if options.has_key('file'):
        file = options['file']
    if options.has_key('project'):
        project = options['project']

    conf = check(project)

    if file and type and type != 'export':
        if not os.path.isfile(file):
            print t(), "SORRY, FILE %s NOT FOUND" % file
            print t(), "PASSING PROCESS AND EXITING"
            sys.exit()
    
    """ make connection to database """
    if project:
        db = DBHandler(conf['ctsr'])
    else:
        print t(), 'You need to specify a database'
        print
        print howto()
        sys.exit()
    
    db.Connect()
    db.SetCursor()
    if type:
        if type == 'delete':
            if os.path.isfile('/export/home/as400/as400delete.lock'):
                print t(), '/export/home/as400/as400delete.lock FOUND'
                print t(), 'ANOTHER PROGRAM IS RUNNING'
                print t(), 'EXIT'
                sys.exit()
            else:
                os.system('touch /export/home/as400/as400delete.lock')
            DeleteRest(db, conf)
            print t(), "RECENT DATA OUT OF warenkorb DELETED"
            os.system('rm /export/home/as400/as400delete.lock')
            sys.exit()
        if type == 'export':
            if os.path.isfile('/export/home/as400/as400export.lock'):
                print t(), '/export/home/as400/as400export.lock FOUND'
                print t(), 'ANOTHER PROGRAM IS RUNNING'
                print t(), 'EXIT'
                sys.exit()
            else:
                os.system('touch /export/home/as400/as400export.lock')
            Export(db, conf)
            print t(), "RECENT DATA OUT OF warenkorb EXPORTED"
            os.system('rm /export/home/as400/as400export.lock')
            sys.exit()
        if type == 'import':
            if file:
                if os.path.isfile('/export/home/as400/as400import.lock'):
                    print t(), '/export/home/as400/as400import.lock FOUND'
                    print t(), 'ANOTHER PROGRAM IS RUNNING'
                    print t(), 'EXIT'
                    sys.exit()
                else:
                    os.system('touch /export/home/as400/as400import.lock')
                Import(db, conf, file)
                os.system('rm /export/home/as400/as400import.lock')
            else:
                print t(), 'You have to specify a file that you want to %s' % type
                print t(), 'stopping process'
                print
                print howto()
                sys.exit()

    else:
        print t(), 'You have to specify the type "delete / import / export"'
        print t(), 'stopping process\n'
        sys.exit()

