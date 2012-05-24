#001 
# - added support for Mongo record fields in filemaker
# - 
# - 
from fmkr import FM
from pymongo import Connection, DESCENDING, ASCENDING
import pprint
import sys
from datetime import *
class FMDB_Compare():
    def __init__(self, username, password, host="127.0.0.1", port=80, protocol="http" ):
        self.f = FM(host, port, protocol)
        self.f.set_db_password(username, password)
    def to_unicode(self, bad_string):
        if(bad_string == None):
            return None
        if(self.isstr( bad_string)):
            strcleaned = ''.join([x for x in bad_string if ord(x) < 128])
            return strcleaned.strip().encode('utf-8')
        elif(self.isunicode( bad_string)):
            strcleaned = ''.join([x for x in bad_string if ord(x) < 128])
            return strcleaned.strip().encode('utf-8')
        elif(self.islist( bad_string)):
            listcount = 0
            newstring = ""
            for item in bad_string:
                newstring = newstring + self.to_unicode(item)
                listcount = listcount + 1
            newstring2 = self.to_unicode( newstring )
            if newstring2 == None or newstring2 == "":
                return None
            print newstring2
            return newstring2
        elif( self.isint( bad_string ) ):
            bad_string = str(bad_string)
            return self.to_unicode( bad_string )
        else:
            print "bad_string is type: "
            print type(bad_string)
    def SearchAndCompareAll(self, db1name, db1layout, db2name, db2layout, maxrecords=500):
        print "searching db1:"
        db1 = self.FindAllRecords(db1name, db1layout, maxrecords)
        print "searching db2:"
        db2 = self.FindAllRecords(db2name, db2layout, maxrecords)
        self.CompareDatabases(db2, db1)
    #If Record Id's don't match, echo & exit!
    def CompareRecordIDs(self, rid1, rid2, autoexit=True):
        if rid1 != rid2:
            print ""
            print "----------------------------------------------"
            print ""
            print "Record ID's DONT MATCH!"
            print ""
            print rid1 + " - " + rid2
            print ""
            print "----------------------------------------------"
            print ""
            if(autoexit):
                exit
    def CompareDatabases(self, db1, db2, limit=500):
        print "Comparing Databases:"
        count = 0
        for row in db1:
            if count > limit:
                break
            print "     ==================================    "
            print "Row:   "
            print "     ==================================    "
            for key, value in row.iteritems():
                if self.to_unicode(key) == "RECORDID":
                    self.CompareRecordIDs(self.to_unicode(value), self.to_unicode(db2[count]["RECORDID"]))
                newvalue = self.to_unicode(value)
                newvalue2 = self.to_unicode(db2[count][key])
                self.isDif(newvalue, newvalue2, key)
            count = count + 1
        print count
    def FindAllRecords(self, database, layout, maxrecords=100):
        self.f.set_db_data(database, layout, maxret=maxrecords)
        r = self.f.fm_find_all()
        return r.resultset
    def IterateColumns(self,columns):
        for columnn in columns:
            yield column
    def isDif(self, fielda, fieldb, key):
        if(fielda is not None):
            if(fieldb is not None):
                if(fielda == fieldb):
                    return False
                else:
                    print "Value Difference - " + key + ": " + fielda + " <> " + fieldb
                    return True
                
            else: 
                print "Value Difference - " + key + ": " + fielda + " <> None"
                return True
        else:
            if(fielda is None and fieldb is None):
                return False
            else:
                print "Value Difference - " + key + ": None <> " + fieldb
                return True
    def to_fm_datetime(self, dt):
        if dt != None:
            return datetime.strftime(dt, '%m/%d/%Y %H:%M:%S')
        else:
            return None
    def isint(self, stri):
        if type(stri) == type(int()):
            return True
        else:
            return False    
    def isstr(self, stri):
        if type(stri) == type(str()):
            return True
        else:
            return False
    def ischr(self, chr):
        if type(chr) == type(chr()):
            return True;
        else:
            return False;
    def islist(self, lis):
        if type(lis) == type(list()):
            return True
        else:
            return False
    def isunicode(self, uni):
        if type(uni) == type(unicode()):
            return True
        else:
            return False    

if __name__ == '__main__':
    a = FMDB_Compare("username", "password", "197.0.0.1", 80, "http")
    a.SearchAndCompareAll("Database1", "LayoutOnDatabase1", "Database2", "LayoutOnDatabase2")