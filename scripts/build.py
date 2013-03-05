import json, urllib, re, sqlite3, os

#returns sqlite queries as dictionaries instead of lists
def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

dir = os.getcwd()

def fetch_data():
    url = "http://www.census.gov/population/projections/files/downloadables/NP2012_D1.csv"
    f = open(dir + "/data/projections.csv", 'w')
    f.write(urllib.urlopen(url).read())
    f.close()

#CSV format: HISP,RACE,SEX,YEAR,TOTAL_POP,POP_[0-99],
#Description: http://www.census.gov/population/projections/files/downloadables/filelayout_NP2008_D1.txt
HISP = [ "Total", "Not Hispanic", "Hispanic" ]
RACE = [ "All", "White", "Black", "AIAN", "Asian", "NHPI", "Two+", "White+", "Black+", "AIAN+", "Asian+", "NHPI+" ]
SEX  = [ "Both", "Male", "Female" ]

def get_data():
    no = 0
    data = []
        
    projs = open(dir + "/data/projections.csv", "r").readlines()
    
    for line in projs:
        line = line.split(",")
        if no > 0 and len(line) > 1: #first line is labels   
            voting_age = 0
            for i in range(24, len(line)):
                voting_age += int(line[i])
                
        
            c.execute('''INSERT OR IGNORE INTO projections (year, hispanic, race, sex, total, voting) VALUES (?,?,?,?,?,?)''',
                        (int(line[3]), HISP[int(line[0])], RACE[int(line[1])], SEX[int(line[2])], int(line[4]), voting_age))
            conn.commit()
        no += 1
        
def get_year(y, ind="voting"):
    return {
        'white': c.execute('''SELECT * from projections WHERE hispanic='Not Hispanic' AND race = 'White' AND sex = "Both" AND year = %i''' % y).fetchone()[ind],
        'black': c.execute('''SELECT * from projections WHERE hispanic='Not Hispanic' AND race = 'Black' AND sex = "Both" AND year = %i''' % y).fetchone()[ind],
        'asian': c.execute('''SELECT * from projections WHERE hispanic='Not Hispanic' AND race = 'Asian' AND sex = "Both" AND year = %i''' % y).fetchone()[ind],
        'hispanic': c.execute('''SELECT * from projections WHERE hispanic='Hispanic' AND race = 'All' AND sex = "Both" AND year = %i''' % y).fetchone()[ind],
        'total': c.execute('''SELECT * from projections WHERE hispanic='Total' AND race = 'All' AND sex = "Both" AND year = %i''' % y).fetchone()[ind]
    }        

conn = sqlite3.connect(dir + '/data/projections.sqlite')
conn.row_factory = dict_factory
c = conn.cursor()

c.execute('CREATE TABLE IF NOT EXISTS projections \
           ("id" INTEGER PRIMARY KEY AUTOINCREMENT, \
            "year" INTEGER, \
            "hispanic" VARCHAR(20), \
            "race" VARCHAR(10), \
            "sex" VARCHAR(10), \
            "total" INTEGER, \
            "voting" INTEGER, \
            CONSTRAINT "unq" UNIQUE (year, hispanic, race, sex))')

conn.commit()

def write_data():
    f = open(dir + "/data/population.csv", "w")
    f.write("year,white,black,hispanic,asian,total\r\n")
    
    i = 2012
    
    while i <= 2060:
        print i
        d = get_year(i, "voting")
        f.write("%i,%i,%i,%i,%i,%i\r\n" % (i,d['white'],d['black'],d['hispanic'],d['asian'],d['total']))
        i += 1
    
    f.close()


#fetch_data()
#get_data()
write_data()

conn.close()
