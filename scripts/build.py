import json, urllib, re, sqlite3

#returns sqlite queries as dictionaries instead of lists
def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


dir = ""
url = "http://www.census.gov/population/projections/files/downloadables/NP2008_D1.csv"

#CSV format: HISP,RACE,SEX,YEAR,TOTAL_POP,POP_[0-99],
#Description: http://www.census.gov/population/projections/files/downloadables/filelayout_NP2008_D1.txt
HISP = [ "Total", "Not Hispanic", "Hispanic" ]
RACE = [ "All", "White", "Black", "AIAN", "Asian", "NHPI", "Two+", "White+", "Black+", "AIAN+", "Asian+", "NHPI+" ]
SEX  = [ "Both", "Male", "Female" ]

def get_data():
    no = 0
    data = []
    
    page = urllib.urlopen(url).read()
    #print page
    
    projs = re.split("\n", page)
    print len(projs)
    
    for line in projs:
        line = line.split(",")
        if no > 1 and len(line) > 1: #first line is labels   
            voting_age = 0
            for i in range(24, len(line)):
                voting_age += int(line[i])
                
        
            c.execute('''INSERT OR IGNORE INTO projections (year, hispanic, race, sex, total, voting) VALUES (?,?,?,?,?,?)''',
                        (int(line[3]), HISP[int(line[0])], RACE[int(line[1])], SEX[int(line[2])], int(line[4]), voting_age))
            conn.commit()
        no += 1
        
def get_year(y):
    return {
        'white': c.execute('''SELECT * from projections WHERE hispanic='Not Hispanic' AND race = 'White' AND sex = "Both" AND year = %i''' % y).fetchone()['voting'],
        'black': c.execute('''SELECT * from projections WHERE hispanic='Not Hispanic' AND race = 'Black' AND sex = "Both" AND year = %i''' % y).fetchone()['voting'],
        'asian': c.execute('''SELECT * from projections WHERE hispanic='Not Hispanic' AND race = 'Asian' AND sex = "Both" AND year = %i''' % y).fetchone()['voting'],
        'hispanic': c.execute('''SELECT * from projections WHERE hispanic='Hispanic' AND race = 'All' AND sex = "Both" AND year = %i''' % y).fetchone()['voting'],
        'multiple': c.execute('''SELECT * from projections WHERE hispanic='Total' AND race = 'Two+' AND sex = "Both" AND year = %i''' % y).fetchone()['voting']
    }        

conn = sqlite3.connect(dir + 'projections.sqlite')
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

get_data()

f = open(dir + "demographics.csv", "w")
f.write("year,white,black,hispanic,asian,multiple\r\n")

i = 2000

while i <= 2048:
    d = get_year(i)
    f.write("%i,%i,%i,%i,%i,%i\r\n" % (i,d['white'],d['black'],d['hispanic'],d['asian'],d['multiple']))
    i += 4
f.close()

conn.close()
