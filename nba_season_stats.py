#! /usr/bin/python -O 
# Jason Matthew Torres 
'''
Retrieve Season Stats for the San Antonio Spurs from Basketball-Reference.com 
Usage: python JTspurs_season_stats.py 

Webpage with Spurs 2015 season game listing:
http://www.basketball-reference.com/teams/SAS/2015_games.html

Example of webpage with game statistics:
http://www.basketball-reference.com/boxscores/201410280SAS.html
Note: These can be accessed by clicking "Box Score" links on the game listing page 
'''
# libraries 
import sys, os, re, sqlite3
from urllib import urlopen 
from bs4 import BeautifulSoup

# globals 
cout = sys.stdout.write
cerr = sys.stderr.write 
main_page = "http://www.basketball-reference.com/teams/SAS/2015_games.html"
game_page = "http://www.basketball-reference.com/boxscores/201411110GSW.html"

createDb = sqlite3.connect('season_2014-15.db')
queryCurs = createDb.cursor()


# functions
# Web Scraping 
def game_info(main_page):
    '''
    Create Dictionary of Spurs Season Game Info from game listing webpage 
    '''
    webpage = urlopen(main_page).read() 
    regex = re.compile('align="center" >([WL])<')
    WLlist = re.findall(regex,webpage)
    soup = BeautifulSoup(webpage)
    table_grab = soup.find_all("table",{"id":"teams_games"})
    table_soup = BeautifulSoup(str(table_grab[0]))  
    a_tags = table_soup.find_all("a")
    date_list,team_list,html_list = [],[],[] 
    for tag in a_tags:
        tstring = str(tag.text)
        try:
            year = int(tstring[-4:]) # Only can be run for dates 
            tstring = tstring.replace(" ","")
            tlist = tstring.split(",")
            date = "_".join(tlist)
            date_list.append(date)
        except(ValueError): 
            if tstring == 'Box Score':
                tstring = str(tag)
                patt = re.compile(r'href=("/boxscores/.*html")')
                relist = re.findall(patt,tstring)
                html = relist[0].replace('"','')
                html = "http://www.basketball-reference.com" + html 
                html_list.append(html)
            elif len(tstring) == 0:
                pass
            elif tstring[-5:] == "TiqIQ":
                pass 
            else:
                team = tstring.replace(" ","_")
                team_list.append(team)
    gdic = {} 
    for i in range(0,len(html_list)):
        game_num = str(i+1)
        gdic[game_num] = [ date_list[i], team_list[i], html_list[i], WLlist[i]] 
    return gdic   
def stat_scrape_basic(game_page):
    '''
    Get Spurs stat info for a particular Game
    '''     
    webpage = urlopen(game_page).read()
    soup = BeautifulSoup(webpage)
    # Basic Stats Table 
    basic_table = soup.find_all("table",{"id":"SAS_basic"})
    basic_string = str(basic_table[0])
    basic_soup = BeautifulSoup(basic_string)
    basic_tags = basic_soup.find_all("tr")    
    player_dic = {}  
    for tag in basic_tags:
        tlist = str(tag.text).strip().replace("%","pct").replace("+/-","PlusMinus").split("\n") 
        tlist = ["0" if x=='' else x for x in tlist]
        if len(tlist) == 21 and tlist[0] != "Starters" and tlist[0] != "Reserves":
            name = tlist[0].replace(" ","_")
            player_dic[name] = tlist[1:]
        elif tlist[0] == "Starters":
            tlist[0] = "Player"
            basic_head_list = tlist
        elif len(tlist) == 20:
            team_total_list = tlist
        elif tlist[0] != "Reserves":
            name = tlist[0].replace(" ","_")
            injury_list = (["NA"] * 20)
            player_dic[name] = injury_list 
        else:
            pass 
    return player_dic, basic_head_list, team_total_list
def stat_scrape_advanced(game_page):
    '''
    Get Spurs stat info for a particular Game
    '''     
    webpage = urlopen(game_page).read()
    soup = BeautifulSoup(webpage)
    # Advanced Stats Table 
    advanced_table = soup.find_all("table",{"id":"SAS_advanced"})
    advanced_string = str(advanced_table[0])
    advanced_soup = BeautifulSoup(advanced_string)
    advanced_tags = advanced_soup.find_all("tr")    
    player_dic = {}  
    for tag in advanced_tags:
        tlist = str(tag.text).strip().replace("%","pct").replace("+/-","PlusMinus").split("\n") 
        tlist = ["0" if x=='' else x for x in tlist]
        if len(tlist) == 16 and tlist[0] != "Starters" and tlist[0] != "Reserves" and tlist[0] != "Team Totals":
            name = tlist[0].replace(" ","_")
            player_dic[name] = tlist[1:]
        elif tlist[0] == "Starters":
            tlist[0] = "Player"
            advanced_head_list = tlist
        elif tlist[0] == "Team Totals":
            name = tlist[0].replace(" ","_")
            tlist[0] = name
            team_total_list = tlist
        elif tlist[0] != "Reserves":
            name = tlist[0].replace(" ","_")
            injury_list = (["NA"] * 15)
            player_dic[name] = injury_list 
        else:
            pass 
    return player_dic, advanced_head_list, team_total_list    
# Databasing 

# Create Table Functions 
def create_teamtotal_table_basic(table_name):
    '''
    Creates a sql table for the 16 team total variables 
    '''
    queryCurs.execute('''CREATE TABLE %s 
    (date TEXT,gameNum INTEGER PRIMARY KEY, outcome TEXT,mp NUMERIC,fg NUMERIC,fga NUMERIC,fGpct NUMERIC,
     threeP NUMERIC,threePA NUMERIC,threePpct NUMERIC,ft NUMERIC,fta NUMERIC,
     fTpct NUMERIC,orb NUMERIC,drb NUMERIC,trb NUMERIC,ast NUMERIC,stl NUMERIC,
     blk NUMERIC,tov NUMERIC,pf NUMERIC,pts NUMERIC)''' % table_name)
def create_teamtotal_table_advanced(table_name):
    '''
    Creates a sql table for the 16 team total variables 
    '''
    queryCurs.execute('''CREATE TABLE %s 
    (date TEXT,gameNum INTEGER PRIMARY KEY,outcome TEXT,mp NUMERIC,tSpct NUMERIC,eFPpct NUMERIC,threePAr NUMERIC,
     fTr NUMERIC,oRBpct NUMERIC,dRBpct NUMERIC,tRBpct NUMERIC,aSTpct NUMERIC,
     sTLpct NUMERIC,bLKpct NUMERIC,toVpct NUMERIC,uSGpct NUMERIC,oRtg NUMERIC,dRtg NUMERIC)''' % table_name) 
def create_game_table_basic(table_name):
    '''
    Creates a sql table for basic game statistics  
    '''
    queryCurs.execute('''CREATE TABLE %s 
    (player TEXT PRIMARY KEY,mp TEXT,fg NUMERIC,fga NUMERIC,fGpct NUMERIC,
     threeP NUMERIC,threePA NUMERIC,threePpct NUMERIC,ft NUMERIC,fta NUMERIC,
     fTpct NUMERIC,orb NUMERIC,drb NUMERIC,trb NUMERIC,ast NUMERIC,stl NUMERIC,
     blk NUMERIC,tov NUMERIC,pf NUMERIC,pts NUMERIC,plusMinus TEXT)''' % table_name)
def create_game_table_advanced(table_name):
    queryCurs.execute('''CREATE TABLE %s 
    (player TEXT PRIMARY KEY,mp NUMERIC,tSpct NUMERIC,eFPpct NUMERIC,threePAr NUMERIC,
     fTr NUMERIC,oRBpct NUMERIC,dRBpct NUMERIC,tRBpct NUMERIC,aSTpct NUMERIC,
     sTLpct NUMERIC,bLKpct NUMERIC,toVpct NUMERIC,uSGpct NUMERIC,oRtg NUMERIC,dRtg NUMERIC)''' % table_name) 

# Add Entry to Table Functions 
def add_date_tt_basic(table_name,date,gameNum,outcome,mp,fg,fga,fGpct,threeP,threePA,threePpct,ft,fta,
                fTpct,orb,drb,trb,ast,stl,blk,tov,pf,pts):
    string = '''INSERT INTO %s (date,gameNum,outcome,mp,fg,fga,fGpct,threeP,threePA,threePpct,ft,
                fta,fTpct,orb,drb,trb,ast,stl,blk,tov,pf,pts)
                values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''' % (table_name) 
    queryCurs.execute(string,(date,gameNum,outcome,mp,fg,fga,fGpct,threeP,threePA,threePpct,ft,
                fta,fTpct,orb,drb,trb,ast,stl,blk,tov,pf,pts))                    

def add_date_tt_advanced(table_name,date,gameNum,outcome,mp,tSpct,eFPpct,threePAr,
                         fTr,oRBpct,dRBpct,tRBpct,aSTpct,sTLpct,bLKpct,toVpct,uSGpct,oRtg,dRtg):
    string = '''INSERT INTO %s (date,gameNum,outcome,mp,tSpct,eFPpct,threePAr,
                fTr,oRBpct,dRBpct,tRBpct,aSTpct,sTLpct,bLKpct,toVpct,uSGpct,oRtg,dRtg)
                values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''' % (table_name) 
    queryCurs.execute(string,(date,gameNum,outcome,mp,tSpct,eFPpct,threePAr,
                fTr,oRBpct,dRBpct,tRBpct,aSTpct,sTLpct,bLKpct,toVpct,uSGpct,oRtg,dRtg)) 

def add_game_basic(table_name,player,mp,fg,fga,fGpct,threeP,threePA,threePpct,ft,fta,
                fTpct,orb,drb,trb,ast,stl,blk,tov,pf,pts,plusMinus):
    string = '''INSERT INTO %s (player,mp,fg,fga,fGpct,threeP,threePA,threePpct,ft,
                fta,fTpct,orb,drb,trb,ast,stl,blk,tov,pf,pts,plusMinus)
                values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''' % (table_name) 
    queryCurs.execute(string,(player,mp,fg,fga,fGpct,threeP,threePA,threePpct,ft,
                fta,fTpct,orb,drb,trb,ast,stl,blk,tov,pf,pts,plusMinus)) 

def add_game_advanced(table_name,player,mp,tSpct,eFPpct,threePAr,
                         fTr,oRBpct,dRBpct,tRBpct,aSTpct,sTLpct,bLKpct,toVpct,uSGpct,oRtg,dRtg):
    string = '''INSERT INTO %s (player,mp,tSpct,eFPpct,threePAr,
                fTr,oRBpct,dRBpct,tRBpct,aSTpct,sTLpct,bLKpct,toVpct,uSGpct,oRtg,dRtg)
                values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''' % (table_name) 
    queryCurs.execute(string,(player,mp,tSpct,eFPpct,threePAr,
                fTr,oRBpct,dRBpct,tRBpct,aSTpct,sTLpct,bLKpct,toVpct,uSGpct,oRtg,dRtg))

# Build Database Functions 
def build_db_teamtotals_basic(tname):
    #tname = "season2014_2015"
    game_dic = game_info(main_page)
    create_teamtotal_table_basic(tname)
    success = 0 
    for game in range(1,83):
        try:
            game = str(game) 
            date,game_page,outcome = game_dic[game][0],game_dic[game][2],game_dic[game][3]
            player_dic,header,tt = stat_scrape_basic(game_page) 
            mp,fg,fga,fGpct,threeP,threePA = tt[1],tt[2],tt[3],tt[4],tt[5],tt[6]
            threePpct,ft,fta,fTpct,orb,drb = tt[7],tt[8],tt[9],tt[10],tt[11],tt[12]
            trb,ast,stl,blk,tov,pf,pts = tt[13],tt[14],tt[15],tt[16],tt[17],tt[18],tt[19]   
        except(IndexError):
            cout("The list lengths do not agree\n") 
        except(KeyError):
            cout("Game %s has not happened yet\n" % game)   
        else:    
            try:
                add_date_tt_basic(tname,date,game,outcome,mp,fg,fga,fGpct,threeP,threePA,threePpct,ft,fta,fTpct,orb,drb,trb,ast,stl,blk,tov,pf,pts)
                createDb.commit()
                success += 1   
            except:
                cout("Failed: %d\n" % date)
    status = r"Number of games successfully added: %d" % success
    cout(status+"\n") 
def build_db_teamtotals_advanced(tname):
    #tname = "season2014_2015"
    game_dic = game_info(main_page)
    create_teamtotal_table_advanced(tname)
    success = 0 
    for game in range(1,83):
        try:
            game = str(game) 
            date,game_page,outcome = game_dic[game][0],game_dic[game][2],game_dic[game][3]
            player_dic,header,tt = stat_scrape_advanced(game_page) 
            mp,tSpct,eFPpct,threePAr,fTr,oRBpct = tt[1],tt[2],tt[3],tt[4],tt[5],tt[6]
            dRBpct,tRBpct,aSTpct,sTLpct,bLKpct,toVpct = tt[7],tt[8],tt[9],tt[10],tt[11],tt[12]
            uSGpct,oRtg,dRtg = tt[13],tt[14],tt[15]
        except(IndexError):
            cout("The list lengths do not agree\n") 
        except(KeyError):
            cout("Game %s has not happened yet\n" % game)   
        else:    
            try:
                add_date_tt_advanced(tname,date,game,outcome,mp,tSpct,eFPpct,threePAr,fTr,oRBpct,dRBpct,tRBpct,aSTpct,sTLpct,bLKpct,toVpct,uSGpct,oRtg,dRtg)
                createDb.commit()
                success += 1   
            except:
                cout("Failed: %s\n" % date)
    status = r"Number of games successfully added: %d" % success
    cout(status+"\n")
def build_db_games_basic():
    game_dic = game_info(main_page)
    success = 0 
    for game in range(1,83):
        try:
            game = str(game) 
            tname = "game_"+game+"_basic"
            create_game_table_basic(tname)
            date,game_page,outcome = game_dic[game][0],game_dic[game][2],game_dic[game][3]
            p,header,tt = stat_scrape_basic(game_page) 
            player_list = p.keys()
            player_list.remove('Basic_Box_Score_Stats')
            for player in player_list:
                try:
                    mp,fg,fga = p[player][0],p[player][1],p[player][2]
                    fGpct,threeP,threePA = p[player][3],p[player][4],p[player][5]
                    threePpct,ft,fta = p[player][6],p[player][7],p[player][8]
                    fTpct,orb,drb = p[player][9],p[player][10],p[player][11]
                    trb,ast,stl = p[player][12],p[player][13],p[player][14] 
                    blk,tov,pf = p[player][15],p[player][16],p[player][17] 
                    pts,plusMinus = p[player][18],p[player][19]
                except(IndexError):
                    cout("The list lengths do not agree\n") 
                else:    
                    try:
                        add_game_basic(tname,player,mp,fg,fga,fGpct,threeP,threePA,threePpct,ft,fta,fTpct,orb,drb,trb,ast,stl,blk,tov,pf,pts,plusMinus)
                        createDb.commit()
                    except:
                        cout("Failed: %d\n" % date)
            success += 1 
        except:
            cout("Game %s has not happened yet\n" % game)   
    status = r"Number of games successfully added: %d" % success
    cout(status+"\n") 

def build_db_games_advanced():
    game_dic = game_info(main_page)
    success = 0 
    for game in range(1,83):
        try:
            game = str(game) 
            tname = "game_"+game+"_advanced"
            create_game_table_advanced(tname)
            date,game_page,outcome = game_dic[game][0],game_dic[game][2],game_dic[game][3]
            p,header,tt = stat_scrape_advanced(game_page) 
            player_list = p.keys()
            player_list.remove('Advanced_Box_Score_Stats')
            for player in player_list:
                try:
                    mp,tSpct,eFPpct = p[player][0],p[player][1],p[player][2]
                    threePAr,fTr,oRBpct = p[player][3],p[player][4],p[player][5]
                    dRBpct,tRBpct,aSTpct = p[player][6],p[player][7],p[player][8]
                    sTLpct,bLKpct,toVpct = p[player][9],p[player][10],p[player][11]
                    uSGpct,oRtg,dRtg = p[player][12],p[player][13],p[player][14] 
                except(IndexError):
                    cout("The list lengths do not agree\n") 
                else:    
                    try:
                        add_game_advanced(tname,player,mp,tSpct,eFPpct,threePAr,fTr,oRBpct,dRBpct,tRBpct,aSTpct,sTLpct,bLKpct,toVpct,uSGpct,oRtg,dRtg)
                        createDb.commit()
                    except:
                        cout("Failed: %d\n" % date)
            success += 1 
        except:
            cout("Game %s has not happened yet\n" % game)   
    status = r"Number of games successfully added: %d" % success
    cout(status+"\n") 

# Query Database / Write File Functions 
def make_tt_file(table_name, outfile):
    fout = open(outfile,'w')
    command = '''SELECT * FROM %s ''' % table_name
    queryCurs.execute(command)
    col_name_list = [tuple[0] for tuple in queryCurs.description]
    headline = "\t".join(col_name_list)
    fout.write(headline+"\n")
    queryCurs.execute(command) # NOTE use queryCurs.execute(command, [value]) when using WHERE something = ? 
    for tup in queryCurs:
        lst = list(tup) 
        for i in range(0,len(lst)):
            lst[i] = str(lst[i])
        line = "\t".join(lst)
        fout.write(line+"\n") 
    fout.close()
    
        
            
def main():
    pass 

if (__name__=="__main__"): game_info(main_page)  
