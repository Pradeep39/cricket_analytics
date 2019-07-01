import requests
import sys
import re
from lxml import html
from bs4 import BeautifulSoup
import pandas as pd
from multiprocessing import Pool
from multiprocessing.pool import ThreadPool
from pyspark.sql.types import *
import time

def getMatchResults(year):
    match_results_url="http://stats.espncricinfo.com/ci/engine/records/team/match_results.html?class=2;id=%s;type=year"%year
    s=requests.get(match_results_url)
    table = html.fromstring(str(s.content))
    all_href_arr=table.xpath("//tr[contains(@class,'data1')]/td/a/@href")
    match_href_arr=[str(k) for k in all_href_arr if 'match' in k]
    df=pd.read_html(match_results_url)[0]
    df['Scorecard']=df['Scorecard'].str.extract(r'\s*([0-9]+)')
    df.rename(columns={'Team 1':'Team1','Team 2':'Team2',
                          'Match Date':'match_date', 'Scorecard':'match_no'}, 
                 inplace=True)
    df['match_url']=match_href_arr
    #df.astype(str)
    return(df)
    
def getMatchData(match_url):
    full_match_url="http://stats.espncricinfo.com"+match_url
    s=requests.get(full_match_url)
    table = html.fromstring(str(s.content))
    match_no=table.xpath("//div[div[@class='match-detail--left']//h4/text()='Match number']//a/text()")[0]
    innings1_team=table.xpath("//li[div/a/@href='#gp-inning-00']/div//h2/text()")
    innings2_team=table.xpath("//li[div/a/@href='#gp-inning-01']/div//h2/text()")
    #print(full_match_url)
    innings1_team=innings1_team[0] if len(innings1_team)>0 else ''
    innings2_team=innings2_team[0] if len(innings2_team)>0 else ''
    innings1_players_href=table.xpath("//li[div/a/@href='#gp-inning-00']//div[contains(@class,'batsmen')]//a[contains(@href,'http') and contains(@href,'player')]/@href")
    innings2_players_href=table.xpath("//li[div/a/@href='#gp-inning-01']//div[contains(@class,'batsmen')]//a[contains(@href,'http') and contains(@href,'player')]/@href")
    innings1_players_df=pd.DataFrame({})
    innings2_players_df=pd.DataFrame({})
    if(len(innings1_players_href)>0):
        innings1_players_df['player_page_href']=innings1_players_href[0:11]
        innings1_players_df['team']=innings1_team
    if(len(innings2_players_href)>0):
        innings2_players_df['player_page_href']=innings2_players_href[0:11]
        innings2_players_df['team']=innings2_team

    all_players_df=innings1_players_df.append(innings2_players_df)
    all_players_df=all_players_df.reset_index(drop=True)
    all_players_df['match_no']=re.search(r'\s*([0-9]+)',match_no).group(1)
    all_players_df['match_url']=match_url
    all_players_df['team']=all_players_df['team'].str.replace(' Innings','')
    all_players_df['player_page_href']=all_players_df['player_page_href'].str.replace('http://www.espncricinfo.com','')
    all_players_df['player_profile']=all_players_df['player_page_href'].str.extract(r'player\/(.*?).html')
    return(all_players_df)
    
def getPlayerSummary(player_profile):
    #try:
    df_list=pd.read_html("http://stats.espncricinfo.com/ci/content/player/%s.html" % (player_profile))
    batting_df=df_list[0]
    batting_filter_df=batting_df[batting_df.iloc[:,0]=='ODIs']
    batting_filter_df=batting_filter_df.iloc[:,1:]
    column_list=batting_filter_df.columns.values.tolist()
    if '4s' not in column_list:
        batting_filter_df.insert(loc=10, column='4s', value=0)
    if '6s' not in column_list:
        batting_filter_df.insert(loc=11, column='6s', value=0)
    bowling_df=df_list[1]
    bowling_filter_df=bowling_df[bowling_df.iloc[:,0]=='ODIs']
    bowling_filter_df=bowling_filter_df.iloc[:,1:].add_prefix('b_')
    player_df=pd.concat([batting_filter_df,bowling_filter_df],axis=1)
    player_df['player_profile']=player_profile
    if len(player_df.columns)!=28:
        print("http://stats.espncricinfo.com/ci/content/player/%s.html" % (player_profile))
    player_df=player_df.fillna(0)
    return player_df

def getCWCTeamData(team_squad_name, team_squad_url):
    team_squad_df=pd.DataFrame({})
    s=requests.get(team_squad_url)
    s_soup = BeautifulSoup(s.content, "html.parser")
    for div in s_soup.findAll("div", {"role":"main"}):
        for li in div.findAll("li"):
            player_details = {}
            player_img_src = li.find('img')['src']
            for h3 in li.findAll('h3'):
                a = h3.find('a', href=True)
                player_name=a.text.strip();
                player_profile=re.search(r'player\/(.*?).html', a['href']).group(1)
                h3_span=h3.find('span')
                if h3_span:
                    main_role = h3_span.text
                else:
                    main_role = 'player'
                player_details['PlayerID']=player_profile
                player_details['PlayerName']=player_name
                player_details['PlayerImg']="http://www.espncricinfo.com%s" % player_img_src
                player_details['PlayerMainRole']=main_role
                for span in h3.parent.findAll('span'):
                    if(span.parent.name!='h3'):
                      keyValues=span.text.split(':')
                      player_details[keyValues[0].strip()]=keyValues[1].strip()
            team_squad_df=team_squad_df.append(player_details, ignore_index=True)
            team_squad_df['Country']=team_squad_name.replace(" Squad", "")
    return(team_squad_df)
    
def getPlayerData( player_data, _type="batting", _class=2, _view="innings"):
    try:
        player_url="http://stats.espncricinfo.com/ci/engine/player/%s.html?class=%s;template=results;type=%s;view=%s\n" \
                % (player_data['PlayerID'],_class,_type,_view)
        #print(player_url)
        df=pd.read_html(player_url)
        if _type=="batting":
            cols = list(range(0,9))
            cols.extend([10,11,12])
        else:
            cols = list(range(0,7))
            cols.extend([8,9,10])
        df=df[3].iloc[:, cols]
        df['Country']=player_data['Country']
        df['PlayerID']=player_data['PlayerID']
        df['PlayerName']=player_data['PlayerName']
        df['RecordType']=_type
        df['PlayerImg']=player_data['PlayerImg']
        df['PlayerMainRole']=player_data['PlayerMainRole']
        df['Age']=player_data['Age']
        df['Batting']=player_data['Batting']
        df['Bowling']=player_data['Bowling']
        df['PlayerRole']=player_data['Playing role']
    except Exception:
      #print("Exception: No Innings Data for:"+player_url)
      df=pd.DataFrame({})
      pass
    return(df)
