
import pandas as pd
from urllib import request
from bs4 import BeautifulSoup
from tqdm import tqdm
import MySQLdb
from MySQLdb import OperationalError
from MySQLdb import ProgrammingError
from urllib.error import URLError, HTTPError
import time
from sqlalchemy import create_engine
from datetime import datetime

class Read_HTML:
    
    def __init__(self, url_1, code):
        self.url = None
        self.url_1 = url_1
        self.code = str(code)
        self.removed_code_list = []
        self.name = None
        self.info_dict = None

        soup = self.read_html()
        self.co_name = soup.find('div', class_='si_i1_1').h2.get_text()
        
    def read_html(self): 
        self.url = self.url_1 + str(self.code)
        try:
            response = request.urlopen(self.url)
        except HTTPError:
            return None
        soup = BeautifulSoup(response, features="html.parser")
        response.close()
        
        return soup

    def get_stock_info(self):
        try:
            container = self.read_html().find('div', id='container')
        except AttributeError:
            return None

        if container.find('section', id='stockinfo', class_='clearfix') == None or container.find('div', id='kobetsu_left') == None:
            self.removed_code_list.append(self.code)
            return None



        keys_list = ['企業名', '証券コード', '株価', '変化', '変化率', '業種', 'per', 'pbr', '利回り', '信用倍率', '時価総額', '日時', '始まり値',
        '高値', '安値', '終値', '出来高（株）', '売買代金（百万）', 'VWAP', '約定回数', '売買最低代金', '発行済み株式数（株株株株株株）', '五日', '二十五日', '七十五日', '二百日', '業績推移']
         
        try:
            list_1 = self.info_1(container)
        except AttributeError:
            list_1 = [None for _ in range(11)]
            self.removed_code_list.append(self.code)
        try:
            list_2 = self.info_2(container)
        except AttributeError:
            list_2 = [None for _ in range(11)]  
        try:  
            list_3 = self.info_3(container)
        except AttributeError:
            list_3 = [None for _ in range(5)]
        values_list = list_1 + list_2 + list_3


        self.info_dict = dict(zip(keys_list, values_list))

        return self.info_dict
    
    def info_1(self, container):
        si_i1 = container.find('div', class_='si_i1_1')
        name = si_i1.h2.text[4:]
        stock_value = container.find('span', class_='kabuka').text.replace(',', '')

        change = container.find('dl', class_='si_i1_dl1').find_all('dd')
        up_down = change[0].text
        up_down_rate = change[1].text
        stock_info = container.find('div', id='stockinfo_i2')
        industry = stock_info.find('div').text.replace('\n', '')

        stock_info3 = container.find('div', id='stockinfo_i3')
        indexes = stock_info3.find_all('td')
        per = indexes[0].text.replace('倍', '')
        pbr = indexes[1].text.replace('倍','')
        yeild = indexes[2].text
        margin_rate = indexes[3].text
        market_capital = indexes[4].text
        info_list_1 = [name, self.code, stock_value, up_down, up_down_rate, industry, per, pbr, yeild, margin_rate, market_capital]

        return info_list_1
        
    def info_2(self, container):
        kobetsu_left = container.find('div', id='kobetsu_left')
        year = datetime.now().strftime('%Y年')
        
        date = year + kobetsu_left.find('h2').text

        tables = kobetsu_left.find_all('table')
        values_1 = tables[0].tbody.find_all('tr')

        start_value = values_1[0].td.text.replace(',', '')
        max_value = values_1[1].td.text.replace(',', '')
        min_value = values_1[2].td.text.replace(',', '')
        end_value = values_1[3].td.text.replace(',', '')

        values_2 = tables[1].tbody.find_all('tr')

        volume = values_2[0].td.text.replace(',', '')
        trade_price = values_2[1].td.text
        vwap = values_2[2].td.text.replace(',', '')
        contract_count = values_2[3].td.text
        min_trade_price = values_2[4].td.text.replace(',', '')
        issued_shares = values_2[7].td.text.replace(',', '').replace('株', '')

        info_list_2 = [date, start_value, max_value, min_value, end_value, volume, trade_price, vwap, contract_count, min_trade_price, issued_shares]

        return info_list_2

    def info_3(self, container):
        kobetsu_right = container.find('div', id='kobetsu_right')
        kabuka_trend = kobetsu_right.find('div', class_='kabuka_trend clearfix')
        trends = kabuka_trend.table.tbody.find_all('tr')[2].find_all('td')

        five_days_before = trends[0].span.text
        twenty_five_days_before = trends[1].span.text
        seventy_five_days_before = trends[2].span.text
        two_hundreds_days_before = trends[3].span.text
        try:
            data = []

            gyouseki = kobetsu_right.find('div', class_='gyouseki_block').table
            headers= [h.text for h in gyouseki.thead.tr.find_all('th')]
            rows = gyouseki.tbody.find_all('tr')

            for i in range(len(rows)):
                list_line = [h.text for h in rows[i].find_all('td')]
                list_line.insert(0, rows[i].th.get_text())
                data.append(list_line)

            df = pd.DataFrame(data, columns=headers)
        except AttributeError:
            df = None

        info_list_3 = [five_days_before, twenty_five_days_before, seventy_five_days_before, two_hundreds_days_before, df]

        return info_list_3

class Read_HTML_FINANCE:

    def __init__(self, url_2, code):
        self.code = code
        self.url_2 = url_2
        self.url = None
        self.df_dict = None

    def read_html_f(self):
        self.url = self.url_2 + str(self.code)
        try:
            response = request.urlopen(self.url)
        except HTTPError:
            print('HTTPエラー')
            return None

        soup = BeautifulSoup(response, features="html.parser")
        response.close()

        return soup

    def get_finance_info(self):

        try:
            finance = self.read_html_f().find('div', id='finance_box')
        except AttributeError:
            print('AttributeError')
            return None
        if finance==None:
            return None
            
        df_list = self.info_1_f(finance)
        column_list = ['業績推移', '収益性', '上期or下期業績', '３ヶ月決算', '過去最高業績', 'CF推移', '今期業績（累計）', '財務']
        self.df_dict = dict(zip(column_list, df_list)) 

        return self.df_dict

    def table_to_pandas(self, table_object):
        table_head = table_object.thead.tr.find_all('th')   
        head_list = [th.text for th in table_head] 
        table_badies = table_object.tbody.find_all('tr')

        bady_list = []
        for tr in table_badies:
            tds = tr.find_all('td')
            if len(tds) + 1 != len(head_list):
                continue
            else:
                row = [tr.th.get_text()]
                row = row + [td.text for td in tds]
                bady_list.append(row)
        df = pd.DataFrame(bady_list, columns=head_list)

        return df     
                
    def info_1_f(self, finance):
        table = finance.find('div', class_='fin_f_t0_d fin_f_t1_d').table
        df_trend_result = self.table_to_pandas(table) 

        table = finance.find('div', class_='fin_f_t0_d fin_f_t4_d dispnone').table
        df_profitability = self.table_to_pandas(table)

        table = finance.find('div', class_='fin_h_t0_d fin_h_t1_d').table
        df_present_result = self.table_to_pandas(table)

        table = finance.find('div', class_='fin_q_t0_d fin_q_t1_d')
        df_three_month_result = self.table_to_pandas(table)

        
        tables = finance.find_all('table', recursive=False)
        df_list = [df_trend_result, df_profitability, df_present_result, df_three_month_result] + self.table_name(tables, finance)

        return df_list
        
    def table_name(self, tables, finance):
        cap1s = [div.get_text() for div in finance.find_all('div', class_='cap1')]
        cap_list = [None for _ in range(4)]
        for i in range(len(tables)):
            
            if cap1s[i] == '過去最高 【実績】':
                cap_list[0] = self.table_to_pandas(tables[i])
            elif cap1s[i] == '通期':
                cap_list[1] = self.table_to_pandas(tables[i])
            elif cap1s[i] == '第１四半期累計決算【実績】' or cap1s[i] == '第２四半期累計決算【実績】' or cap1s[i] == '前期【実績】' or cap1s[i] == '第３四半期累計決算【実績】':
                cap_list[2] = self.table_to_pandas(tables[i])
            elif cap1s[i] == '財務 【実績】':
                cap_list[3] = self.table_to_pandas(tables[i])
            else:
                print('There is not a caption:', cap1s[i])
                
        return cap_list

            
class Create_DB:

    def __init__(self, code, co_name):
        self.code = code
        self.co_name = co_name
        self.conn = MySQLdb.connect(
            user='root',
            passwd='mukatitech2019',
            host='localhost'
            )
        self.db_name = None

    def create_db(self):
        cursor = self.conn.cursor()
        query = str('CREATE DATABASE {}_db'.format(self.co_name))
        self.db_name = '{}_db'.format(str(self.co_name))
        try:
            cursor.execute(query)
            return True
        except ProgrammingError:
            return False
        except OperationalError:
            return False

    def create_table(self, info_dict):
        cursor = self.conn.cursor()
        query = 'USE {}_db'.format(self.co_name)
        cursor.execute(query)

        try:
           
            query = """CREATE TABLE stock_info_1(企業名 VARCHAR(32),証券コード VARCHAR(10),株価 VARCHAR(10),変化 VARCHAR(10),
            変化率 VARCHAR(10),業種 VARCHAR(10),per VARCHAR(10),pbr VARCHAR(10),利回り VARCHAR(10),信用倍率 VARCHAR(10),
            時価総額 VARCHAR(10),日時 VARCHAR(30),始まり値 VARCHAR(10),高値 VARCHAR(10),安値 VARCHAR(10),終値 VARCHAR(10),
            出来高（株） VARCHAR(30),売買代金（百万） VARCHAR(30), VWAP VARCHAR(30),約定回数 VARCHAR(10),売買最低代金 VARCHAR(10),
            発行済み株式数（株株株株株株） VARCHAR(32),五日 VARCHAR(10),二十五日 VARCHAR(10),七十五日 VARCHAR(10),二百日 VARCHAR(10), 
            予備 VARCHAR(10));"""
            
            cursor.execute(query)
            self.conn.commit()
        except OperationalError:
            query = "INSERT IGNORE INTO stock_info_1 ({}予備) VALUES ({}NULL)".format('{},'*26, '\'{}\','*26)
            query = query.format(*list(info_dict.keys())[:-1], *list(info_dict.values())[:-1])
            cursor.execute(query)
            self.conn.commit()

            return True

        query = "INSERT IGNORE INTO stock_info_1 ({}予備) VALUES ({}NULL)".format('{},'*26, '\'{}\','*26)
        query = query.format(*list(info_dict.keys())[:-1], *list(info_dict.values())[:-1])
        cursor.execute(query)
        self.conn.commit()

        return False
    
    def change_df_table(self, df, caption):
        order = 'mysql://root:mukatitech2019@localhost/{}?charset=utf8'.format(self.db_name)
        engine = create_engine(order)
        table_name = '{}'.format(caption)
        df.to_sql(table_name ,con=engine, if_exists='append', index=False)
        
        
class Save_Data(Read_HTML, Read_HTML_FINANCE):

    def __init__(self, url_1, url_2, code):
        Read_HTML.__init__(
            self,
            url_1,
            code
        )
        Read_HTML_FINANCE.__init__(
            self,
            url_2,
            code
        )
        self.code = code
        
        self.creater = Create_DB(self.code, self.co_name)

        create_db = self.creater.create_db()
        
    def save_to_table(self):

        info_dict = self.get_stock_info()
        if info_dict != None:
            if self.creater.create_table(info_dict) == False:
                pass
            else:
                df = self.arrange_stock_info()
                self.back_to_db(df, 'stock_info_1')
        else:
            print('上場廃止になっています。')
            return 


 
        df_dict = self.get_finance_info()
        if df_dict == None:
            return 

        for caption, df in df_dict.items():
            try:
                df = self.arrange_df(df, caption)
                self.creater.change_df_table(df, caption)
                df_arranged = self.arrange_db(caption)
                sql = 'DROP TABLE IF EXISTS {}'.format(caption)
                connection = self.creater.conn
                cursor = connection.cursor()
                cursor.execute(sql)
                connection.commit()
                try:
                    self.creater.change_df_table(df_arranged, caption)
                except OperationalError:
                    continue
            
            except AttributeError:
                continue
    
    def arrange_db(self, caption):
        connection = self.creater.conn
        cursor = connection.cursor()

        query = "USE {}_db".format(self.co_name)
        cursor.execute(query)
        query = 'SELECT * FROM {}'.format(caption)
        cursor.execute(query)
        df = pd.read_sql(sql = query, con=self.creater.conn).drop_duplicates()

        return df
    
    def arrange_stock_info(self):
        connection = self.creater.conn
        cursor = connection.cursor()
        query = "USE {}_db".format(self.co_name)
        cursor.execute(query)
        query = 'SELECT * FROM stock_info_1'
        cursor.execute(query)

        df = pd.read_sql(sql = query, con=self.creater.conn).drop_duplicates()

        return df

    def back_to_db(self, df, caption):

        query = "DROP TABLE IF EXISTS {}".format(caption)
        connection = self.creater.conn
        cursor = connection.cursor()
        cursor.execute(query)
        self.creater.change_df_table(df, caption)

    def arrange_df(self, df, caption):
        caption_columns_dict = {
            '業績推移':['決算期', '売上高', '営業益', '経常益', '最終益', '修正1株益', '1株配', '発表日'],
            '過去最高業績':['特になし', '売上高', '営業益', '経常益', '最終益', '修正1株益'],
            '上期or下期業績':['決算期', '売上高', '営業益', '経常益', '最終益', '修正1株益', '1株配', '発表日'],
            '今期業績（累計）':['決算期', '売上高', '営業益', '経常益', '最終益', '修正1株益', '上期or通期進捗率', '発表日'],
            '３ヶ月決算':['決算期', '売上高', '営業益', '経常益', '最終益', '修正1株益', '売上営業損益率', '発表日'],
            '財務':['決算期', '1株純資産', '自己資本比率', '純資産', '自己資本', '剰余金', '有利子負債倍率', '発表日'],
            'CF推移':['決算期', '営業益', 'フリーCF', '営業CF', '投資CF', '現金等残高', '現金比率'],
            '収益性':['決算期', '売上高', '営業益', '売上営業利益率', 'ROE', 'ROA', '純資産回転率', '修正1株益']
        }
        new_columns_list = caption_columns_dict[caption]
        old_columns_list = list(df.columns)

        corr_dict = dict(zip(old_columns_list, new_columns_list))
        df = df.rename(columns=corr_dict)

        return df

start_time = datetime.now()


file_name = '/Users/mukaiyama/株価/companylist.csv'
df = pd.read_csv(file_name)
df_co = pd.DataFrame()
for i in range(len(df)):
    df_i = pd.DataFrame(df.iloc[i, :]).T
    

    if '（内国株）'in str(df_i['市場・商品区分']):
        df_co = pd.concat([df_co, df_i], axis=0)

id_list = ['0000'] + list(df_co['コード'])  
      

url_1 = 'https://kabutan.jp/stock/?code='
url_2 = 'https://kabutan.jp/stock/finance?code='

for id in tqdm(id_list):
    save = Save_Data(url_1, url_2, str(id))
    save.save_to_table()

end_time = datetime.now()

df = pd.read_csv('/Users/mukaiyama/株価/time_table.csv')
df = df.append({'開始時刻': start_time, '終了時刻': end_time}, ignore_index=True)
df.to_csv('/Users/mukaiyama/株価/time_table.csv', index=False)



