# !/usr/bin/env python
# -*- coding: utf-8 -*-

import io
import logging
import os
import sys
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../')))
sys.stdout = io.TextIOWrapper(sys.stdout.detach(), encoding = 'utf-8')
sys.stderr = io.TextIOWrapper(sys.stderr.detach(), encoding = 'utf-8')
from google.cloud import bigquery
from google.cloud import storage

import time
import pandas as pd
import numpy as np
import matplotlib
from matplotlib import pyplot as plt

from PIL import Image, ImageDraw, ImageFont,ImageEnhance, ImageFilter

class Bq(object):
    def __init__(self, prj_name, dataset_name, table_name, max_records=None):
        self.prj = prj_name
        self.dataset = dataset_name
        self.table = table_name
        self.max_record = max_records
        self.start()

    def start(self):
        self.bqc = bigquery.Client(self.prj)
        self.ds = self.bqc.dataset(self.dataset)
        self.tb = self.ds.table(self.table)
        self.tb.reload()

    def getdata(self):
        if self.tb.table_type == "VIEW":
            tmp_text = ("LIMIT {}".format(self.max_record) if self.max_record else "")
            query_results = self.bqc.run_sync_query("SELECT * FROM {}.{} ".format(self.dataset, self.table))
            query_results.run()
            # print([ (i.field_type, i.mode, i.name) for i in query_results.schema])
            columns = [(i.name) for i in query_results.schema]
            rows = query_results.rows
        else:
            schema = [(i.name) for i in self.tb.schema]
            # print([(i.name, i.field_type) for i in tb.schema])
            columns = [(i.name) for i in self.tb.schema]
            rows = list(self.tb.fetch_data(max_results=self.max_record))
        return (columns, rows)



class Pir(object):
    save_path = "/Users/data/Desktop/cindy/fun/pli/pir"
    layout_path = "/Users/data/Desktop/cindy/fun/pli/pir/layout"
    font_path = '/Library/Fonts/AppleGothic.ttf'
    alpha = 75
    colorspool = {
    "1":(255, 0, 0, alpha), "2":(255, 64, 0, alpha), "3":(255, 128, 0, alpha),"4":(255, 191, 0, alpha),
    "5":(255, 255, 0, alpha), "6":(191, 255, 0, alpha),"7":(0, 255, 0, alpha), "8":(0, 255, 191, alpha),
    "9":(0, 255, 255, alpha),"10":(0, 191, 255, alpha), "11":(0, 128, 255, alpha), "12":(0, 0, 255, alpha),
    "13":(128, 0, 255, alpha), "14":(191, 0, 255, alpha), "15":(255, 0, 255, alpha)
    }



    def __init__(self, pir_col, pir, zone_col, zone, mode="overall", handmade=True):
        self.mode = mode # (overall, day, hour)
        self.handmade = handmade
        self.pic_path = ""
        self.data = []
        self.listcnt = 7
        self.font_size = 24
        self.df_pir_raw = pd.DataFrame(pir, columns=pir_col)
        self.df_zone = pd.DataFrame(zone, columns=zone_col)
        self.df_zone.columns = ["clientid","storeid","floor","apid","device","zoneid","zonename","x","y","gda_regtime","gda_lastupdate"]

        # df_zone = df_zone[["apid","sensorid","storeid","zoneid","zonename","floor","x","y"]]
        self.df_pir_cal = []
        self.tmp = []
        self.start()

    def start(self):
        self.save_path = "{}/{}".format(self.save_path, self.mode)
        if self.handmade :
            self.df_zone = self.extraZoneData()
        # apid, logdate, device, hour, cnt, clientid, storeid, floor, zoneid, zonename, x, y, gda_regtime, gda_lastupdate
        # WSTW0001, 2017-03-28, 7F002101, 23, 2, wscnair, 469-1, 1, 1, 0, zone, 1, 100.0, 100.0, 2017-04-05 06:14:48.850000+00:00
        self.df_pir_cal = pd.merge(self.df_pir_raw, self.df_zone, how='left', on=["apid", "device"])

        if self.mode == "overall":
            self.tmp = self.df_pir_cal.groupby(['apid','zoneid','x','y','jpg'], as_index=False)[["cnt"]].sum()
        elif self.mode == "day":
            self.tmp = self.df_pir_cal.groupby(['apid','logdate','zoneid','x','y','jpg'], as_index=False).agg({'cnt': np.sum})
            # self.data = self.tmp.query("logdate == '{}'".format("2017-04-03")).sort_values(by=["cnt"], ascending=[0])
        else:
            self.tmp = self.df_pir_cal.groupby(['apid','logdate','hour','zoneid','x','y','jpg'], as_index=False).agg({'cnt': np.sum})

    def extraZoneData(self):
        # if need to update x,y by handmade
        url = "/Users/data/Desktop/cindy/fun/pli/pir/layout/results-20170410-183208.csv"
        layout = pd.read_csv(url)
        layoutdata = layout[["apid", "device", "x", "y", "jpg"]]
        new_layout = pd.merge(self.df_zone, layoutdata, how='left', on=["apid","device"])
        # new_layout = pd.merge(self.df_zone, layoutdata, how='left', left_on=["apid","device"], right_on=["apid","device"])
        data = new_layout[new_layout['jpg'].notnull() & (new_layout['x_x'] != 0) & (new_layout['x_x'].notnull())][["apid","device","zoneid","zonename","x_y","y_y","jpg"]]
        data.columns = ["apid","device","zoneid","zonename","x","y","jpg"]
        return data

    def getZonedata(self, i):
        if self.mode == "overall":
            x = i[2]
            y = i[3]
            cnt = i[5]
            zoneid = i[1]
        elif self.mode == "day":
            x = i[3]
            y = i[4]
            cnt = i[6]
            zoneid = i[2]
        else:
            x = i[4]
            y = i[5]
            cnt = i[7]
            zoneid = i[3]
        return (x, y, zoneid, cnt)

    def draw_pir_layout(self):
        tmp_path = "{}/{}".format(self.layout_path, self.data.jpg.unique()[0])
        logdate = (self.data.logdate.unique()[0] if 'logdate' in self.data.columns else time.strftime("%Y-%m-%d"))
        hour = ("_{}".format(self.data.hour.unique()[0]) if 'hour' in self.data.columns else "")
        r = 80
        layout = Image.open(tmp_path)
        font = ImageFont.truetype(self.font_path, self.font_size)
        draw = ImageDraw.Draw(layout, 'RGBA')
        draw.ink = 0*255*256 + 0*255*256 + 0*256*256
        draw.text((layout.size[0]-4*r, 50), "{}".format(self.mode), font=font, fill='green')

        for num, i in enumerate(self.data.values):
            if num < len(self.colorspool):
                num = num + 1
            else:
                num = self.colorspool[len(self.colorspool)-1]
            (x, y, zoneid, cnt) = self.getZonedata(i)

            draw.text((x-r/2, y-r/5), str(cnt), font=font, fill='red')
            # draw.ellipse((x-r, y-r, x+r, y+r), fill=self.colorspool[str(num)], outline = "white")
            draw.ellipse((x-r, y-r, x+r-num*8, y+r-num*8), fill=self.colorspool[str(num)], outline = "white")

        layout.save("{}/{}_{}{}.jpg".format(self.save_path, self.mode, logdate, hour))
        del draw
        layout.close()



class WiFi(object):
    save_path = "/Users/data/Desktop/cindy/fun/pli/wifi"
    plt.rcParams["figure.figsize"] = [20.0, 3.0]

    def __init__(self, wifi_col, wifi):
        self.wifi = wifi
        self.col = wifi_col
        self.mode = "day"
        self.start()
        self.dfindex = ""
        self.getcol = []
        self.alldata = []

    def start(self):
        if self.mode == "day":
            self.getcol = ['apid', 'Status_Group', 'logdate', 'adj_mac_z_bq']
            self.dfindex = "logdate"
        df_wifi = pd.DataFrame(self.wifi, columns=self.col)[self.getcol]
        self.tmp = df_wifi.query("Status_Group in ['1','3','5']").pivot_table(index=[self.dfindex], values=['adj_mac_z_bq'], columns=['Status_Group'], aggfunc=np.sum,fill_value=0)
        self.tmp.columns = ["env","instore","long"]
        self.all_data = {'1':(self.tmp[['env']],'green',"env"),
                        '2':(self.tmp[['env','instore']],['g', 'b'],"env_instore"),
                        '3':(self.tmp[['instore','long']],['b', 'orange'],"instore_long"),
                        '4':(self.tmp[['env','instore','long']],['g', 'b', 'orange'],"all")}

    def draw_line(self):
        # ['apid', 'Status_Group', 'mac_days', 'mac_mean', 'mac_std', 'mac_cnt', 'logdate', 'mac_z', 'min_mac_z', 'adj_mac_z_bq']
        for num, i in enumerate(self.all_data):
            data = self.all_data[i][0]
            t = self.all_data[i][0].plot(grid=True, rot=10, legend=True, color=self.all_data[i][1])
            # ttp.set_xlim([0, 5])
            t.set_ylim([1, 6])
            lgd = t.legend(loc='upper right', title="Status_Group", fontsize=8, borderpad=1,
                 ncol=7, fancybox=True, shadow=True, bbox_to_anchor=(1, 1.4))

            if self.all_data[i][2] == "env_instore" :
                self.mark_ei_peak()
            if self.all_data[i][2] == "instore_long" :
                self.mark_il_peak()
            plt.savefig('{}/{}.png'.format(self.save_path, self.all_data[i][2]), dpi=100, format='png',
                         bbox_inches='tight')
            plt.close()

    def mark_ei_peak(self):
        self.tmp['env_in_dist'] = self.tmp['env'] - self.tmp['instore']
        max_row = self.tmp.ix[self.tmp['env_in_dist'].idxmax()]
        min_row = self.tmp.ix[self.tmp['env_in_dist'].idxmin()]
        plt.scatter(self.tmp.index.get_loc(max_row.name), max_row.env, 1000, color ='green', alpha=0.2)
        plt.scatter(self.tmp.index.get_loc(max_row.name), max_row.instore, 1000, color ='blue', alpha=0.2)
        tmpcolor = 'red'
        if abs(min_row.env_in_dist) > 1:
            plt.scatter(self.tmp.index.get_loc(min_row.name), min_row.env, 1000, color ='green', alpha=0.2)
            plt.scatter(self.tmp.index.get_loc(min_row.name), min_row.instore, 1000, color ='blue', alpha=0.2)
        else :
            plt.scatter(self.tmp.index.get_loc(min_row.name), min_row.instore, 1000, color =tmpcolor, alpha=0.2)

    def mark_il_peak(self):
        self.tmp['in_long_dist'] = self.tmp['instore'] - self.tmp['long']
        max_row = self.tmp.ix[self.tmp['in_long_dist'].idxmax()]
        min_row = self.tmp.ix[self.tmp['in_long_dist'].idxmin()]
        plt.scatter(self.tmp.index.get_loc(max_row.name), max_row.instore, 1000, color ='blue', alpha=0.2)
        plt.scatter(self.tmp.index.get_loc(max_row.name), max_row.long, 1000, color ='orange', alpha=0.2)
        tmpcolor = 'red'
        if abs(min_row.in_long_dist) > 0.8:
            plt.scatter(self.tmp.index.get_loc(min_row.name), min_row.instore, 1000, color ='blue', alpha=0.2)
            plt.scatter(self.tmp.index.get_loc(min_row.name), min_row.long, 1000, color ='orange', alpha=0.2)
        else :
            plt.scatter(self.tmp.index.get_loc(min_row.name), min_row.long, 1000, color =tmpcolor, alpha=0.2)

if __name__ == "__main__":
    mode = "pir"
    prj_name = 'gomi-dev'
    dataset_name = ('cindy_iot' if mode == "wifi" else "iot")
    table_name = ('wifi_day' if mode == "wifi" else 'pir_new')
    tb_zone = 'tw_IoT_StoreZone_Mapping'

    # source_file_name = "/Users/data/Desktop/cindy/weather/up/history/history_20170215.csv"
    if mode == "wifi":
        wifidata = Bq(prj_name, dataset_name, table_name)
        (wifi_col, wifi) = wifidata.getdata()
        wifi = WiFi(wifi_col, wifi)
        wifi.draw_line()
    else:
        zonedata = Bq(prj_name, dataset_name, tb_zone)
        pirdata = Bq(prj_name, dataset_name, table_name, max_records=None)

        (zone_col, zone) = zonedata.getdata()
        (pir_col, pir) = pirdata.getdata()

        # ['apid', 'logdate', 'device', 'hour', 'cnt']
        # [('WSTW0001', '2017-03-28', '7F002101', 23, 2)]
        # ['clientid', 'storeid', 'floor', 'apid', 'sensorid', 'zoneid', 'zonename', 'x', 'y', 'gda_regtime', 'gda_lastupdate']
        # 'wscnair', '469-1', '1', 'WSTW0001', '7F002001', '0', 'zone 0', 0, 0,

        # pir1 = Pir(pir_col, pir, zone_col, zone, mode="overall")
        # pir1.data = pir1.tmp.sort_values(by=["cnt"], ascending=[0])
        # pir1.draw_pir_layout()

        # pir_day = Pir(pir_col, pir, zone_col, zone, mode="day")
        # for i in pir_day.tmp.logdate.unique():
        #     pir_day.data = pir_day.tmp.query("logdate == '{}'".format(i)).sort_values(by=["cnt"], ascending=[0])
        #     pir_day.draw_pir_layout()

        pir_hour = Pir(pir_col, pir, zone_col, zone, mode="hour")
        # for i in pir_hour.tmp.logdate.unique():
        tmpdata = pir_hour.tmp.query("logdate == '{}' ".format("2017-04-26"))
        for j in tmpdata.hour.unique():
            pir_hour.data = tmpdata.query("hour == {} ".format(j)).sort_values(by=["cnt"], ascending=[0])
            pir_hour.draw_pir_layout()
