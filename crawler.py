from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import time, re, os
import concurrent.futures
import pandas as pd
from datetime import datetime, timedelta

class BaseScraper:
    def __init__(self):
        self.chrome_options = Options()
        self._setup_options()
        self.driver = None

    def _setup_options(self):
        self.chrome_options.add_argument("--no-sandbox")
        self.chrome_options.add_argument("--disable-dev-shm-usage")
        self.chrome_options.add_experimental_option("prefs", {
            "profile.default_content_settings.popups": 0,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True
        })

    def create_driver(self):
        self.driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()),
            options=self.chrome_options
        )

    def close_driver(self):
        if self.driver:
            self.driver.quit()

class VesselDataProcessor:
    @staticmethod
    def extract_dates(text):
        patterns = {
            'days': r'(\d+)\s*days? ago',
            'weeks': r'(\d+)\s*weeks? ago',
            'months': r'(\d+)\s*months? ago'
        }
        
        today = datetime.now()
        for key, pattern in patterns.items():
            match = re.search(pattern, text)
            if match:
                number = int(match.group(1))
                if key == 'days':
                    return (today - timedelta(days=number)).strftime('%Y-%m-%d')
                elif key == 'weeks':
                    return (today - timedelta(weeks=number)).strftime('%Y-%m-%d')
                elif key == 'months':
                    return (today.replace(day=1) - timedelta(days=30 * number)).strftime('%Y-%m-%d')
        return today.strftime('%Y-%m-%d')

    @staticmethod
    def parse_shipping_data(table):
        row = table.text.strip()
        res = re.split(
            r'Destination|ETA: |ATA: |Predicted ETA|Distance / Time|Course / Speed|Current draught|Navigation Status|Position received|Length / Beam|IMO / MMSI|MMSI|Callsign|Last Port|ATD: ',
            row
        )
        res = [item.strip() for item in res]
        
        return {
            'Destination': res[1],
            'ARRIVAL': res[2].replace("\n", " ").split(" (")[0],
            'Status': "ARRIVED" if "ATA" in res[2] else "SAILING",
            'UpdateTime': VesselDataProcessor.extract_dates(res[8]),
            'NavStatus': res[7],
            'Last Port': res[-2],
            'ATD': res[-1].split(" (")[0]
        }

class TPSScraper(BaseScraper):
    def scrape(self):
        self.create_driver()
        self.driver.get("https://tps.co.id:8081/webaccess/")
        time.sleep(2)
        soup = BeautifulSoup(self.driver.page_source, 'html.parser')
        tables = soup.find_all(class_='card-body p-5')
        self.close_driver()
        return self._process_tables(tables)

    def _process_tables(self, tables):
        data = []
        exp_text=tables[0].text
        entries = re.split(r'\n{2,}', exp_text.strip())
        for entri in entries:
            list_entri=entri.split("\n")
            vessel=list_entri[0].strip()
            atb=":".join(list_entri[2].split(":")[1:]).strip()
            etd=":".join(list_entri[3].split(":")[1:]).strip()
            sub_data={}
            sub_data["Vessel Name"]=vessel
            sub_data["Arrival"]=atb
            sub_data["Berthing"]=atb
            sub_data["Departure"]=etd
            sub_data["Open Stack"]="-"
            sub_data["Closing"]="-"
            sub_data["Status"]="Alongside"
            sub_data["UpdateTime"]=datetime.now().strftime("%d/%m/%Y %H:%M")
            sub_data["Destination"]="-"
            sub_data["Origin"]="Surabaya, Indonesia"
            data.append(sub_data)
        exp_text=tables[1].text
        entries = re.split(r'\n{2,}', exp_text.strip())
        for entri in entries:
            list_entri=entri.split("\n")
            vessel=list_entri[0].strip()
            eta=":".join(list_entri[2].split(":")[1:]).strip()
            etd=":".join(list_entri[3].split(":")[1:]).strip()
            OpenStack=":".join(list_entri[4].split(":")[1:]).strip()
            CTC=":".join(list_entri[5].split(":")[1:]).strip()
            sub_data={}
            sub_data["Vessel Name"]=vessel
            sub_data["Arrival"]=eta
            sub_data["Berthing"]=eta
            sub_data["Departure"]=etd
            sub_data["Open Stack"]=OpenStack
            sub_data["Closing"]=CTC
            sub_data["Status"]="Schedule"
            sub_data["UpdateTime"]=datetime.now().strftime("%d/%m/%Y %H:%M")
            sub_data["Destination"]="Surabaya, Indonesia"
            sub_data["Origin"]="-"
            data.append(sub_data)
        return data

class IBS_TPKSScraper(BaseScraper):
    def scrape(self):
        self.create_driver()
        self.driver.get("https://ibstpks.pelindo.co.id/webaccess/")
        time.sleep(2)
        soup = BeautifulSoup(self.driver.page_source, 'html.parser')
        tables = soup.find_all(class_='vessel')
        self.close_driver()
        return self._process_tables(tables)

    def _process_tables(self, tables):
        data = []
        exp_text=tables[0].text
        entries = re.split(r'\n{2,}', exp_text.strip())[1:]
        if 'No Vessel Berthing' not in entries:
            entries_final = [entries[i]+entries[i+1] for i in range(0,len(entries),2)]
        else:
            entries_final=[]

        for entri in entries_final:
            if "Detail" in entri:
                continue
            list_entri=entri.split("\n")
            vessel=list_entri[0].split("(")[0].strip()
            vessel_type=list_entri[2].strip() if list_entri[2].strip()!="" else "DOMESTIC"
            atb=":".join(list_entri[3].split(":")[1:]).strip().replace("\n"," ")
            etd=":".join(list_entri[4].split(":")[1:]).strip().replace("\n"," ")
            sub_data={}
            sub_data["Vessel Name"]=vessel
            sub_data["CompanyName"]="-"
            sub_data["VesselType"]=vessel_type
            sub_data["Arrival"]=atb
            sub_data["Berthing"]=atb
            sub_data["Departure"]=etd
            sub_data["Open Stack"]="-"
            sub_data["Closing"]="-"
            sub_data["Status"]="Alongside"
            sub_data["UpdateTime"]=datetime.now().strftime("%d/%m/%Y %H:%M")
            sub_data["Origin"]="Semarang, Indonesia"
            sub_data["Destination"]="-"
            data.append(sub_data)

        exp_text=tables[1].text
        entries = re.split(r'\n{2,}', exp_text.strip())[1:]
        
        if 'No Confirmed Vessel' not in entries:
            entries_final = [entries[i]+entries[i+1] for i in range(0,len(entries),2)]
        else:
            entries_final=[]

        for entri in entries_final:
            if "Detail" in entri:
                continue
            list_entri=entri.split("\n")
            vessel=list_entri[0].split("(")[0].strip()
            company=list_entri[3].strip()
            vessel_type=list_entri[4].strip() if list_entri[4].strip()!="" else "DOMESTIC"
            etb=":".join(list_entri[5].split(":")[1:]).strip().replace("\n"," ")
            etd=":".join(list_entri[6].split(":")[1:]).strip().replace("\n"," ")
            OpenStack=":".join(list_entri[7].split(":")[1:]).strip().replace("\n"," ")
            ct=":".join(list_entri[8].split(":")[1:]).strip().replace("\n"," ")
            sub_data={}
            sub_data["Vessel Name"]=vessel
            sub_data["CompanyName"]=company
            sub_data["VesselType"]=vessel_type
            sub_data["Arrival"]=etb
            sub_data["Berthing"]=etb
            sub_data["Departure"]=etd
            sub_data["Open Stack"]=OpenStack
            sub_data["Closing"]=ct
            sub_data["Status"]="Confirmed"
            sub_data["UpdateTime"]=datetime.now().strftime("%d/%m/%Y %H:%M")
            sub_data["Destination"]="Semarang, Indonesia"
            sub_data["Origin"]="-"
            data.append(sub_data)

        exp_text=tables[2].text
        entries = re.split(r'\n{2,}', exp_text.strip())[1:]
        entries_final = [entries[i]+entries[i+1] for i in range(0,len(entries),2)]

        for entri in entries_final:
            if "Detail" in entri:
                continue
            list_entri=entri.split("\n")
            vessel=list_entri[0].split("(")[0].strip()
            company=list_entri[3].strip()
            vessel_type=list_entri[4].strip() if list_entri[4].strip()!="" else "DOMESTIC"
            etb=":".join(list_entri[6].split(":")[1:]).strip().replace("\n"," ")
            etd=":".join(list_entri[7].split(":")[1:]).strip().replace("\n"," ")
            OpenStack=":".join(list_entri[8].split(":")[1:]).strip().replace("\n"," ")
            ct=":".join(list_entri[9].split(":")[1:]).strip().replace("\n"," ")
            sub_data={}
            sub_data["Vessel Name"]=vessel
            sub_data["CompanyName"]=company
            sub_data["VesselType"]=vessel_type
            sub_data["Arrival"]=etb
            sub_data["Berthing"]=etb
            sub_data["Departure"]=etd
            sub_data["Open Stack"]=OpenStack
            sub_data["Closing"]=ct
            sub_data["Status"]="Open Stack"
            sub_data["UpdateTime"]=datetime.now().strftime("%d/%m/%Y %H:%M")
            sub_data["Destination"]="Semarang, Indonesia"
            sub_data["Origin"]="-"
            data.append(sub_data)
        return data

class JICTScraper(BaseScraper):
    def scrape(self):
        self.create_driver()
        self.driver.get("https://www.jict.co.id/vessel-schedule")
        time.sleep(7)
        soup = BeautifulSoup(self.driver.page_source, 'html.parser')
        table = soup.find('table')
        self.close_driver()
        return self._process_table(table)

    def _process_table(self, table):
        data = []
        if table:
            rows = table.find_all('tr')
            column_name=rows[0].text.split("\n")[1:-1]
            for row in rows[1:]:
                data.append(row.text.split("\n")[1:-1])
        df_jict = pd.DataFrame(data, columns=column_name)
        df_jict=df_jict[df_jict["Status"]!="SAILING"]
        for i, sub_data in df_jict.iterrows():
            df_jict.loc[i, "UpdateTime"]=datetime.now().strftime("%d/%m/%Y %H:%M")
            if sub_data["Status"]=="-":
                df_jict.loc[i, "Destination"] = "Jakarta, Indonesia"
                df_jict.loc[i, "Origin"] = "-"
            else:
                df_jict.loc[i, "Origin"] = "Jakarta, Indonesia"
                df_jict.loc[i, "Destination"] = "-"
        return df_jict.to_dict('records')

class VesselStatusChecker:
    def __init__(self):
        self.chrome_options = Options()
        self._setup_options()
        self.data = []

    def _setup_options(self):
        self.chrome_options.add_argument("--no-sandbox")
        self.chrome_options.add_argument("--disable-dev-shm-usage")
        self.chrome_options.add_experimental_option("prefs", {
            "profile.managed_default_content_settings.images": 2
        })
        self.chrome_options.page_load_strategy = 'eager'

    def check_status(self, combined_df):
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(self._check_single_status, row) for _, row in combined_df.iterrows()]
            concurrent.futures.wait(futures)
        return pd.DataFrame(self.data)

    def _check_single_status(self, row):
        driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()),
            options=self.chrome_options
        )
        try:
            driver.get(f"https://www.vesselfinder.com/vessels/details/{row['IMO']}")
            time.sleep(2)
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            table = soup.find(class_='col vfix-top lpr')
            if table:
                res = VesselDataProcessor.parse_shipping_data(table)
                if row["Origin"]=="-":
                    row["Origin"]=res["Last Port"]
                else:
                    row["Destination"]=res["Destination"]
            self.data.append(row)
        except Exception as e:
            print(f"Error processing {row['IMO']}: {e}")
            self.data.append(row)
        finally:
            driver.quit()

class ScheduleManager:
    def __init__(self):
        self.imo_file = "data/master_IMO_final.xlsx"
        self.schedule_file = "data/vessel_schedule.xlsx"
        self.history_file = "data/history_vessel.xlsx"

    def update_imo_mapping(self, combined_df):
        df_imo = pd.read_excel(self.imo_file)
        dict_imo={k:str(v) for k,v in zip(df_imo["Vessel"],df_imo["IMO"])}
        combined_df["Vessel Name"]=combined_df["Vessel Name"].str.strip()
        combined_df["IMO"]=combined_df["Vessel Name"].map(dict_imo)
        null_imo=combined_df[combined_df["IMO"].isnull()]["Vessel Name"].tolist()
        print("\n".join(null_imo))
        last_id=df_imo["Id"].max()
        list_data=[]
        for i,imo in enumerate(null_imo):
            data={}
            data["RowStatus"]="ACT"
            data["Id"]=last_id+i+1
            data["Vessel"]=imo
            data["IMO"]=0
            data["BuiltYear"]="0"
            data["IdLama"]="0"
            data["ModifiedBy"]=""
            data["ModifiedOn"]=""
            data["CreatedBy"]="1"
            data["CreatedOn"]=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            list_data.append(data)

        df_imo=pd.concat([df_imo,pd.DataFrame(list_data)])
        df_imo.to_excel(self.imo_file, index=False)

    def manage_schedules(self, df_final):
        df_final=df_final.reindex(columns=['Vessel Name',	'IMO',	'Origin',	'Destination',	'Arrival',	'Berthing',	'Departure',	'Closing',	'Open Stack',	'Status',	'Update Time'])
        status_map={'-':'Schedule','Confirmed':'Schedule','Schedule':'Schedule','Open Stack':'Schedule','Alongside':'Alongside','WORKING':'Alongside'}
        df_final["Status"]=df_final["Status"].map(status_map)
        df_final['Arrival'] = pd.to_datetime(df_final['Arrival'], format='%d/%m/%Y %H:%M')
        df_final['Update Time'] = pd.to_datetime(datetime.now().strftime("%d/%m/%Y %H:%M"), format='%d/%m/%Y %H:%M')
        df_final.loc[(df_final['Status'] == 'Schedule') & (df_final['Arrival'] < df_final['Update Time']), 'Status'] = 'Pending'
        df_final=df_final.sort_values(by=['Arrival']).reset_index(drop=True)
        now_vessel=df_final["Vessel Name"].tolist()
        if os.path.isfile("data/vessel_schedule.xlsx"):
            schedule_df=pd.read_excel("data/vessel_schedule.xlsx")
            schedule_df=schedule_df.sort_values(by=['Arrival']).reset_index(drop=True)
        else:
            schedule_df=df_final
        history_vessel = schedule_df[~schedule_df["Vessel Name"].isin(now_vessel)]

        schedule_vessel = schedule_df[schedule_df["Vessel Name"].isin(now_vessel)]
        vessel_dict = {}
        for i in range(len(schedule_vessel) - 1, -1, -1):  # Iterasi dari belakang
            # Mengambil baris dari df_final yang sesuai dengan Vessel Name
            vessel_row = df_final[df_final["Vessel Name"] == schedule_vessel.iloc[i]["Vessel Name"]]
            
            if schedule_vessel.iloc[i]["Vessel Name"] not in vessel_dict.keys():
                vessel_dict[schedule_vessel.iloc[i]["Vessel Name"]] = [i]
            else:
                vessel_dict[schedule_vessel.iloc[i]["Vessel Name"]].append(i)
            
            # Memastikan bahwa vessel_row tidak kosong
            try:
                schedule_vessel.iloc[i] = vessel_row.iloc[-len(vessel_dict[schedule_vessel.iloc[i]["Vessel Name"]])]
            except:
                history_vessel = history_vessel.append(schedule_vessel.iloc[i], ignore_index=True)
                schedule_vessel = schedule_vessel.drop(schedule_vessel.index[i]) 
                
        history_status={'Schedule':'Canceled','Alongside':'History','Pending':'Canceled'}
        history_vessel["Status"]=history_vessel["Status"].map(history_status)

        fname="data/history_vessel.xlsx"
        if os.path.isfile(fname):
            old_history=pd.read_excel(fname)
            new_history=pd.concat([history_vessel,old_history])
            new_history.to_excel(fname,index=False)
        else:
            history_vessel.to_excel(fname,index=False)

        now_df = df_final[~df_final["Vessel Name"].isin(schedule_df["Vessel Name"].tolist())]
        result=pd.concat([schedule_vessel,now_df])
        result=result.sort_values(by=['Arrival']).reset_index(drop=True)
        result.to_excel("data/vessel_schedule.xlsx",index=False)
        return result

class VesselScheduler:
    def __init__(self):
        self.scrapers = {
            'tps': TPSScraper(),
            'ibs_tpks': IBS_TPKSScraper(),
            'jict': JICTScraper()
        }
        self.processor = VesselDataProcessor()
        self.status_checker = VesselStatusChecker()
        self.schedule_manager = ScheduleManager()


    def run(self):
        while True:
            try:
                scraped_data = []
                for source in self.scrapers.values():
                    scraped_data.extend(source.scrape())
                
                combined_df = pd.DataFrame(scraped_data)
                combined_df = combined_df.reindex(columns=["Vessel Name","Origin","Destination","Arrival",	"Berthing",	"Departure",	"Closing",	"Open Stack","Status",	"UpdateTime"])
                combined_df.columns = ["Vessel Name","Origin","Destination","Arrival",	"Berthing",	"Departure",	"Closing",	"Open Stack","Status",	"Update Time"]
                self.schedule_manager.update_imo_mapping(combined_df)
                
                df_final = self.status_checker.check_status(combined_df)
                result=self.schedule_manager.manage_schedules(df_final)
                
                next_run = self._calculate_next_run(result)
                print(f"Next run at {datetime.now() + timedelta(seconds=next_run)}")
                time.sleep(next_run)
            except Exception as e:
                print(e)
                time.sleep(3600)
                continue

    def _calculate_next_run(self, df):
        time_arrival = df['Arrival'][df["Status"] == "Schedule"].tolist()
        time_arrival_dt = [pd.to_datetime(arrival) for arrival in time_arrival]
        min_arrival = min(time_arrival_dt)
        time_now = datetime.now()
        next_run = (min_arrival - time_now).total_seconds() + 3700
        return next_run

if __name__ == "__main__":
    scheduler = VesselScheduler()
    scheduler.run()