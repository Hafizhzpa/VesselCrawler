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

def extract_dates(text):
    # Pola untuk mendeteksi hari, minggu, dan bulan
    patterns = {
        'days': r'(\d+)\s*days? ago',
        'weeks': r'(\d+)\s*weeks? ago',
        'months': r'(\d+)\s*months? ago'
    }
    
    today = datetime.now()
    results = {}

    for key, pattern in patterns.items():
        match = re.search(pattern, text)
        if match:
            number = int(match.group(1))
            if key == 'days':
                date=today - timedelta(days=number)
                return date.strftime('%Y-%m-%d')
            elif key == 'weeks':
                date= today - timedelta(weeks=number)
                return date.strftime('%Y-%m-%d')
            elif key == 'months':
                date= (today.replace(day=1) - timedelta(days=1)).replace(day=1) - timedelta(days=30 * (number - 1))
                return date.strftime('%Y-%m-%d')
        else:
            return today.strftime('%Y-%m-%d')

def parse_shipping_data(table):
    # Split the text by the delimiter
    row = table.text.strip()
    res = re.split(r'Destination|ETA: |ATA: |Predicted ETA|Distance / Time|Course / Speed|Current draught|Navigation Status|Position received|Length / Beam|IMO / MMSI|MMSI|Callsign|Last Port|ATD: ', row)
    res=[re.strip() for re in res]
    dest=res[1]
    eta=res[2].replace("\n"," ").split(" (")[0]
    status="ARRIVED" if "ATA" in eta else "SAILING"
    nav_status=res[7]
    update=extract_dates(res[8])
    origin=res[-2]
    atd=res[-1].split(" (")[0]
    parsed_row = {
        'Destination': dest,
        'ARRIVAL': eta,
        'Status': status,
        'UpdateTime': update,
        'NavStatus':nav_status,
        'Last Port': origin,
        'ATD':atd
    }
    
    return parsed_row

chrome_options = Options()
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
# chrome_options.add_argument("--headless")
chrome_options.add_experimental_option("prefs", {
    "profile.default_content_settings.popups": 0,
    "download.prompt_for_download": False,
    "download.directory_upgrade": True
})
driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()), 
        options=chrome_options
    )
    
driver.get(f"https://tps.co.id:8081/webaccess/")
time.sleep(2)
soup = BeautifulSoup(driver.page_source, 'html.parser')
table = soup.find_all(class_='card-body p-5')
driver.quit()

import re
from datetime import datetime
data=[]
exp_text=table[0].text
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
exp_text=table[1].text
entries = re.split(r'\n{2,}', exp_text.strip())
for entri in entries:
    list_entri=entri.split("\n")
    vessel=list_entri[0].strip()
    eta=":".join(list_entri[2].split(":")[1:]).strip()
    etd=":".join(list_entri[3].split(":")[1:]).strip()
    OS=":".join(list_entri[4].split(":")[1:]).strip()
    CTC=":".join(list_entri[5].split(":")[1:]).strip()
    CTD=":".join(list_entri[6].split(":")[1:]).strip()
    sub_data={}
    sub_data["Vessel Name"]=vessel
    sub_data["Arrival"]=eta
    sub_data["Berthing"]=eta
    sub_data["Departure"]=etd
    sub_data["Open Stack"]=OS
    sub_data["Closing"]=CTC
    sub_data["Status"]="Schedule"
    sub_data["UpdateTime"]=datetime.now().strftime("%d/%m/%Y %H:%M")
    sub_data["Destination"]="Surabaya, Indonesia"
    sub_data["Origin"]="-"
    data.append(sub_data)

df_sub=pd.DataFrame(data)

chrome_options = Options()
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
# chrome_options.add_argument("--headless")
chrome_options.add_experimental_option("prefs", {
    "profile.default_content_settings.popups": 0,
    "download.prompt_for_download": False,
    "download.directory_upgrade": True
})
driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()), 
        options=chrome_options
    )
    
driver.get(f"https://ibstpks.pelindo.co.id/webaccess/")
time.sleep(2)
soup = BeautifulSoup(driver.page_source, 'html.parser')
table = soup.find_all(class_='vessel')
driver.quit()

data=[]
exp_text=table[0].text
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

exp_text=table[1].text
entries = re.split(r'\n{2,}', exp_text.strip())[1:]
entries_final = [entries[i]+entries[i+1] for i in range(0,len(entries),2)]

for entri in entries_final:
    if "Detail" in entri:
        continue
    list_entri=entri.split("\n")
    vessel=list_entri[0].split("(")[0].strip()
    company=list_entri[3].strip()
    vessel_type=list_entri[4].strip() if list_entri[4].strip()!="" else "DOMESTIC"
    etb=":".join(list_entri[5].split(":")[1:]).strip().replace("\n"," ")
    etd=":".join(list_entri[6].split(":")[1:]).strip().replace("\n"," ")
    os=":".join(list_entri[7].split(":")[1:]).strip().replace("\n"," ")
    ct=":".join(list_entri[8].split(":")[1:]).strip().replace("\n"," ")
    sub_data={}
    sub_data["Vessel Name"]=vessel
    sub_data["CompanyName"]=company
    sub_data["VesselType"]=vessel_type
    sub_data["Arrival"]=etb
    sub_data["Berthing"]=etb
    sub_data["Departure"]=etd
    sub_data["Open Stack"]=os
    sub_data["Closing"]=ct
    sub_data["Status"]="Confirmed"
    sub_data["UpdateTime"]=datetime.now().strftime("%d/%m/%Y %H:%M")
    sub_data["Destination"]="Semarang, Indonesia"
    sub_data["Origin"]="-"
    data.append(sub_data)

exp_text=table[2].text
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
    os=":".join(list_entri[8].split(":")[1:]).strip().replace("\n"," ")
    ct=":".join(list_entri[9].split(":")[1:]).strip().replace("\n"," ")
    sub_data={}
    sub_data["Vessel Name"]=vessel
    sub_data["CompanyName"]=company
    sub_data["VesselType"]=vessel_type
    sub_data["Arrival"]=etb
    sub_data["Berthing"]=etb
    sub_data["Departure"]=etd
    sub_data["Open Stack"]=os
    sub_data["Closing"]=ct
    sub_data["Status"]="Open Stack"
    sub_data["UpdateTime"]=datetime.now().strftime("%d/%m/%Y %H:%M")
    sub_data["Destination"]="Semarang, Indonesia"
    sub_data["Origin"]="-"
    data.append(sub_data)

exp_text=table[3].text
entries = re.split(r'\n{2,}', exp_text.split("Vessel")[3])[1:]
entries_final = [entries[i]+entries[i+1] for i in range(0,len(entries)-1,2)]

for entri in entries_final:
    if "Detail" in entri:
        continue
    list_entri=entri.split("\n")
    vessel=list_entri[0].split("(")[0].strip()
    company=list_entri[2].strip()
    vessel_type=list_entri[3].strip() if list_entri[3].strip()!="" else "DOMESTIC"
    etb=":".join(list_entri[5].split(":")[1:]).strip().replace("\n"," ")
    etd=":".join(list_entri[6].split(":")[1:]).strip().replace("\n"," ")
    sub_data={}
    sub_data["Vessel Name"]=vessel
    sub_data["CompanyName"]=company
    sub_data["VesselType"]=vessel_type
    sub_data["Arrival"]=etb
    sub_data["Berthing"]=etb
    sub_data["Departure"]=etd
    sub_data["Open Stack"]="-"
    sub_data["Closing"]="-"
    sub_data["Status"]="History"
    sub_data["UpdateTime"]=datetime.now().strftime("%d/%m/%Y %H:%M")
    sub_data["Origin"]="Semarang, Indonesia"
    sub_data["Destination"]="-"
    data.append(sub_data)

df_smg=pd.DataFrame(data)

chrome_options = Options()
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
# chrome_options.add_argument("--headless")
chrome_options.add_experimental_option("prefs", {
    "profile.default_content_settings.popups": 0,
    "download.prompt_for_download": False,
    "download.directory_upgrade": True
})
driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()), 
        options=chrome_options
    )
    
driver.get(f"https://www.jict.co.id/vessel-schedule")
time.sleep(5+2)
soup = BeautifulSoup(driver.page_source, 'html.parser')
table = soup.find('table')
driver.quit()

data=[]
if table:
    rows = table.find_all('tr')  # Mengambil semua row dalam tabel
    column_name=rows[0].text.split("\n")[1:-1]
    for row in rows[1:]:
        data.append(row.text.split("\n")[1:-1])

df_jict = pd.DataFrame(data, columns=column_name)

for i, sub_data in df_jict.iterrows():
    df_jict.loc[i, "UpdateTime"]=datetime.now().strftime("%d/%m/%Y %H:%M")
    if sub_data["Status"]=="-":
        df_jict.loc[i, "Destination"] = "Jakarta, Indonesia"
        df_jict.loc[i, "Origin"] = "-"
    else:
        df_jict.loc[i, "Origin"] = "Jakarta, Indonesia"
        df_jict.loc[i, "Destination"] = "-"

df_jict=df_jict[df_jict["Status"]!="SAILING"]
df_smg=df_smg[df_smg["Status"]!="History"]

combined_df = pd.concat([df_jict, df_sub, df_smg], join='inner', ignore_index=True)
combined_df = combined_df.reindex(columns=["Vessel Name","Origin","Destination","Arrival",	"Berthing",	"Departure",	"Closing",	"Open Stack","Status",	"UpdateTime"])
combined_df.columns = ["Vessel Name","Origin","Destination","Arrival",	"Berthing",	"Departure",	"Closing",	"Open Stack","Status",	"Update Time"]
df_imo=pd.read_excel("data/master_IMO_final.xlsx")
dict_imo={k:str(v) for k,v in zip(df_imo["Vessel"],df_imo["IMO"])}
combined_df["IMO"]=combined_df["Vessel Name"].map(dict_imo)
print("\n".join(combined_df[combined_df["IMO"].isnull()]["Vessel Name"].tolist()))

chrome_options = Options()
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
# chrome_options.add_argument("--headless")
chrome_options.add_experimental_option("prefs", {
    "profile.default_content_settings.popups": 0,
    "download.prompt_for_download": False,
    "download.directory_upgrade": True
})
chrome_options.page_load_strategy='eager'

data = []

def find_status(sub_df):
    global data
    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()), 
        options=chrome_options
    )
    
    try:
        driver.get(f"https://www.vesselfinder.com/vessels/details/{sub_df['IMO']}")
        time.sleep(2)
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        table = soup.find(class_='col vfix-top lpr')
        res = parse_shipping_data(table)
        if sub_df["Origin"]=="-":
            sub_df["Origin"]=res["Last Port"]
        else:
            sub_df["Destination"]=res["Destination"]
        data.append(sub_df)
        return res
    except Exception as e:
        print(f"Error processing {sub_df['IMO']}: {e}")
        data.append(sub_df)
        return None
    finally:
        driver.quit()

# Use ThreadPoolExecutor to run find_status concurrently
with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
    futures = []
    for i, sub_df in combined_df.iterrows():
        futures.append(executor.submit(find_status, sub_df))

df_final=pd.DataFrame(data)
df_final=df_final.reindex(columns=['Vessel Name',	'IMO',	'Origin',	'Destination',	'Arrival',	'Berthing',	'Departure',	'Closing',	'Open Stack',	'Status',	'Update Time'])
status_map={'-':'Schedule','Confirmed':'Schedule','Schedule':'Schedule','Open Stack':'Schedule','Alongside':'Alongside','WORKING':'Alongside'}
df_final["Status"]=df_final["Status"].map(status_map)
df_final.loc[(df_final['Status'] == 'Schedule') & (df_final['Arrival'] < df_final['Update Time']), 'Status'] = 'Pending'
df_final=df_final.sort_values(by=['Arrival']).reset_index(drop=True)
now_vessel=df_final["Vessel Name"].tolist()
if os.path.isfile("data/vessel_schedule.xlsx"):
    schedule_df=pd.read_excel("data/vessel_schedule.xlsx")
else:
    schedule_df=df_final
schedule_vessel=schedule_df["Vessel Name"].tolist()
history_vessel = schedule_df[~schedule_df["Vessel Name"].isin(now_vessel)]
history_status={'Schedule':'Canceled','Alongside':'History'}
history_vessel["Status"]=history_vessel["Status"].map(history_status)


fname="data/history_vessel.xlsx"
if os.path.isfile(fname):
    old_history=pd.read_excel(fname)
    new_history=pd.concat([history_vessel,old_history])
    new_history.to_excel(fname,index=False)
else:
    history_vessel.to_excel(fname,index=False)

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
    if not vessel_row.empty:
        schedule_vessel.iloc[i] = vessel_row.iloc[len(vessel_dict[schedule_vessel.iloc[i]["Vessel Name"]]) - 1]
    else:
        print(f"Tidak ada data ditemukan untuk '{schedule_vessel.iloc[i]['Vessel Name']}' pada df_final.")

now_df = df_final[~df_final["Vessel Name"].isin(schedule_df["Vessel Name"].tolist())]
result=pd.concat([schedule_vessel,now_df])
result.to_excel("data/vessel_schedule.xlsx")