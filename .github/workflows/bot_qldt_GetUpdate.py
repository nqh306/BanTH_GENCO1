import requests
import json
import pandas as pd
from datetime import datetime
import calendar
import urllib3
import time
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread.exceptions import APIError
import math
import numpy as np
import asyncio
import os

# Record the start time
start_time = time.time()
#Disable notification of warning can't verify when send request
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning) 

async def source_send_telegram(textMessage):
    import telegram
    try:
        token = '6422892988:AAGUP63bMTfcP6NXZJ49-CxKU66kN6R1_-0'
        chat_id = '-1001940344495'
        telegram_notify = telegram.Bot(token=token)
        await telegram_notify.send_message(chat_id= chat_id, text= textMessage, parse_mode='Markdown')
    except Exception as ex:
        print(ex)

def send_telegram(textMessage):
    asyncio.run(source_send_telegram(textMessage))

# Define Mappings and Constants Here
MAPPING_STATUS_TBMT = {
                        "DXT": "03. Đang xét thầu",
                        "CNTTT": "04. Có nhà thầu trúng thầu",
                        "KCNTTT": "05. Không có nhà thầu trúng thầu",
                        "DHT": "06. Đã huỷ thầu",
                        "DHTBMT": "07. Đã huỷ Thông báo mời thầu",
                        "": "02. Chưa đóng thầu"
                    }
MAPPING_STATUS_KHONG_TBMT = {
                            "DTRR": "01. Chưa có TBMT",
                            "DTHC": "00. Đấu thầu hạn chế",
                            "CHCT": "01. Chưa có TBMT",
                            "CHCTRG": "01. Chưa có TBMT",
                            "CDT": "00. Chỉ định thầu",
                            "CDTRG": "00. Chỉ định thầu rút gọn",
                            "LCNT_DB": "00. Lựa chọn nhà thầu trong trường hợp đặc biệt",
                            "MSTT": "00. Mua sắm trực tiếp",
                            "TTH": "00. Tự thực hiện",
                            None: "01. Chưa có TBMT"
                        }
MAPPING_TRONGNUOC_QUOCTE = {
                                0: "Quốc tế",
                                1: "Trong nước",
                                None: ""
                            }
MAPPING_HINHTHUC_DUTHAU = {
                                0: "Không qua mạng",
                                1: "Qua mạng",
                                None: ""
                            }
MAPPING_LOAI_HD = {
                    "TG": "Trọn gói",
                    "DGDC": "Theo đơn giá điều chỉnh",
                    "DGCD": "Theo đơn giá cố định",
                    "TG_DGCD": "Trọn gói và Theo đơn giá cố định",
                    "TG_DGDC": "Trọn gói và Theo đơn giá điều chỉnh",
                    None: ""
                    }
MAPPING_TENDONVI = {
                    "vn5701662152": "1. Tổng công ty Phát điện 1",
                    "vnz000009825": "9. Ban QLDA Nhiệt điện 3",
                    "vnz000005052": "2. CTNĐ Uông Bí",
                    "vnz000017073": "4. CTNĐ Duyên Hải",
                    "vnz000005091": "3. CTNĐ Nghi Sơn",
                    "vnz000023752": "5. CTTĐ Bản Vẽ",
                    "vnz000013297": "6. CTTĐ Sông Tranh",
                    "vnz000023738": "7. CTTĐ Đồng Nai",
                    "vnz000016981": "8. CTTĐ Đại Ninh",
                    "vn5800452036": "10. CTCP TĐ DHD",
                    "vn5700434869": "11. CTCP NĐ Quảng Ninh",
                    "vn0101264520": "12. VNPD",
                    "vn0102379203": "13. EVNI",
                    None: ""
                    }
MAPPING_LINHVUC = {
                    "HH": "Hàng hoá",
                    "TV": "Tư vấn",
                    "PTV": "Phi tư vấn",
                    "XL": "Xây lắp",
                    "HON_HOP": "Hỗn hợp",
                    None: ""
                    }
MAPPING_HINHTHUC_LCNT = {
                        "DTRR": "Đấu thầu rộng rãi",
                        "DTHC": "Đấu thầu hạn chế",
                        "CHCT": "Chào hàng cạnh tranh",
                        "CHCTRG": "Chào hàng cạnh tranh rút gọn",
                        "CDT": "Chỉ định thầu",
                        "CDTRG": "Chỉ định thầu rút gọn",
                        "LCNT_DB": "Lựa chọn nhà thầu trong trường hợp đặc biệt",
                        "MSTT": "Mua sắm trực tiếp",
                        "TTH": "Tự thực hiện",
                        None: ""
                        }
MAPPING_PHUONGTHUC_LCNT = {
                            "1_MTHS": "Một giai đoạn một túi hồ sơ",
                            "1_HTHS": "Một giai đoạn hai túi hồ sơ",
                            None: ""
                            }
MAPPING_PHANLOAI_KHLCNT = {
                            "TX": "Thường xuyên",
                            "DTPT": "Đầu tư phát triển",
                            "KHAC": "Khác",
                            None: ""
                            }
MAPPING_HINHTHUC_QLDA = {
                        "KHAC": "Hình thức khác",
                        "TTQLDA": "Chủ đầu tư trực tiếp quản lý dự án",
                        None: ""
                        }
MAPPING_NHOM_DUAN = {
                        "KHAC": "Dự án khác",
                        "NA": "Nhóm A",
                        "NB": "Nhóm B",
                        "NC": "Nhóm C",
                        None: ""
                    }
MAPPING_VIETTAT_THOIGIAN = {
                        "M": "Tháng",
                        "D": "Ngày",
                        "Y": "Năm",
                        None: ""
                    }
MAPPING_EVALTYPE_HSDXKT = {
                            1: "Đánh giá tính hợp lệ",
                            2: "Đánh giá năng lực kinh nghiệm",
                            3: "Đánh giá kỹ thuật",
                            5: "Đánh giá Năng lực kinh nghiệm (Nhân sự chủ chốt)",
                            6: "Đánh giá Năng lực kinh nghiệm (Thiết bị thi công)",
                            None: ""
                        }
MAPPING_RESULT_HSDXKT = {
                            "0": "Không đạt",
                            "2": "Đạt",
                        }
MAPPING_PHANLO = {
                            0: "Không chia phần/lô",
                            1: "Có chia phần/ lô",
                        }
API_BASE_URL = "https://muasamcong.mpi.gov.vn/o/egp-portal-contractor-selection-v2/services"
API_HEADER = {
  'Accept': 'application/json, text/plain, */*',
  'Accept-Language': 'vi,en;q=0.9,en-US;q=0.8',
  'Connection': 'keep-alive',
  'Content-Type': 'application/json',
  'Cookie': 'COOKIE_SUPPORT=true; GUEST_LANGUAGE_ID=vi_VN; _ga=GA1.1.544732483.1690811905; JSESSIONID=KQX6yDBVu02PRfbPr-fXmK7NCSd7yVwB0HSG68eY.dc_app1_02; NSC_WT_QSE_QPSUBM_NTD_NQJ=ffffffffaf183e2245525d5f4f58455e445a4a4217de; LFR_SESSION_STATE_20103=1694413734216; citrix_ns_id=AAE7i7L-ZDtOpzsCAAAAADuFeyfrzB16Q6f2OzBmufpqrxJznGrtoFWvkMWyR9BuOw==LLf-ZA==nkc4GeqrCxXTW208xAufTvdv-PY=; _ga_19996Z37EE=GS1.1.1694413452.39.1.1694413736.0.0.0; citrix_ns_id=AAE7Isn_ZDu03TwCAAAAADuFeyfrzB16Q6f2OzBmufpqrxJznGrtoFWvkMWyR9BuOw==psz_ZA==M91hjjdUAkb5kxFYhFbfAN8BNY0=; JSESSIONID=L4bxGs_tLXujeFFRCaj7pOvbsNQwgXLHkyX8OWpe.dc_app1_02; NSC_WT_QSE_QPSUBM_NTD_NQJ=ffffffffaf183e2245525d5f4f58455e445a4a4217de',
  'Origin': 'https://muasamcong.mpi.gov.vn',
  'Referer': 'https://muasamcong.mpi.gov.vn/web/guest/contractor-selection?render=index',
  'Sec-Fetch-Dest': 'empty',
  'Sec-Fetch-Mode': 'cors',
  'Sec-Fetch-Site': 'same-origin',
  'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36',
  'sec-ch-ua': '"Chromium";v="116", "Not)A;Brand";v="24", "Google Chrome";v="116"',
  'sec-ch-ua-mobile': '?0',
  'sec-ch-ua-platform': '"macOS"'
}
#"vn5701662152 AND vnz000009825 AND vnz000005052 AND vnz000017073 AND vnz000005091 AND vnz000023752 AND vnz000013297 AND vnz000023738 AND vnz000016981 AND vn5800452036 AND vn5700434869 AND vn0101264520 AND vn0102379203"
payload_ds_KHLCNT = json.dumps([{ "pageSize": 10000,"pageNumber": 0,"query": [{"index": "es-contractor-selection","keyWord":"vn5701662152 AND vnz000009825 AND vnz000005052 AND vnz000017073 AND vnz000005091 AND vnz000023752 AND vnz000013297 AND vnz000023738 AND vnz000016981 AND vn5800452036 AND vn5700434869 AND vn0101264520 AND vn0102379203",
                                            "matchType": "all-1","matchFields": ["planNo","name","investorName","procuringEntityName"],"filters": [{"fieldName": "type","searchType": "in","fieldValues": ["es-plan-project-p"]}]}]}])
# token_QLDT_GoogleSheet = "token-qldt.json"
token_QLDT_GoogleSheet = os.environ.get("TOKEN_GOOGLE_SHEET_QLDT")
if not token_QLDT_GoogleSheet:
    raise RuntimeError("TOKEN_GOOGLE_SHEET_QLDT not found")
else:
    print("TOKEN_GOOGLE_SHEET_QLDT has been installed")
spreadsheetID_QLDT = "1lfkxn5sPh1lqTmyvUe3jSyXNXu2ZG0CSiMEuyId6TaE"

#FUNCTION TO UPLOAD DATAFRAME TO GOOGLESHEET
def is_dataframe_empty(data_frame, name="DataFrame"):
    if data_frame.empty:
        print(f"{name} is empty.")
        return True
    return False

def upload_dataframe_to_google_sheet(token_file, spreadsheet_id, worksheet_name, data_frame, replace_data=True, column_name_to_delete=None, id_values_to_delete=None):
    data_frame = data_frame.replace([np.inf, -np.inf], np.nan).fillna('')
    try:
        if is_dataframe_empty(data_frame):
            return False, "DataFrame is empty for worksheet: " + str(worksheet_name)
        else:
            # Define the scope and authenticate using the service account credentials
            scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
            creds = ServiceAccountCredentials.from_json_keyfile_name(token_file, scope)
            client = gspread.authorize(creds)
            # Open the Google Sheet by its spreadsheet_id
            spreadsheet = client.open_by_key(spreadsheet_id)
            worksheet = spreadsheet.worksheet(worksheet_name)
            if replace_data == True:
                # Clear the existing data and insert new data
                worksheet.clear()
                data_to_insert = [data_frame.columns.tolist()] + data_frame.values.tolist()
                worksheet.insert_rows(data_to_insert, 1)
            else:
                if len(worksheet.get_all_values()) > 1:
                    #DELETE DATA BEFORE APPEND
                    data = worksheet.get_all_values()
                    header = data[0]
                    data = data[1:]
                    # Find the index of the ID column
                    id_column_index = header.index(column_name_to_delete)
                    # Create a new data list excluding rows with specified ID values
                    new_data = [row for row in data if row[id_column_index] not in id_values_to_delete]
                    # Clear the existing data in the worksheet
                    worksheet.clear()
                    # Insert the new data
                    if new_data:
                        data_to_insert = [header] + new_data
                        worksheet.insert_rows(data_to_insert)
                
                # Append data to the worksheet
                data_to_insert = data_frame.values.tolist()
                num_rows = len(worksheet.get_all_values())
                worksheet.insert_rows(data_to_insert, num_rows + 1)
            return True, "Data uploaded successfully for worksheet: " + str(worksheet_name)
    except APIError as e:
        return False, f"API Error: {e}"
    except Exception as e:
        # Check if the error message indicates JSON serialization issue
        error_message = str(e)
        if "Out of range float values are not JSON compliant" in error_message:
            # Identify the problematic rows or values in the DataFrame
            problematic_rows = data_frame[data_frame.applymap(lambda x: isinstance(x, float) and (math.isnan(x) or math.isinf(x))).any(axis=1)]
            return False, f"Data contains non-JSON compliant float values in rows:\n{problematic_rows}"
        else:
            return False, f"An error occurred: {e}"

# Define error classes for better error handling
class APIRequestError(Exception):
    pass
class JSONDecodeError(Exception):
    pass

def get_listValue_from_col_in_GoogleSheet(token, spreadsheet_id, worksheet_name, column_number):
    try:
        # Define the scope of access (in this case, just Google Sheets)
        scope = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
        # Initialize the credentials and client
        credentials = ServiceAccountCredentials.from_json_keyfile_name(token, scope)
        client = gspread.authorize(credentials)
        # Open the Google Sheet by its URL or title
        spreadsheet = client.open_by_key(spreadsheet_id)
        worksheet = spreadsheet.worksheet(worksheet_name)
        # Check if the specified column exists
        num_columns = worksheet.col_count
        if column_number > num_columns:
            raise ValueError("Column number exceeds the number of columns in the worksheet")
        # Get all values in the specified column as a list
        column_values = worksheet.col_values(column_number)
        # Remove empty or None values (optional)
        column_values = [value for value in column_values if value]
        return column_values
    except Exception as e:
        return f"Error: {str(e)}"
    
def get_listValue_from_col_in_GoogleSheet_with_condition(token, spreadsheet_id, worksheet_name, column_number_need, column_number_condition, text_match_condition):
    try:
        # Define the scope of access (in this case, just Google Sheets)
        scope = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
        # Initialize the credentials and client
        credentials = ServiceAccountCredentials.from_json_keyfile_name(token, scope)
        client = gspread.authorize(credentials)
        # Open the Google Sheet by its URL or title
        spreadsheet = client.open_by_key(spreadsheet_id)
        worksheet = spreadsheet.worksheet(worksheet_name)
        # Check if the specified column exists
        num_columns = worksheet.col_count
        if column_number_need > num_columns:
            raise ValueError("Column number exceeds the number of columns in the worksheet")
        # Create a set to store matching values (to automatically remove duplicates)
        matching_values_set = set()
        # Extract values from the specified column where the condition is met
        for row in worksheet.get_all_values():
            if row[column_number_condition] == text_match_condition:
                matching_values_set.add(row[column_number_need])
        # Convert the set back to a list if needed
        matching_values_list = list(matching_values_set)
        return matching_values_list
    except Exception as e:
        return f"Error: {str(e)}"
    
def get_values_from_2_columns_with_condition(token, spreadsheet_id, worksheet_name, column_number1, column_number2, column_number_condition, text_match_condition):
    try:
        # Define the scope of access (in this case, just Google Sheets)
        scope = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
        # Initialize the credentials and client
        credentials = ServiceAccountCredentials.from_json_keyfile_name(token, scope)
        client = gspread.authorize(credentials)
        # Open the Google Sheet by its URL or title
        spreadsheet = client.open_by_key(spreadsheet_id)
        worksheet = spreadsheet.worksheet(worksheet_name)
        # Check if the specified columns exist
        num_columns = worksheet.col_count
        if column_number1 > num_columns or column_number2 > num_columns or column_number_condition > num_columns:
            raise ValueError("Column number exceeds the number of columns in the worksheet")
        # Create a list to store matching values
        matching_values = []
        # Extract values from the specified columns where the condition is met
        for row in worksheet.get_all_values():
            if row[column_number_condition] == text_match_condition:
                matching_values.append((row[column_number1], row[column_number2]))
        return matching_values
    except Exception as e:
        return f"Error: {str(e)}"

def fetch_data_from_api(url, payload, headers):
    try:
        response = requests.post(url, headers=headers, data=payload, timeout=30, verify=True)
        response.raise_for_status()
        return json.loads(response.text)
    except requests.exceptions.SSLError as e:
        print(f"SSL Error: {e}")

def process_get_chitiet_KHLCNT_GOITHAU(id_KHLCNT):
    try:
        data_api_KHLCNT_ct = fetch_data_from_api(url=f"{API_BASE_URL}/expose/lcnt/bid-po-bidp-plan-project-view/get-by-id",
                                                payload=json.dumps({"id": id_KHLCNT}),
                                                headers=API_HEADER)
    except APIRequestError as e:
        print(e)
        data_api_KHLCNT_ct = None

    if data_api_KHLCNT_ct:
        print('[STEP 1- LẤY THÔNG TIN CHI TIẾT KHCLNT] ID: ' + str(id_KHLCNT))
        #LẤY THÔNG TIN CHI TIẾT KHLCNT
        thoi_gian_thuc_hien_du_an = None
        pperiod = data_api_KHLCNT_ct['bidPoBidpPlanProjectDetailView']['pperiod']
        pperiod_unit = data_api_KHLCNT_ct['bidPoBidpPlanProjectDetailView']['pperiodUnit']
        if pperiod is not None:
            if pperiod_unit == "D":
                thoi_gian_thuc_hien_du_an = f"{pperiod} ngày"
            else:
                thoi_gian_thuc_hien_du_an = f"{pperiod} tháng"
        data_ct_khlcnt = {
                        'MaDinhDanh': [str(data_api_KHLCNT_ct['bidpPlanDetailToProjectList'][0]['createdBy']).split("-")[0]],
                        'TenDonVi': [MAPPING_TENDONVI[str(data_api_KHLCNT_ct['bidpPlanDetailToProjectList'][0]['createdBy']).split("-")[0]]],
                        'planID': [data_api_KHLCNT_ct['bidPoBidpPlanProjectDetailView']['id']], 
                        'planNo': [data_api_KHLCNT_ct['bidPoBidpPlanProjectDetailView']['planNo']],
                        'planName': [data_api_KHLCNT_ct['bidPoBidpPlanProjectDetailView']['name']],
                        'planVersion': [data_api_KHLCNT_ct['bidPoBidpPlanProjectDetailView']['planVersion']],
                        'planStatus': [data_api_KHLCNT_ct['bidPoBidpPlanProjectDetailView']['status']],
                        'TenDuToanMuaSam': [data_api_KHLCNT_ct['bidPoBidpPlanProjectDetailView']['pname']],
                        'SoLuongGoiThau': [data_api_KHLCNT_ct['bidPoBidpPlanProjectDetailView']['bidPack']],
                        'DuToanMuaSam': [data_api_KHLCNT_ct['bidPoBidpPlanProjectDetailView']['investTotal']],
                        'CCY_DuToanMuaSam': [data_api_KHLCNT_ct['bidPoBidpPlanProjectDetailView']['investTotalUnit']],
                        'DuToanMuaSam_BangChu': [data_api_KHLCNT_ct['bidPoBidpPlanProjectDetailView']['investTotalUnit']],
                        'QD_KHLCNT_So': [data_api_KHLCNT_ct['bidPoBidpPlanProjectDetailView']['decisionNo']],
                        'QD_KHLCNT_Ngay': [datetime.strptime(data_api_KHLCNT_ct['bidPoBidpPlanProjectDetailView']['decisionDate'], "%Y-%m-%dT%H:%M:%S").strftime("%d/%m/%Y")],
                        'QD_KHLCNT_NoiBanHanh': [data_api_KHLCNT_ct['bidPoBidpPlanProjectDetailView']['decisionAgency']],
                        'QD_KHLCNT_FileID': [data_api_KHLCNT_ct['bidPoBidpPlanProjectDetailView']['decisionFileId']],
                        'QD_KHLCNT_FileName': [data_api_KHLCNT_ct['bidPoBidpPlanProjectDetailView']['decisionFileName']],
                        'NgayDangTai_KHLCNT': [datetime.strptime(str(data_api_KHLCNT_ct['bidPoBidpPlanProjectDetailView']['publicDate']).split(".")[0], "%Y-%m-%dT%H:%M:%S").strftime("%d/%m/%Y")],
                        'PHANLOAI_KHLCNT': [MAPPING_PHANLOAI_KHLCNT[data_api_KHLCNT_ct['bidPoBidpPlanProjectDetailView']['planType']]],
                        'MucTieuDauTu': [data_api_KHLCNT_ct['bidPoBidpPlanProjectDetailView']['investTarget']],
                        'SuDungVonODA': [data_api_KHLCNT_ct['bidPoBidpPlanProjectDetailView']['isOda']],
                        'DiaDiemThucHien': [data_api_KHLCNT_ct['bidPoBidpPlanProjectDetailView']['location']],
                        'HinhThucQLDA': [MAPPING_HINHTHUC_QLDA[data_api_KHLCNT_ct['bidPoBidpPlanProjectDetailView']['pform']]],
                        'NhomDuAn': [MAPPING_NHOM_DUAN[data_api_KHLCNT_ct['bidPoBidpPlanProjectDetailView']['pgroup']]],
                        'ThoiGianThucHienDuAn': [thoi_gian_thuc_hien_du_an]
                    }

        df_GOITHAU = pd.DataFrame()
        #LẤY LIST GÓI THẦU THUỘC KHLCNT
        for j in range(len(data_api_KHLCNT_ct['lsBidpPlanDetailDTO'])):
            print('[STEP 2- LẤY THÔNG TIN CƠ BẢN CỦA GÓI THẦU] ' + str(j+1) + "/" + str(len(data_api_KHLCNT_ct['lsBidpPlanDetailDTO'])) + ": [ID_GOITHAU] " + data_api_KHLCNT_ct['lsBidpPlanDetailDTO'][j]['id'])
            #XỬ LÝ THỜI GIAN BẮT ĐẦU TỔ CHỨC LCNT
            giatri_quy = data_api_KHLCNT_ct['lsBidpPlanDetailDTO'][j]["bidStartQuarter"]
            giatri_nam = data_api_KHLCNT_ct['lsBidpPlanDetailDTO'][j]["bidStartYear"]
            if data_api_KHLCNT_ct['lsBidpPlanDetailDTO'][j]["bidStartUnit"] == "Q":
                quy_to_month = {"I": 3, "II": 6, "III": 9, "IV": 12}
                giatri_thang = quy_to_month.get(giatri_quy)
                giatri_batdau_LCNT_ct = datetime(giatri_nam, giatri_thang, calendar.monthrange(giatri_nam, giatri_thang)[1]).strftime("%d/%m/%Y")
                TGIAN_BATDAU_TOCHUC_LCNT = f"Quý {giatri_quy}, {giatri_nam}"
                TGIAN_BATDAU_TOCHUC_LCNT_CHITIET = giatri_batdau_LCNT_ct
            else:
                giatri_thang = int(data_api_KHLCNT_ct['lsBidpPlanDetailDTO'][j]["bidStartMonth"])
                TGIAN_BATDAU_TOCHUC_LCNT = f"Tháng {giatri_thang}, {giatri_nam}"
                TGIAN_BATDAU_TOCHUC_LCNT_CHITIET = datetime(giatri_nam, giatri_thang, calendar.monthrange(giatri_nam, giatri_thang)[1]).strftime("%d/%m/%Y")
            THANG_BAOCAO = giatri_thang
            NAM_BAOCAO = giatri_nam
            Ma_TBMT = ""
            ID_TBMT = ""
            numBidderJoin = ""
            bidRealityOpenDate = ""
            STATUS_BID = ""
            if data_api_KHLCNT_ct['lsBidpPlanDetailDTO'][j]["linkNotifyInfo"] != None:
                Ma_TBMT = str(data_api_KHLCNT_ct['lsBidpPlanDetailDTO'][j]["linkNotifyInfo"]).split('","')[0].split('":"')[1]
                ID_TBMT = str(data_api_KHLCNT_ct['lsBidpPlanDetailDTO'][j]["linkNotifyInfo"]).split('","')[2].split('":"')[1]
                payload_search_TBMT = json.dumps([
                                        {
                                            "pageSize": 10,
                                            "pageNumber": "0",
                                            "query": [
                                            {
                                                "index": "es-contractor-selection",
                                                "keyWord": Ma_TBMT,
                                                "matchType": "all-1",
                                                "matchFields": [
                                                "notifyNo",
                                                "bidName"
                                                ],
                                                "filters": [
                                                {
                                                    "fieldName": "type",
                                                    "searchType": "in",
                                                    "fieldValues": [
                                                    "es-notify-contractor"
                                                    ]
                                                },
                                                {
                                                    "fieldName": "caseKHKQ",
                                                    "searchType": "not_in",
                                                    "fieldValues": [
                                                    "1"
                                                    ]
                                                }
                                                ]
                                            }
                                            ]
                                        }
                                        ])
                data_api_search_TBMT = fetch_data_from_api(url=f"{API_BASE_URL}/smart/search",payload=payload_search_TBMT,headers=API_HEADER)
                STATUS_BID = data_api_search_TBMT['page']['content'][0]["status"]
                STATUS_TBMT = MAPPING_STATUS_TBMT[data_api_search_TBMT['page']['content'][0]["statusForNotify"]]
                if "numBidderJoin" in data_api_search_TBMT['page']['content'][0]:
                    numBidderJoin = data_api_search_TBMT['page']['content'][0]["numBidderJoin"]
                if "bidRealityOpenDate" in data_api_search_TBMT['page']['content'][0]:
                    bidRealityOpenDate = data_api_search_TBMT['page']['content'][0]["bidRealityOpenDate"]
            else:
                STATUS_TBMT = MAPPING_STATUS_KHONG_TBMT[data_api_KHLCNT_ct['lsBidpPlanDetailDTO'][j]["bidForm"]]
                
            data_GoiThau = {
                                'MaDinhDanh': [str(data_api_KHLCNT_ct['bidpPlanDetailToProjectList'][0]['createdBy']).split("-")[0]],
                                'TenDonVi': [MAPPING_TENDONVI[str(data_api_KHLCNT_ct['bidpPlanDetailToProjectList'][0]['createdBy']).split("-")[0]]],
                                'ID_GOITHAU': [data_api_KHLCNT_ct['lsBidpPlanDetailDTO'][j]["id"]],
                                'planNo': [data_api_KHLCNT_ct['lsBidpPlanDetailDTO'][j]["planNo"]],
                                'planID': [data_api_KHLCNT_ct['lsBidpPlanDetailDTO'][j]["planId"]],
                                'TEN_GOITHAU': [data_api_KHLCNT_ct['lsBidpPlanDetailDTO'][j]["bidName"]],
                                'TRONGNUOC_QUOCTE': [MAPPING_TRONGNUOC_QUOCTE[data_api_KHLCNT_ct['lsBidpPlanDetailDTO'][j]["isDomestic"]]],
                                'HINHTHUC_DUTHAU': [MAPPING_HINHTHUC_DUTHAU[data_api_KHLCNT_ct['lsBidpPlanDetailDTO'][j]["isInternet"]]],
                                'LOAI_HOPDONG': [MAPPING_LOAI_HD[data_api_KHLCNT_ct['lsBidpPlanDetailDTO'][j]["ctype"]]],
                                'PHUONGTHUC_LCNT': [MAPPING_PHUONGTHUC_LCNT[data_api_KHLCNT_ct['lsBidpPlanDetailDTO'][j]["bidMode"]]],
                                'HINHTHUC_LCNT': [MAPPING_HINHTHUC_LCNT[data_api_KHLCNT_ct['lsBidpPlanDetailDTO'][j]["bidForm"]]],
                                'LINH_VUC': [MAPPING_LINHVUC[data_api_KHLCNT_ct['lsBidpPlanDetailDTO'][j]["bidField"]]],
                                'GIA_GOITHAU': [data_api_KHLCNT_ct['lsBidpPlanDetailDTO'][j]["bidPrice"]],
                                'CCY_GIA_GOITHAU': [data_api_KHLCNT_ct['lsBidpPlanDetailDTO'][j]["bidPriceUnit"]],
                                'PHAN_LO': [MAPPING_PHANLO[data_api_KHLCNT_ct['lsBidpPlanDetailDTO'][j]["isMultiLot"]]],
                                'NGUON_VON': [data_api_KHLCNT_ct['lsBidpPlanDetailDTO'][j]["capitalDetail"]],
                                'THOIGIAN_THUCHIEN_HOPDONG': [str(data_api_KHLCNT_ct['lsBidpPlanDetailDTO'][j]["cperiod"]) + " " + str(MAPPING_VIETTAT_THOIGIAN[data_api_KHLCNT_ct['lsBidpPlanDetailDTO'][j]["cperiodUnit"]])],
                                'NAM_BAOCAO': [NAM_BAOCAO],
                                'THANG_BAOCAO': [THANG_BAOCAO],
                                'TGIAN_BATDAU_TOCHUC_LCNT': [TGIAN_BATDAU_TOCHUC_LCNT],
                                'TGIAN_BATDAU_TOCHUC_LCNT_CHITIET': [TGIAN_BATDAU_TOCHUC_LCNT_CHITIET],
                                'MA_TBMT': [Ma_TBMT],
                                'ID_TBMT': [ID_TBMT],
                                'STATUS_TBMT': [STATUS_TBMT],
                                'STATUS_BID': [STATUS_BID],
                                'linkNotifyInfo': [data_api_KHLCNT_ct['lsBidpPlanDetailDTO'][j]["linkNotifyInfo"]],
                                "numBidderJoin": [numBidderJoin],
                                "bidRealityOpenDate": [bidRealityOpenDate]
                            }
            df_GOITHAU = pd.concat([df_GOITHAU, pd.DataFrame(data_GoiThau)], ignore_index=True)
        
        return data_ct_khlcnt, df_GOITHAU

def check_new_TBMT(id_KHLCNT,list_maTBMT):
    try:
        data_api_KHLCNT_ct = fetch_data_from_api(url=f"{API_BASE_URL}/expose/lcnt/bid-po-bidp-plan-project-view/get-by-id",
                                                payload=json.dumps({"id": id_KHLCNT}),
                                                headers=API_HEADER)
    except APIRequestError as e:
        print(e)
        data_api_KHLCNT_ct = None
    if data_api_KHLCNT_ct:
        df_GOITHAU = pd.DataFrame()
        #LẤY LIST GÓI THẦU THUỘC KHLCNT
        for j in range(len(data_api_KHLCNT_ct['lsBidpPlanDetailDTO'])):
            if data_api_KHLCNT_ct['lsBidpPlanDetailDTO'][j]["linkNotifyInfo"] != None:
                Ma_TBMT = str(data_api_KHLCNT_ct['lsBidpPlanDetailDTO'][j]["linkNotifyInfo"]).split('","')[0].split('":"')[1]
                ID_TBMT = str(data_api_KHLCNT_ct['lsBidpPlanDetailDTO'][j]["linkNotifyInfo"]).split('","')[2].split('":"')[1]
                if Ma_TBMT not in list_maTBMT:
                    print("[TÌM THẤY TBMT MỚI ĐĂNG TẢI] " + str(Ma_TBMT))
                    #XỬ LÝ THỜI GIAN BẮT ĐẦU TỔ CHỨC LCNT
                    giatri_quy = data_api_KHLCNT_ct['lsBidpPlanDetailDTO'][j]["bidStartQuarter"]
                    giatri_nam = data_api_KHLCNT_ct['lsBidpPlanDetailDTO'][j]["bidStartYear"]
                    if data_api_KHLCNT_ct['lsBidpPlanDetailDTO'][j]["bidStartUnit"] == "Q":
                        quy_to_month = {"I": 3, "II": 6, "III": 9, "IV": 12}
                        giatri_thang = quy_to_month.get(giatri_quy)
                        giatri_batdau_LCNT_ct = datetime(giatri_nam, giatri_thang, calendar.monthrange(giatri_nam, giatri_thang)[1]).strftime("%d/%m/%Y")
                        TGIAN_BATDAU_TOCHUC_LCNT = f"Quý {giatri_quy}, {giatri_nam}"
                        TGIAN_BATDAU_TOCHUC_LCNT_CHITIET = giatri_batdau_LCNT_ct
                    else:
                        giatri_thang = int(data_api_KHLCNT_ct['lsBidpPlanDetailDTO'][j]["bidStartMonth"])
                        TGIAN_BATDAU_TOCHUC_LCNT = f"Tháng {giatri_thang}, {giatri_nam}"
                        TGIAN_BATDAU_TOCHUC_LCNT_CHITIET = datetime(giatri_nam, giatri_thang, calendar.monthrange(giatri_nam, giatri_thang)[1]).strftime("%d/%m/%Y")
                    THANG_BAOCAO = giatri_thang
                    NAM_BAOCAO = giatri_nam
                    numBidderJoin = ""
                    bidRealityOpenDate = ""
                    STATUS_BID = ""
                    payload_search_TBMT = json.dumps([
                                            {
                                                "pageSize": 10,
                                                "pageNumber": "0",
                                                "query": [
                                                {
                                                    "index": "es-contractor-selection",
                                                    "keyWord": Ma_TBMT,
                                                    "matchType": "all-1",
                                                    "matchFields": [
                                                    "notifyNo",
                                                    "bidName"
                                                    ],
                                                    "filters": [
                                                    {
                                                        "fieldName": "type",
                                                        "searchType": "in",
                                                        "fieldValues": [
                                                        "es-notify-contractor"
                                                        ]
                                                    },
                                                    {
                                                        "fieldName": "caseKHKQ",
                                                        "searchType": "not_in",
                                                        "fieldValues": [
                                                        "1"
                                                        ]
                                                    }
                                                    ]
                                                }
                                                ]
                                            }
                                            ])
                    data_api_search_TBMT = fetch_data_from_api(url=f"{API_BASE_URL}/smart/search",payload=payload_search_TBMT,headers=API_HEADER)
                    STATUS_BID = data_api_search_TBMT['page']['content'][0]["status"]
                    STATUS_TBMT = MAPPING_STATUS_TBMT[data_api_search_TBMT['page']['content'][0]["statusForNotify"]]
                    if "numBidderJoin" in data_api_search_TBMT['page']['content'][0]:
                        numBidderJoin = data_api_search_TBMT['page']['content'][0]["numBidderJoin"]
                    if "bidRealityOpenDate" in data_api_search_TBMT['page']['content'][0]:
                        bidRealityOpenDate = data_api_search_TBMT['page']['content'][0]["bidRealityOpenDate"]
                
                    data_GoiThau = {
                                        'MaDinhDanh': [str(data_api_KHLCNT_ct['bidpPlanDetailToProjectList'][0]['createdBy']).split("-")[0]],
                                        'TenDonVi': [MAPPING_TENDONVI[str(data_api_KHLCNT_ct['bidpPlanDetailToProjectList'][0]['createdBy']).split("-")[0]]],
                                        'ID_GOITHAU': [data_api_KHLCNT_ct['lsBidpPlanDetailDTO'][j]["id"]],
                                        'planNo': [data_api_KHLCNT_ct['lsBidpPlanDetailDTO'][j]["planNo"]],
                                        'planID': [data_api_KHLCNT_ct['lsBidpPlanDetailDTO'][j]["planId"]],
                                        'TEN_GOITHAU': [data_api_KHLCNT_ct['lsBidpPlanDetailDTO'][j]["bidName"]],
                                        'TRONGNUOC_QUOCTE': [MAPPING_TRONGNUOC_QUOCTE[data_api_KHLCNT_ct['lsBidpPlanDetailDTO'][j]["isDomestic"]]],
                                        'HINHTHUC_DUTHAU': [MAPPING_HINHTHUC_DUTHAU[data_api_KHLCNT_ct['lsBidpPlanDetailDTO'][j]["isInternet"]]],
                                        'LOAI_HOPDONG': [MAPPING_LOAI_HD[data_api_KHLCNT_ct['lsBidpPlanDetailDTO'][j]["ctype"]]],
                                        'PHUONGTHUC_LCNT': [MAPPING_PHUONGTHUC_LCNT[data_api_KHLCNT_ct['lsBidpPlanDetailDTO'][j]["bidMode"]]],
                                        'HINHTHUC_LCNT': [MAPPING_HINHTHUC_LCNT[data_api_KHLCNT_ct['lsBidpPlanDetailDTO'][j]["bidForm"]]],
                                        'LINH_VUC': [MAPPING_LINHVUC[data_api_KHLCNT_ct['lsBidpPlanDetailDTO'][j]["bidField"]]],
                                        'GIA_GOITHAU': [data_api_KHLCNT_ct['lsBidpPlanDetailDTO'][j]["bidPrice"]],
                                        'CCY_GIA_GOITHAU': [data_api_KHLCNT_ct['lsBidpPlanDetailDTO'][j]["bidPriceUnit"]],
                                        'PHAN_LO': [MAPPING_PHANLO[data_api_KHLCNT_ct['lsBidpPlanDetailDTO'][j]["isMultiLot"]]],
                                        'NGUON_VON': [data_api_KHLCNT_ct['lsBidpPlanDetailDTO'][j]["capitalDetail"]],
                                        'THOIGIAN_THUCHIEN_HOPDONG': [str(data_api_KHLCNT_ct['lsBidpPlanDetailDTO'][j]["cperiod"]) + " " + str(MAPPING_VIETTAT_THOIGIAN[data_api_KHLCNT_ct['lsBidpPlanDetailDTO'][j]["cperiodUnit"]])],
                                        'NAM_BAOCAO': [NAM_BAOCAO],
                                        'THANG_BAOCAO': [THANG_BAOCAO],
                                        'TGIAN_BATDAU_TOCHUC_LCNT': [TGIAN_BATDAU_TOCHUC_LCNT],
                                        'TGIAN_BATDAU_TOCHUC_LCNT_CHITIET': [TGIAN_BATDAU_TOCHUC_LCNT_CHITIET],
                                        'MA_TBMT': [Ma_TBMT],
                                        'ID_TBMT': [ID_TBMT],
                                        'STATUS_TBMT': [STATUS_TBMT],
                                        'STATUS_BID': [STATUS_BID],
                                        'linkNotifyInfo': [data_api_KHLCNT_ct['lsBidpPlanDetailDTO'][j]["linkNotifyInfo"]],
                                        "numBidderJoin": [numBidderJoin],
                                        "bidRealityOpenDate": [bidRealityOpenDate]
                                    }
                    df_GOITHAU = pd.concat([df_GOITHAU, pd.DataFrame(data_GoiThau)], ignore_index=True)
        return df_GOITHAU

def get_info_GoiThau_by_MaTBMT_from_id_KHLCNT(id_KHLCNT,MaTBMT):
    try:
        data_api_KHLCNT_ct = fetch_data_from_api(url=f"{API_BASE_URL}/expose/lcnt/bid-po-bidp-plan-project-view/get-by-id",
                                                payload=json.dumps({"id": id_KHLCNT}),
                                                headers=API_HEADER)
    except APIRequestError as e:
        print(e)
        data_api_KHLCNT_ct = None
    if data_api_KHLCNT_ct:
        df_GOITHAU = pd.DataFrame()
        #LẤY LIST GÓI THẦU THUỘC KHLCNT
        for j in range(len(data_api_KHLCNT_ct['lsBidpPlanDetailDTO'])):
            print('[GET BID FROM PLAN] ' + str(j+1) + "/" + str(len(data_api_KHLCNT_ct['lsBidpPlanDetailDTO'])) + ": [ID_GOITHAU]" + data_api_KHLCNT_ct['lsBidpPlanDetailDTO'][j]['id'])
            if data_api_KHLCNT_ct['lsBidpPlanDetailDTO'][j]["linkNotifyInfo"] != None:
                Ma_TBMT = str(data_api_KHLCNT_ct['lsBidpPlanDetailDTO'][j]["linkNotifyInfo"]).split('","')[0].split('":"')[1]
                ID_TBMT = str(data_api_KHLCNT_ct['lsBidpPlanDetailDTO'][j]["linkNotifyInfo"]).split('","')[2].split('":"')[1]
                if Ma_TBMT == MaTBMT:
                    print('[GET BID FROM PLAN] ' + str(j+1) + "/" + str(len(data_api_KHLCNT_ct['lsBidpPlanDetailDTO'])) + ": [MA_TBMT] " + str(Ma_TBMT))
                    #XỬ LÝ THỜI GIAN BẮT ĐẦU TỔ CHỨC LCNT
                    giatri_quy = data_api_KHLCNT_ct['lsBidpPlanDetailDTO'][j]["bidStartQuarter"]
                    giatri_nam = data_api_KHLCNT_ct['lsBidpPlanDetailDTO'][j]["bidStartYear"]
                    if data_api_KHLCNT_ct['lsBidpPlanDetailDTO'][j]["bidStartUnit"] == "Q":
                        quy_to_month = {"I": 3, "II": 6, "III": 9, "IV": 12}
                        giatri_thang = quy_to_month.get(giatri_quy)
                        giatri_batdau_LCNT_ct = datetime(giatri_nam, giatri_thang, calendar.monthrange(giatri_nam, giatri_thang)[1]).strftime("%d/%m/%Y")
                        TGIAN_BATDAU_TOCHUC_LCNT = f"Quý {giatri_quy}, {giatri_nam}"
                        TGIAN_BATDAU_TOCHUC_LCNT_CHITIET = giatri_batdau_LCNT_ct
                    else:
                        giatri_thang = int(data_api_KHLCNT_ct['lsBidpPlanDetailDTO'][j]["bidStartMonth"])
                        TGIAN_BATDAU_TOCHUC_LCNT = f"Tháng {giatri_thang}, {giatri_nam}"
                        TGIAN_BATDAU_TOCHUC_LCNT_CHITIET = datetime(giatri_nam, giatri_thang, calendar.monthrange(giatri_nam, giatri_thang)[1]).strftime("%d/%m/%Y")
                    THANG_BAOCAO = giatri_thang
                    NAM_BAOCAO = giatri_nam
                    numBidderJoin = ""
                    bidRealityOpenDate = ""
                    STATUS_BID = ""
                    payload_search_TBMT = json.dumps([
                                            {
                                                "pageSize": 10,
                                                "pageNumber": "0",
                                                "query": [
                                                {
                                                    "index": "es-contractor-selection",
                                                    "keyWord": Ma_TBMT,
                                                    "matchType": "all-1",
                                                    "matchFields": [
                                                    "notifyNo",
                                                    "bidName"
                                                    ],
                                                    "filters": [
                                                    {
                                                        "fieldName": "type",
                                                        "searchType": "in",
                                                        "fieldValues": [
                                                        "es-notify-contractor"
                                                        ]
                                                    },
                                                    {
                                                        "fieldName": "caseKHKQ",
                                                        "searchType": "not_in",
                                                        "fieldValues": [
                                                        "1"
                                                        ]
                                                    }
                                                    ]
                                                }
                                                ]
                                            }
                                            ])
                    data_api_search_TBMT = fetch_data_from_api(url=f"{API_BASE_URL}/smart/search",payload=payload_search_TBMT,headers=API_HEADER)
                    print('[CHECK STATUS BID] MA_TBMT: ' + str(Ma_TBMT) + "; ID_TBMT: " + str(ID_TBMT))
                    STATUS_BID = data_api_search_TBMT['page']['content'][0]["status"]
                    STATUS_TBMT = MAPPING_STATUS_TBMT[data_api_search_TBMT['page']['content'][0]["statusForNotify"]]
                    if "numBidderJoin" in data_api_search_TBMT['page']['content'][0]:
                        numBidderJoin = data_api_search_TBMT['page']['content'][0]["numBidderJoin"]
                    if "bidRealityOpenDate" in data_api_search_TBMT['page']['content'][0]:
                        bidRealityOpenDate = data_api_search_TBMT['page']['content'][0]["bidRealityOpenDate"]
                
                    data_GoiThau = {
                                        'MaDinhDanh': [str(data_api_KHLCNT_ct['bidpPlanDetailToProjectList'][0]['createdBy']).split("-")[0]],
                                        'TenDonVi': [MAPPING_TENDONVI[str(data_api_KHLCNT_ct['bidpPlanDetailToProjectList'][0]['createdBy']).split("-")[0]]],
                                        'ID_GOITHAU': [data_api_KHLCNT_ct['lsBidpPlanDetailDTO'][j]["id"]],
                                        'planNo': [data_api_KHLCNT_ct['lsBidpPlanDetailDTO'][j]["planNo"]],
                                        'TEN_GOITHAU': [data_api_KHLCNT_ct['lsBidpPlanDetailDTO'][j]["bidName"]],
                                        'TRONGNUOC_QUOCTE': [MAPPING_TRONGNUOC_QUOCTE[data_api_KHLCNT_ct['lsBidpPlanDetailDTO'][j]["isDomestic"]]],
                                        'HINHTHUC_DUTHAU': [MAPPING_HINHTHUC_DUTHAU[data_api_KHLCNT_ct['lsBidpPlanDetailDTO'][j]["isInternet"]]],
                                        'LOAI_HOPDONG': [MAPPING_LOAI_HD[data_api_KHLCNT_ct['lsBidpPlanDetailDTO'][j]["ctype"]]],
                                        'PHUONGTHUC_LCNT': [MAPPING_PHUONGTHUC_LCNT[data_api_KHLCNT_ct['lsBidpPlanDetailDTO'][j]["bidMode"]]],
                                        'HINHTHUC_LCNT': [MAPPING_HINHTHUC_LCNT[data_api_KHLCNT_ct['lsBidpPlanDetailDTO'][j]["bidForm"]]],
                                        'LINH_VUC': [MAPPING_LINHVUC[data_api_KHLCNT_ct['lsBidpPlanDetailDTO'][j]["bidField"]]],
                                        'GIA_GOITHAU': [data_api_KHLCNT_ct['lsBidpPlanDetailDTO'][j]["bidPrice"]],
                                        'CCY_GIA_GOITHAU': [data_api_KHLCNT_ct['lsBidpPlanDetailDTO'][j]["bidPriceUnit"]],
                                        'PHAN_LO': [MAPPING_PHANLO[data_api_KHLCNT_ct['lsBidpPlanDetailDTO'][j]["isMultiLot"]]],
                                        'NGUON_VON': [data_api_KHLCNT_ct['lsBidpPlanDetailDTO'][j]["capitalDetail"]],
                                        'THOIGIAN_THUCHIEN_HOPDONG': [str(data_api_KHLCNT_ct['lsBidpPlanDetailDTO'][j]["cperiod"]) + " " + str(MAPPING_VIETTAT_THOIGIAN[data_api_KHLCNT_ct['lsBidpPlanDetailDTO'][j]["cperiodUnit"]])],
                                        'NAM_BAOCAO': [NAM_BAOCAO],
                                        'THANG_BAOCAO': [THANG_BAOCAO],
                                        'TGIAN_BATDAU_TOCHUC_LCNT': [TGIAN_BATDAU_TOCHUC_LCNT],
                                        'TGIAN_BATDAU_TOCHUC_LCNT_CHITIET': [TGIAN_BATDAU_TOCHUC_LCNT_CHITIET],
                                        'MA_TBMT': [Ma_TBMT],
                                        'ID_TBMT': [ID_TBMT],
                                        'STATUS_TBMT': [STATUS_TBMT],
                                        'STATUS_BID': [STATUS_BID],
                                        'linkNotifyInfo': [data_api_KHLCNT_ct['lsBidpPlanDetailDTO'][j]["linkNotifyInfo"]],
                                        "numBidderJoin": [numBidderJoin],
                                        "bidRealityOpenDate": [bidRealityOpenDate]
                                    }
                    df_GOITHAU = pd.concat([df_GOITHAU, pd.DataFrame(data_GoiThau)], ignore_index=True)
        return df_GOITHAU

def check_status_TBMT_if_changed_get_info_GOITHAU(status_search):
    # Use more descriptive function and variable names
    khlcnt_data = get_values_from_2_columns_with_condition(token_QLDT_GoogleSheet, spreadsheetID_QLDT, "2.1.GOI_THAU", 4, 21, 24, status_search)
    df_goithau = pd.DataFrame()  # Initialize an empty DataFrame to collect results
    for row in khlcnt_data:
        # Define the payload as a separate dictionary for better readability
        payload_search_tbmt = {
            "pageSize": 10,
            "pageNumber": "0",
            "query": [
                {
                    "index": "es-contractor-selection",
                    "keyWord": row[1],
                    "matchType": "all-1",
                    "matchFields": ["notifyNo", "bidName"],
                    "filters": [
                        {
                            "fieldName": "type",
                            "searchType": "in",
                            "fieldValues": ["es-notify-contractor"]
                        },
                        {
                            "fieldName": "caseKHKQ",
                            "searchType": "not_in",
                            "fieldValues": ["1"]
                        }
                    ]
                }
            ]
        }
        # Fetch data from the API
        data_api_search_tbmt = fetch_data_from_api(f"{API_BASE_URL}/smart/search", json.dumps([payload_search_tbmt]), API_HEADER)
        if data_api_search_tbmt['page']['content'][0]["status"] != status_search:
            # Get info for GoiThau by MaTBMT from the external source
            print(str(row[1]) + " thay đổi trạng thái")
            df_new_goithau = get_info_GoiThau_by_MaTBMT_from_id_KHLCNT(row[0], row[1])
            if not df_new_goithau.empty:
                # Concatenate the new data to the existing DataFrame
                df_goithau = pd.concat([df_goithau, df_new_goithau], ignore_index=True)
    return df_goithau

def GET_INFO_BIENBAN_MOTHAU(Ma_TBMT, ID_TBMT, data):
    df_mothau_new = pd.DataFrame(data['bidSubmissionByContractorViewResponse']['bidSubmissionDTOList'])
    df_mothau_new['Ma_TBMT'] = Ma_TBMT
    df_mothau_new['ID_TBMT'] = ID_TBMT
    df_mothau_new['contractorName_final'] = df_mothau_new.apply(lambda row: row['contractorName'] if pd.isnull(row['ventureName']) else row['ventureName'], axis=1)
    df_mothau_new['contractorCode_final'] = df_mothau_new.apply(lambda row: row['contractorCode'] if pd.isnull(row['ventureCode']) else row['ventureCode'], axis=1)
    # Define a function to format the contract period
    def format_contract_period(row):
        if row['contractPeriodDTUnit'] == 'D':
            return f"{row['contractPeriodDT']} ngày"
        elif row['contractPeriodDTUnit'] == 'M':
            return f"{row['contractPeriodDT']} tháng"
        else:
            return None  # Handle other units if needed

    # Apply the function to create the 'NewContractPeriod' column
    df_mothau_new['NewContractPeriod'] = df_mothau_new.apply(format_contract_period, axis=1)
    df_mothau_new = df_mothau_new[[
                                'id', 
                                'Ma_TBMT',
                                'ID_TBMT',
                                'contractorCode_final',
                                'contractorName_final',
                                'bidPrice',
                                'bidPriceUnit',
                                'saleNumber',
                                'bidFinalPrice',
                                'bidGuarantee',
                                'bidGuaranteeValidity',
                                'NewContractPeriod',
                                'createdDateBidOpen'
                                ]]
    return df_mothau_new

def GET_INFO_HSDXKT(ma_dinh_danh, ten_don_vi, tech_req_id):
    try:
        # Fetch data from the API
        response = fetch_data_from_api(url=f"{API_BASE_URL}/ldtdsnt/tech-req-approval/get-by-id",
                                       payload=json.dumps({"id": tech_req_id}),
                                       headers=API_HEADER)
        response_data = response.get('bidrTechReqApprovalDTO', {}).get('bidReqApprovalDetail', {})
        # Extract relevant information
        hsdxkt_so_qd = response_data.get('decisionNo', '')
        hsdxkt_ngay_qd = response_data.get('approvalDate', '').split('.')[0]  # Assuming approvalDate is in ISO format
        hsdxkt_noibanhhanh_qd = response_data.get('decisionAgencyName', '')
        hsdxkt_file_id = response_data.get('decisionFileId', '')
        hsdxkt_file_name = response_data.get('decisionFileName', '')
        df_dxkt_data = []
        if response.get("lsBideLotEvalResultViewDTO"):
            for result_view in response["lsBideLotEvalResultViewDTO"]:
                dxkt_data = {
                    'MaDinhDanh': ma_dinh_danh,
                    'TenDonVi': ten_don_vi,
                    'MA_TBMT': result_view.get('notifyNo', ''),
                    'ID_TBMT': result_view.get('notifyId', ''),
                    'MADINHDANH': result_view.get('contractorCode', ''),
                    'TEN_NHATHAU': result_view.get('contractorName', ''),
                    'MA_LIENDANH': result_view.get('ventureCode', ''),
                    'TEN_LIENDANH': result_view.get('ventureName', ''),
                    'LOAI_DANHGIA': MAPPING_EVALTYPE_HSDXKT.get(result_view.get('evalType', ''), ''),
                    'KETQUA_DANHGIA': MAPPING_RESULT_HSDXKT.get(result_view.get('result', ''), ''),
                    'TECH_SCORE': result_view.get('techScore', '')
                }
                df_dxkt_data.append(dxkt_data)
        # Create the DataFrame
        df_dxkt = pd.DataFrame(df_dxkt_data)
        return df_dxkt, hsdxkt_so_qd, hsdxkt_ngay_qd, hsdxkt_noibanhhanh_qd, hsdxkt_file_id, hsdxkt_file_name
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return pd.DataFrame(), '', '', '', '', ''

def format_contract_period(row):
    if row['contractPeriodDTUnit'] == 'D':
        return f"{row['contractPeriodDT']} ngày"
    elif row['contractPeriodDTUnit'] == 'M':
        return f"{row['contractPeriodDT']} tháng"
    else:
        return None  # Handle other units if needed

def GET_INFO_HSDXTC(Ma_TBMT, ID_TBMT):
    #try:
        print(Ma_TBMT)
        print(ID_TBMT)
        data_DXTC = fetch_data_from_api(url=f"{API_BASE_URL}/expose/ldtkqmt/bid-notification-p/bid-open", payload=json.dumps({"notifyNo": Ma_TBMT,"type": "TBMT","packType": 2,"viewType": 0,"notifyId": ID_TBMT}), headers=API_HEADER)
        THOIDIEM_HOANTHANH_MO_HSDXTC = ""
        df_DXTC = pd.DataFrame()
        
        if data_DXTC and 'bidSubmissionByContractorViewResponse' in data_DXTC:
            bid_submission_list = data_DXTC["bidSubmissionByContractorViewResponse"]["bidSubmissionDTOList"]
            if bid_submission_list:
                THOIDIEM_HOANTHANH_MO_HSDXTC = datetime.strptime(bid_submission_list[0]["createdDateBidOpen"].split(".")[0], "%Y-%m-%dT%H:%M:%S").strftime("%d/%m/%Y")
                df_DXTC_new = pd.DataFrame(bid_submission_list)
                df_DXTC_new['Ma_TBMT'] = Ma_TBMT
                df_DXTC_new['ID_TBMT'] = ID_TBMT
                # Apply a lambda function to conditionally fill ventureName
                df_DXTC_new['contractorName_final'] = df_DXTC_new.apply(lambda row: row['contractorName'] if pd.isnull(row['ventureName']) else row['ventureName'], axis=1)
                df_DXTC_new['contractorCode_final'] = df_DXTC_new.apply(lambda row: row['contractorCode'] if pd.isnull(row['ventureCode']) else row['ventureCode'], axis=1)
                # Apply the format_contract_period function to create the 'NewContractPeriod' column
                df_DXTC_new['NewContractPeriod'] = df_DXTC_new.apply(format_contract_period, axis=1)
                # Select specific columns in the desired order
                df_DXTC_new = df_DXTC_new[[
                    'id', 
                    'Ma_TBMT',
                    'ID_TBMT',
                    'contractorCode_final',
                    'contractorName_final',
                    'bidPrice',
                    'bidPriceUnit',
                    'saleNumber',
                    'bidFinalPrice',
                    'bidGuarantee',
                    'bidGuaranteeValidity',
                    'techScore',
                    'NewContractPeriod',
                ]]
                df_DXTC = pd.concat([df_DXTC, df_DXTC_new], ignore_index=True)
        return df_DXTC, THOIDIEM_HOANTHANH_MO_HSDXTC
    # except Exception as e:
    #     print(f"An error occurred: {str(e)}")
    #     return pd.DataFrame(), ''
    
def GET_INFO_KQLCNT(id_KQLCNT, Ma_TBMT, ID_TBMT, STATUS_TBMT):
    df_DanhMucHangHoa = pd.DataFrame()
    df_KQLCNT = pd.DataFrame()
    GIATRUNGTHAU = None
    NGAY_KY_HOPDONG = None
    KQLCNT_SO_QD = None
    KQLCNT_NGAY_QD = None
    KQLCNT_NOIBANHANH_QD = None
    KQLCNT_FILE_ID = None
    KQLCNT_FILE_NAME = None
    BCDG_HSDT_FILE_ID = None
    BCDG_HSDT_FILE_NAME = None
    data_KQLCNT = fetch_data_from_api(url=f"{API_BASE_URL}/expose/contractor-input-result/get",
                                    payload=json.dumps({"id": id_KQLCNT}),
                                    headers=API_HEADER)
    if data_KQLCNT["bideContractorInputResultDTO"] != []:
        print("GET THÔNG TIN KẾT QUẢ LỰA CHỌN NHÀ THẦU")
        KQLCNT_SO_QD = data_KQLCNT['bideContractorInputResultDTO']['decisionNo']
        KQLCNT_NGAY_QD = datetime.strptime(str(data_KQLCNT['bideContractorInputResultDTO']['decisionDate']).split(".")[0], "%Y-%m-%dT%H:%M:%S").strftime("%d/%m/%Y")
        KQLCNT_NOIBANHANH_QD = data_KQLCNT['bideContractorInputResultDTO']['decisionAgency']
        KQLCNT_FILE_ID = data_KQLCNT['bideContractorInputResultDTO']['decisionFileId']
        KQLCNT_FILE_NAME = data_KQLCNT['bideContractorInputResultDTO']['decisionFileName']
        BCDG_HSDT_FILE_ID = data_KQLCNT['bideContractorInputResultDTO']['reportFileId']
        BCDG_HSDT_FILE_NAME = data_KQLCNT['bideContractorInputResultDTO']['reportFileName']
        if data_KQLCNT["bideContractorInputResultDTO"]['lotResultDTO'] != []:
            for row_json in range(0,len(data_KQLCNT["bideContractorInputResultDTO"]['lotResultDTO'])):
                if data_KQLCNT["bideContractorInputResultDTO"]['lotResultDTO'][row_json]['contractorList'] != []:
                    df_KQLCNT_new = pd.DataFrame(data_KQLCNT["bideContractorInputResultDTO"]['lotResultDTO'][row_json]['contractorList'])
                    df_KQLCNT_new['Ma_TBMT'] = Ma_TBMT
                    df_KQLCNT_new['ID_TBMT'] = ID_TBMT
                    if STATUS_TBMT == "04. Có nhà thầu trúng thầu":
                        GIATRUNGTHAU = [item["bidWiningPrice"] for item in data_KQLCNT["bideContractorInputResultDTO"]['lotResultDTO'][row_json]['contractorList'] if item["bidWiningPrice"] is not None][0]
                        NGAY_KY_HOPDONG = [item["contractSignDate"] for item in data_KQLCNT["bideContractorInputResultDTO"]['lotResultDTO'][row_json]['contractorList'] if item["bidResult"] == 1]
                        if NGAY_KY_HOPDONG[0] is not None:
                            NGAY_KY_HOPDONG = datetime.strptime(NGAY_KY_HOPDONG[0], "%Y-%m-%dT%H:%M:%S").strftime("%d/%m/%Y")
                        else:
                            NGAY_KY_HOPDONG = ""

                    df_KQLCNT_new['lotName'] = data_KQLCNT["bideContractorInputResultDTO"]['lotResultDTO'][row_json]["lotName"]
                    df_KQLCNT_new['lotNo'] = data_KQLCNT["bideContractorInputResultDTO"]['lotResultDTO'][row_json]["lotNo"]
                    df_KQLCNT_new['lotPrice'] = data_KQLCNT["bideContractorInputResultDTO"]['lotResultDTO'][row_json]["lotPrice"]
                    #df_KQLCNT_new['winningCode'] = data_KQLCNT["bideContractorInputResultDTO"]['lotResultDTO'][row_json]["winningCode"]
                    df_KQLCNT_new = df_KQLCNT_new[[
                                                'id', 
                                                'Ma_TBMT',
                                                'ID_TBMT',
                                                'lotNo',
                                                'lotName',
                                                'ventureCode',
                                                'ventureName',
                                                'orgCode',
                                                'orgFullname',
                                                'bidWiningPrice',
                                                'reason',
                                                'bidResult',
                                                'role',
                                                'evalBidPrice',
                                                'lotPrice',
                                                'lotFinalPrice',
                                                'discountPercent',
                                                'techScore',
                                                'recEmail',
                                                'taxCode',
                                                'cperiodText',
                                                'contractSignDate',
                                                ]]
                    df_KQLCNT = pd.concat([df_KQLCNT, df_KQLCNT_new], ignore_index=True)

                    print("GET THÔNG TIN DANH MỤC HÀNG HOÁ")
                    if data_KQLCNT["bideContractorInputResultDTO"]['lotResultDTO'][row_json]["goodsList"] != None:
                        if data_KQLCNT["bideContractorInputResultDTO"]['lotResultDTO'][row_json]["goodsList"] != "[]":
                            for row_json in json.loads(data_KQLCNT["bideContractorInputResultDTO"]['lotResultDTO'][row_json]["goodsList"]):
                                if "formValue" in row_json:
                                    if "lotContent"  in row_json["formValue"]:
                                        if "Table"  in row_json["formValue"]["lotContent"]:
                                            # Extract the "Table" data
                                            table_data = row_json["formValue"]["lotContent"]["Table"]
                                            # Convert the data to a DataFrame
                                            df_DanhMucHangHoa_new = pd.DataFrame(table_data)
                                            # Add the additional columns
                                            df_DanhMucHangHoa_new["formCode"] = row_json["formCode"]
                                            df_DanhMucHangHoa_new["lotNo"] = row_json["lotNo"]
                                            df_DanhMucHangHoa_new["sharedFiles"] = row_json["formValue"]["lotContent"]["sharedFiles"]
                                            df_DanhMucHangHoa_new["attachFiles"] = row_json["formValue"]["lotContent"]["attachFiles"]
                                            df_DanhMucHangHoa_new["contractorCode"] = row_json["contractorCode"]
                                            df_DanhMucHangHoa_new["MA_TBMT"] = Ma_TBMT
                                            df_DanhMucHangHoa_new["ID_TBMT"] = ID_TBMT
                                            # Check if columns exist in df_DanhMucHangHoa_new but not in df_DanhMucHangHoa
                                            missing_columns = [col for col in df_DanhMucHangHoa_new.columns if col not in df_DanhMucHangHoa.columns]
                                            # Add missing columns to df_DanhMucHangHoa with NaN values
                                            for col in missing_columns:
                                                df_DanhMucHangHoa[col] = None
                                            # Check if columns in df_DanhMucHangHoa_new exist in df_DanhMucHangHoa
                                            missing_columns = [col for col in df_DanhMucHangHoa.columns if col not in df_DanhMucHangHoa_new.columns]
                                            # Add missing columns to df_DanhMucHangHoa_new with NaN values
                                            for col in missing_columns:
                                                df_DanhMucHangHoa_new[col] = None
                                            df_DanhMucHangHoa = pd.concat([df_DanhMucHangHoa, df_DanhMucHangHoa_new], ignore_index=True)
    return df_KQLCNT, df_DanhMucHangHoa, KQLCNT_SO_QD, KQLCNT_NGAY_QD, KQLCNT_NOIBANHANH_QD, KQLCNT_FILE_ID, KQLCNT_FILE_NAME, BCDG_HSDT_FILE_ID, BCDG_HSDT_FILE_NAME, GIATRUNGTHAU, NGAY_KY_HOPDONG
    
def get_info_giahan(data):
    df_giahan = pd.DataFrame()
    if data['bidNoContractorResponse'] != None:
                if data['bidNoContractorResponse']['bidNotification'] != None:
                    if 'delayDTOList' in data['bidNoContractorResponse']['bidNotification']:
                        if data['bidNoContractorResponse']['bidNotification']['delayDTOList'] != None:
                            for row_json in range(0,len(data['bidNoContractorResponse']['bidNotification']['delayDTOList'])):
                                data_giahan_new = {
                                    'MA_TBMT': [data['bidNoContractorResponse']['bidNotification']['delayDTOList'][row_json]["notifyNo"]],
                                    'ID_GIAHAN': [data['bidNoContractorResponse']['bidNotification']['delayDTOList'][row_json]["id"]],
                                    'LAN_GIAHAN': ["Lần " + str(row_json + 1)],
                                    'THOIDIEM_MOTHAU_CU': [data['bidNoContractorResponse']['bidNotification']['delayDTOList'][row_json]["bidOpenDate"]],
                                    'THOIDIEM_DONGTHAU_CU': [data['bidNoContractorResponse']['bidNotification']['delayDTOList'][row_json]["bidCloseDate"]],
                                    'THOIDIEM_MOTHAU_MOI': [data['bidNoContractorResponse']['bidNotification']['delayDTOList'][row_json]["bidOpenDelayDate"]],
                                    'THOIDIEM_DONGTHAU_MOI': [data['bidNoContractorResponse']['bidNotification']['delayDTOList'][row_json]["bidCloseDelayDate"]],
                                    'LYDO': [data['bidNoContractorResponse']['bidNotification']['delayDTOList'][row_json]["reason"]],
                                    'NGAY_GIAHAN': [data['bidNoContractorResponse']['bidNotification']['delayDTOList'][row_json]["createdDate"]],
                                    'TAIKHOAN_GIAHAN': [data['bidNoContractorResponse']['bidNotification']['delayDTOList'][row_json]["createdBy"]]
                                }
                                df_giahan = pd.concat([df_giahan, pd.DataFrame(data_giahan_new)], ignore_index=True)
    return df_giahan

# Main Function
def main():
    # Get list id of KHLCNT from Google Sheet
    print("I. KIỂM TRA KHLCNT MỚI ĐĂNG TẢI")
    list_id_khlcnt_googleSheet = get_listValue_from_col_in_GoogleSheet(token_QLDT_GoogleSheet, spreadsheetID_QLDT, "2.1.KHLCNT", 3)
    print("I.1. DOWNLOAD LIST ID CỦA KHLCNT TRONG DATABASE: " + str(len(list_id_khlcnt_googleSheet)-1) + " giá trị")
    #Lấy toàn bộ danh sách KHLCNT
    list_khlcnt= fetch_data_from_api(f"{API_BASE_URL}/smart/search", payload_ds_KHLCNT, API_HEADER)["page"]["content"]
    print("I.2. DOWNLOAD LIST ID CỦA KHLCNT TỪ TRANG MUASAMCONG: " + str(len(list_khlcnt)) + " giá trị")
    df_ct_khlcnt = pd.DataFrame()
    df_GOITHAU = pd.DataFrame()
    df_telegram = pd.DataFrame()
    
    print("I.3. MAPPING LIST KHLCNT TỪ I.1 và I.2")
    #LẤY CHI TIẾT KHCLNT VÀ LIST GÓI THẦU TỪ ID KHLCNT MỚI, CHƯA CÓ TRONG DATABASE
    if list_khlcnt:
        for row_list_khlcnt in range(len(list_khlcnt)):
            #Check ID của KHLCNT trong dach sách đã được tải về lên Google Sheet chưa?
            if list_khlcnt[row_list_khlcnt]["id"] not in list_id_khlcnt_googleSheet:
                print("TÌM THẤY KHLCNT MỚI " + str(row_list_khlcnt+1) + "/" + str(len(list_khlcnt)) + ": " + str(list_khlcnt[row_list_khlcnt]["id"]))
                data_ct_khlcnt, df_new_GOITHAU = process_get_chitiet_KHLCNT_GOITHAU(list_khlcnt[row_list_khlcnt]["id"])
                if data_ct_khlcnt:
                    df_ct_khlcnt = pd.concat([df_ct_khlcnt, pd.DataFrame(data_ct_khlcnt)], ignore_index=True)
                    print('[STEP 3- GỬI TIN NHẮN TELEGRAM] NEW_KHLCNT: ' + str(data_ct_khlcnt["planNo"][0]))
                    if df_telegram.empty:
                        telegram_count = 0
                    else:
                        telegram_count = len(df_telegram)
                    data_telegram = {
                                    'No': [telegram_count + 1],
                                    'TextMessage': ["🆕 KHLCNT MỚI 🌟" + '\n' + 
                                                    "Đơn vị: " + str(data_ct_khlcnt["TenDonVi"][0]).split(". ")[1] + '\n' +
                                                    "Mã KHLCNT: " + str(data_ct_khlcnt["planNo"][0]) + '\n' +
                                                    "Tên KHLCNT: " + str(data_ct_khlcnt["planName"][0]) + '\n' +
                                                    "Ngày đăng tải: " + str(data_ct_khlcnt["NgayDangTai_KHLCNT"][0]) + '\n' +
                                                    "Số lượng gói thầu: " + str(data_ct_khlcnt["SoLuongGoiThau"][0]) + '\n' +
                                                    "QĐ phê duyệt: " + str(data_ct_khlcnt["QD_KHLCNT_So"][0]) + " (" + str(data_ct_khlcnt["QD_KHLCNT_Ngay"][0]) + ")"
                                                    ],
                                    'Date_get_data': [datetime.now().strftime("%Y-%m-%d %H:%M:%S")]}
                    df_telegram = pd.concat([df_telegram, pd.DataFrame(data_telegram)], ignore_index=True)
                if not df_new_GOITHAU.empty:
                    df_new_GOITHAU['ACTION'] = "TBMT_MOI"
                    df_GOITHAU = pd.concat([df_GOITHAU, df_new_GOITHAU], ignore_index=True)
    
    #LẤY THÔNG TIN GÓI THẦU MỚI ĐĂNG TẢI TBMT:
    print("II. KIỂM TRA GÓI THẦU MỚI ĐĂNG TẢI TBMT")
    list_id_KHLCNT_not_yet_TBMT = get_listValue_from_col_in_GoogleSheet_with_condition(token_QLDT_GoogleSheet,spreadsheetID_QLDT,"2.1.GOI_THAU",4,23,"01. Chưa có TBMT")
    print("II.1. DOWNLOAD LIST MÃ KHLCNT CHƯA CÓ TBMT TỪ DATABASE " + str(len(list_id_KHLCNT_not_yet_TBMT)) + " giá trị")
    print("II.2. DOWNLOAD LIST MÃ TBMT ĐÃ TỒN TẠI TRONG DATABASE")
    list_maTBMT = get_listValue_from_col_in_GoogleSheet(token=token_QLDT_GoogleSheet,spreadsheet_id=spreadsheetID_QLDT,worksheet_name="2.1.GOI_THAU_CT",column_number=4)
    print("II.3. CHECK TBMT MỚI ĐĂNG TẢI TỪ TRANG MUASAMCONG")
    row_count = 0
    for id_khlcnt in list_id_KHLCNT_not_yet_TBMT:
        row_count = row_count + 1
        print("[" + str(row_count) + "/" + str(len(list_id_KHLCNT_not_yet_TBMT)) + "] - ID_KHLCNT: " + id_khlcnt)
        df_new_GOITHAU = check_new_TBMT(id_khlcnt,list_maTBMT)
        if not df_new_GOITHAU.empty:
            df_new_GOITHAU['ACTION'] = "TBMT_MOI"
            df_GOITHAU = pd.concat([df_GOITHAU, df_new_GOITHAU], ignore_index=True)

    #CHECK STATUS GOITHAU CÓ THAY ĐỔI HAY KHÔNG?
    print("III. KIỂM TRA CÓ TBMT NÀO MỚI CHUYỂN TRẠNG THÁI KHÔNG?")
    #LẤY THÔNG TIN CHƯA MỞ THẦU
    print("III.1. KIỂM TRA STATUS 01")
    df_new_GOITHAU = check_status_TBMT_if_changed_get_info_GOITHAU("01")
    if not df_new_GOITHAU.empty:
        df_new_GOITHAU['ACTION'] = "CHUYEN_STATUS_TU_01"
        df_GOITHAU = pd.concat([df_GOITHAU, df_new_GOITHAU], ignore_index=True)

    #LẤY THÔNG TIN OPEN_BID
    print("III.2. KIỂM TRA STATUS OPEN_BID")
    df_new_GOITHAU = check_status_TBMT_if_changed_get_info_GOITHAU("OPEN_BID")
    if not df_new_GOITHAU.empty:
        df_new_GOITHAU['ACTION'] = "CHUYEN_STATUS_TU_OPEN_BID"
        df_GOITHAU = pd.concat([df_GOITHAU, df_new_GOITHAU], ignore_index=True)

    #LẤY THÔNG TIN OPEN_DXKT
    print("III.3. KIỂM TRA STATUS OPEN_DXKT")
    df_new_GOITHAU = check_status_TBMT_if_changed_get_info_GOITHAU("OPEN_DXKT")
    if not df_new_GOITHAU.empty:
        df_new_GOITHAU['ACTION'] = "CHUYEN_STATUS_TU_OPEN_DXKT"
        df_GOITHAU = pd.concat([df_GOITHAU, df_new_GOITHAU], ignore_index=True)

    #LẤY THÔNG TIN OPEN_DXTC
    print("III.4. KIỂM TRA STATUS OPEN_DXTC")
    df_new_GOITHAU = check_status_TBMT_if_changed_get_info_GOITHAU("OPEN_DXTC")
    if not df_new_GOITHAU.empty:
        df_new_GOITHAU['ACTION'] = "CHUYEN_STATUS_TU_OPEN_DXTC"
        df_GOITHAU = pd.concat([df_GOITHAU, df_new_GOITHAU], ignore_index=True)

    if df_GOITHAU.empty:
        print("Số lượng gói thầu mới cập nhật: 0")
    else:
        print("Số lượng gói thầu mới cập nhật: " + str(len(df_GOITHAU)))

    #LẤY CHI TIẾT THÔNG BÁO MỜI THẦU CỦA GÓI THẦU
    df_GOITHAU_ct = pd.DataFrame()
    df_mothau = pd.DataFrame()
    df_DXKT = pd.DataFrame()
    df_DXTC = pd.DataFrame()
    df_KQLCNT = pd.DataFrame()
    df_HANGHOA = pd.DataFrame()
    df_GIAHAN = pd.DataFrame()
    df_lamro = pd.DataFrame()
    df_kiennghi = pd.DataFrame()
    if not df_GOITHAU.empty:
        print("IV. DOWNLOAD THÔNG TIN CHI TIẾT GÓI THẦU TỪ TRANG MUASAMCONG")
        for i in range(0, df_GOITHAU['ID_GOITHAU'].count()):
            
            print("[IV. GET BID DETAILS]" + str(i+1) + "/" + str(df_GOITHAU['ID_GOITHAU'].count()) + ": " + str(df_GOITHAU['MA_TBMT'][i]))
            if str(df_GOITHAU['MA_TBMT'][i]) != '':
                Ma_TBMT = df_GOITHAU['MA_TBMT'][i]
                if Ma_TBMT != "IB2300151471": #KHÔNG HIỂU SAO GÓI THẦU NÀY LẠI CÓ CẤU TRÚC KHÁC VỚI TOÀN BỘ CÁC GÓI
                    payload_search_ID = json.dumps([{"pageSize": 1,"pageNumber": 0,
                                                    "query": [{"index": "es-contractor-selection","keyWord": str(Ma_TBMT),"matchType": "exact","matchFields": ["notifyNo","bidName"],
                                                    "filters": [{"fieldName": "type","searchType": "in","fieldValues": ["es-notify-contractor"]}]}]}])
                    data_search_ID = fetch_data_from_api(url=f"{API_BASE_URL}/smart/search",
                                                        payload=payload_search_ID,
                                                        headers=API_HEADER)
                    ID_TBMT = data_search_ID['page']['content'][0]['notifyId']
                    data_get_by_id = fetch_data_from_api(url=f"{API_BASE_URL}/expose/lcnt/bid-po-bido-notify-contractor-view/get-by-id",
                                                        payload=json.dumps({"id": str(ID_TBMT)}),
                                                        headers=API_HEADER)
                    THOI_DIEM_HOANTHANH_MOTHAU = ""
                    if df_GOITHAU["bidRealityOpenDate"][i] != "":
                        THOI_DIEM_HOANTHANH_MOTHAU = datetime.strptime(str(df_GOITHAU["bidRealityOpenDate"][i]).split(".")[0], "%Y-%m-%dT%H:%M:%S").strftime("%d/%m/%Y")
                        PACK_TYPE = {"1_MTHS": 0, "1_HTHS": 1}.get(data_get_by_id['bidNoContractorResponse']['bidNotification']["bidMode"])
                        #LẤY THÔNG TIN BIÊN BẢN MỞ THẦU
                        print("GET THÔNG TIN BIÊN BẢN MỜI THẦU")
                        data_mothau = fetch_data_from_api(url=f"{API_BASE_URL}/expose/ldtkqmt/bid-notification-p/bid-open",
                                                            payload=json.dumps({"notifyNo": str(Ma_TBMT),"notifyId": str(ID_TBMT),"packType": PACK_TYPE}),
                                                            headers=API_HEADER)
                        if data_mothau['bidSubmissionByContractorViewResponse'] != None:
                            if data_mothau['bidSubmissionByContractorViewResponse']['bidSubmissionDTOList'] != None:
                                df_new_mothau = GET_INFO_BIENBAN_MOTHAU(Ma_TBMT,ID_TBMT,data_mothau)
                                df_mothau = pd.concat([df_mothau, df_new_mothau], ignore_index=True)

                    #LẤY THÔNG TIN PHÊ DUYỆT HSĐXKT
                    HSDXKT_SO_QD = ""
                    HSDXKT_NGAY_QD = ""
                    HSDXKT_NOIBANHANH_QD = ""
                    HSDXKT_FILE_ID = ""
                    HSDXKT_FILE_NAME = ""
                    THOIDIEM_HOANTHANH_MO_HSDXTC = ""
                    if data_get_by_id['bidNoContractorResponse']['bidNotification']["bidMode"] == "1_HTHS":
                        if 'techReqId' in data_search_ID['page']['content'][0]: #CHECK XEM ĐÃ PHÊ DUYỆT KẾT QUẢ ĐẠT BƯỚC KỸ THUẬT HAY CHƯA
                            print("GET THÔNG TIN HỒ SƠ ĐỀ XUẤT KỸ THUẬT")
                            df_DXKT_new, HSDXKT_SO_QD, HSDXKT_NGAY_QD, HSDXKT_NOIBANHANH_QD, HSDXKT_FILE_ID, HSDXKT_FILE_NAME = GET_INFO_HSDXKT(df_GOITHAU['MaDinhDanh'][i],df_GOITHAU['TenDonVi'][i],data_search_ID['page']['content'][0]['techReqId'])
                            df_DXKT = pd.concat([df_DXKT, df_DXKT_new], ignore_index=True)

                            #LẤY THÔNG TIN HỒ SƠ ĐỀ XUẤT TÀI CHÍNH
                            print("GET THÔNG TIN HỒ SƠ ĐỀ XUẤT TÀI CHÍNH")
                            df_DXTC_new, THOIDIEM_HOANTHANH_MO_HSDXTC = GET_INFO_HSDXTC(Ma_TBMT,ID_TBMT)
                            if not df_DXTC_new.empty:
                                df_DXTC = pd.concat([df_DXTC, df_DXTC_new], ignore_index=True)

                    #LẤY THÔNG TIN KẾT QUẢ LỰA CHỌN NHÀ THẦU
                    KQLCNT_SO_QD = ""
                    KQLCNT_NGAY_QD = ""
                    KQLCNT_NOIBANHANH_QD = ""
                    KQLCNT_FILE_ID = ""
                    KQLCNT_FILE_NAME = ""
                    BCDG_HSDT_FILE_ID = ""
                    BCDG_HSDT_FILE_NAME = ""
                    GIATRUNGTHAU = ""
                    NGAY_KY_HOPDONG = ""

                    if 'inputResultId' in data_search_ID['page']['content'][0]:
                        inputResultId = data_search_ID['page']['content'][0]['inputResultId']
                        df_KQLCNT_new, df_HANGHOA_new, KQLCNT_SO_QD, KQLCNT_NGAY_QD, KQLCNT_NOIBANHANH_QD, KQLCNT_FILE_ID, KQLCNT_FILE_NAME, BCDG_HSDT_FILE_ID, BCDG_HSDT_FILE_NAME, GIATRUNGTHAU, NGAY_KY_HOPDONG = GET_INFO_KQLCNT(inputResultId, Ma_TBMT, ID_TBMT, df_GOITHAU["STATUS_TBMT"][i])
                        
                        df_KQLCNT = pd.concat([df_KQLCNT, df_KQLCNT_new], ignore_index=True)
                        df_HANGHOA = pd.concat([df_HANGHOA, df_HANGHOA_new], ignore_index=True)

                    #TÍNH TỶ LỆ TIẾT KIỆM
                    TYLE_TIETKIEM = None
                    if df_GOITHAU["STATUS_TBMT"][i] == "04. Có nhà thầu trúng thầu":
                        if data_get_by_id['bidoNotifyContractorM']['bidEstimatePrice'] is not None:
                            TYLE_TIETKIEM = round((data_get_by_id['bidoNotifyContractorM']['bidEstimatePrice'] - GIATRUNGTHAU) * 100 / data_get_by_id['bidoNotifyContractorM']['bidEstimatePrice'],0)
                        else:
                            TYLE_TIETKIEM = round((df_GOITHAU["GIA_GOITHAU"][i] - GIATRUNGTHAU) * 100 / df_GOITHAU["GIA_GOITHAU"][i],0)

                    #LẤY THÔNG TIN HUỶ THẦU
                    HUYTHAU_LOAI = ""
                    HUYTHAU_SO_QD = ""
                    HUYTHAU_NGAY_QD = ""
                    HUYTHAU_NOIBANHANH_QD = ""
                    HUYTHAU_FILE_ID = ""
                    HUYTHAU_FILE_NAME = ""
                    HUYTHAU_NGAY = ""
                    HUYTHAU_REASON = ""
                    if "bidCancelingResponse" in data_get_by_id:
                        if "cancelType" in data_get_by_id["bidCancelingResponse"]:
                            if data_get_by_id["bidCancelingResponse"]["cancelType"] != None:
                                if data_get_by_id["bidCancelingResponse"]["cancelType"] == '0':
                                    HUYTHAU_LOAI = "Huỷ TBMT"
                                    HUYTHAU_SO_QD = data_get_by_id["bidCancelingResponse"]["cancelDecisionNo"]
                                    HUYTHAU_NGAY_QD = data_get_by_id["bidCancelingResponse"]["cancelDecisionDate"]
                                    HUYTHAU_NOIBANHANH_QD = data_get_by_id["bidCancelingResponse"]["cancelDecisionAgency"]
                                    HUYTHAU_FILE_ID = data_get_by_id["bidCancelingResponse"]["cancelFileAttachId"]
                                    HUYTHAU_FILE_NAME = data_get_by_id["bidCancelingResponse"]["cancelFileAttachName"]
                                    HUYTHAU_NGAY = data_get_by_id["bidCancelingResponse"]["cancelDate"]
                                    HUYTHAU_REASON = data_get_by_id["bidCancelingResponse"]["cancelReason"]
                                else:
                                    HUYTHAU_LOAI = "Huỷ thầu"
                                    HUYTHAU_SO_QD = data_get_by_id["bidCancelingResponse"]["cancelDecisionNo"]
                                    HUYTHAU_NGAY_QD = data_get_by_id["bidCancelingResponse"]["cancelDecisionDate"]
                                    HUYTHAU_NOIBANHANH_QD = data_get_by_id["bidCancelingResponse"]["cancelDecisionAgency"]
                                    HUYTHAU_FILE_ID = data_get_by_id["bidCancelingResponse"]["cancelFileAttachId"]
                                    HUYTHAU_FILE_NAME = data_get_by_id["bidCancelingResponse"]["cancelFileAttachName"]
                                    HUYTHAU_NGAY = data_get_by_id["bidCancelingResponse"]["cancelDate"]
                                    HUYTHAU_REASON = data_get_by_id["bidCancelingResponse"]["cancelReason"]

                    #LẤY THÔNG TIN CHIA LÔ/ CHIA PHẦN
                    THONGTIN_CHIALO = ""
                    if 'lotDTOList' in data_get_by_id['bidNoContractorResponse']['bidNotification']:
                        print(df_GOITHAU["PHAN_LO"][i])
                        if df_GOITHAU["PHAN_LO"][i] == "Có chia phần/ lô":
                            for row_PhanLo in range(len(data_get_by_id['bidNoContractorResponse']['bidNotification']['lotDTOList'])):
                                THONGTIN_CHIALO = THONGTIN_CHIALO + '\n' + '- ' + str(data_get_by_id['bidNoContractorResponse']['bidNotification']['lotDTOList'][row_PhanLo]["lotName"]) + ": " + str(data_get_by_id['bidNoContractorResponse']['bidNotification']['lotDTOList'][row_PhanLo]["lotPrice"]) 
                    #LẤY THÔNG TIN GIA HẠN
                    df_GIAHAN = pd.concat([df_GIAHAN, get_info_giahan(data_get_by_id)], ignore_index=True)
                    
                    #PREPARE DATA TO SEND TELEGRAM
                    if df_GOITHAU['ACTION'][i] == "TBMT_MOI":
                        if df_telegram.empty:
                            telegram_count = 0
                        else:
                            telegram_count = len(df_telegram)
                        data_telegram = {
                                        'No': [telegram_count + 1],
                                        'TextMessage': ["🆕 TBMT MỚI 🌟" + '\n' + 
                                                        "Đơn vị: " + str(df_GOITHAU['TenDonVi'][i]).split(". ")[1] + '\n' +
                                                        "Mã TBMT: " + str(df_GOITHAU["MA_TBMT"][i]) + '\n' +
                                                        "Tên gói thầu: " + str(df_GOITHAU["TEN_GOITHAU"][i]) + '\n' +
                                                        "Ngày đăng tải: " + str(datetime.strptime(str(data_get_by_id['bidNoContractorResponse']['bidNotification']['publicDate']).split(".")[0], "%Y-%m-%dT%H:%M:%S").strftime("%d/%m/%Y %H:%M:%S")) + '\n' +
                                                        "Giá gói thầu: " + f"{df_GOITHAU['GIA_GOITHAU'][i]:,}" + ' VNĐ' + '\n' +
                                                        "Thời điểm mở thầu: " + str(data_get_by_id['bidNoContractorResponse']['bidNotification']['bidOpenDate']) + '\n' +
                                                        "Thời điểm mở thầu: " + str(data_get_by_id['bidNoContractorResponse']['bidNotification']['bidCloseDate']) + '\n' +
                                                        "QĐ phê duyệt HSMT: " + str(data_get_by_id['bidInvContractorOfflineDTO']['decisionNo']) + " (" + str(datetime.strptime(str(data_get_by_id['bidInvContractorOfflineDTO']['decisionDate']).split(".")[0], "%Y-%m-%dT%H:%M:%S").strftime("%d/%m/%Y")) + ")"
                                                        ],
                                        'Date_get_data': [datetime.now().strftime("%Y-%m-%d %H:%M:%S")]}
                        df_telegram = pd.concat([df_telegram, pd.DataFrame(data_telegram)], ignore_index=True)
                    elif df_GOITHAU['ACTION'][i] == "CHUYEN_STATUS_TU_01":
                        if df_GOITHAU["STATUS_BID"][i] == "03":
                            if df_GOITHAU["STATUS_TBMT"][i] == "06. Đã huỷ thầu" or df_GOITHAU["STATUS_TBMT"][i] == "07. Đã huỷ Thông báo mời thầu":
                                data_telegram = {
                                                'No': [telegram_count + 1],
                                                'TextMessage': ["🌟 ĐÃ HUỶ THẦU 🌟" + '\n' + 
                                                                "Đơn vị: " + str(df_GOITHAU['TenDonVi'][i]).split(". ")[1] + '\n' +
                                                                "Mã TBMT: " + str(df_GOITHAU["MA_TBMT"][i]) + '\n' +
                                                                "Phân loại huỷ thầu: " + str(HUYTHAU_LOAI) + '\n' +
                                                                "Ngày huỷ thầu: " + str(HUYTHAU_NGAY) + '\n' +
                                                                "Lý do huỷ thầu: " + str(HUYTHAU_REASON) + '\n' +
                                                                "QĐ phê duyệt: " + str(HUYTHAU_SO_QD) + " (" + str(HUYTHAU_NGAY_QD) + ")"
                                                                ],
                                                'Date_get_data': [datetime.now().strftime("%Y-%m-%d %H:%M:%S")]}
                                df_telegram = pd.concat([df_telegram, pd.DataFrame(data_telegram)], ignore_index=True)
                        else:
                            THONGTIN_NHATHAU = ""
                            for row_mothau in range(len(df_new_mothau)):
                                THONGTIN_NHATHAU = THONGTIN_NHATHAU + '\n' + str(row_mothau + 1) + ". " + df_new_mothau["contractorName_final"][row_mothau]
                            if df_telegram.empty:
                                telegram_count = 0
                            else:
                                telegram_count = len(df_telegram)
                            data_telegram = {
                                            'No': [telegram_count + 1],
                                            'TextMessage': ["🌟 GÓI THẦU ĐÃ HOÀN THÀNH MỞ THẦU 🌟" + '\n' + 
                                                            "Đơn vị: " + str(df_GOITHAU['TenDonVi'][i]).split(". ")[1] + '\n' +
                                                            "Mã TBMT: " + str(df_GOITHAU["MA_TBMT"][i]) + '\n' +
                                                            "Tên gói thầu: " + str(df_GOITHAU["TEN_GOITHAU"][i]) + '\n' +
                                                            "Số lượng nhà thầu tham dự: " + str(df_GOITHAU["numBidderJoin"][i]) + '\n' +
                                                            "Thời điểm hoàn thành mở thầu: " + str(THOI_DIEM_HOANTHANH_MOTHAU) + '\n' +
                                                            "Nhà thầu tham gia: " + THONGTIN_NHATHAU
                                                            ],
                                            'Date_get_data': [datetime.now().strftime("%Y-%m-%d %H:%M:%S")]}
                            df_telegram = pd.concat([df_telegram, pd.DataFrame(data_telegram)], ignore_index=True)
                    elif df_GOITHAU['ACTION'][i] == "CHUYEN_STATUS_TU_OPEN_DXKT":
                        THONGTIN_NHATHAU = ""
                        row_count = 0
                        for row_mothau in range(len(df_DXKT_new)):
                            if df_DXKT_new["TEN_NHATHAU"][row_mothau] not in THONGTIN_NHATHAU:
                                row_count = row_count + 1
                                THONGTIN_NHATHAU = THONGTIN_NHATHAU + '\n' + str(row_count) + ". " + str(df_DXKT_new["TEN_NHATHAU"][row_mothau]) + ": " + str(df_DXKT_new["KETQUA_DANHGIA"][row_mothau]) + " (Tech score: " + str(df_DXKT_new["TECH_SCORE"][row_mothau]) + ")"
                        if df_telegram.empty:
                            telegram_count = 0
                        else:
                            telegram_count = len(df_telegram)
                        data_telegram = {
                                        'No': [telegram_count + 1],
                                        'TextMessage': ["🌟 GÓI THẦU ĐÃ HOÀN THÀNH ĐÁNH GIÁ HSĐXKT 🌟" + '\n' + 
                                                        "Đơn vị: " + str(df_GOITHAU['TenDonVi'][i]).split(". ")[1] + '\n' +
                                                        "Mã TBMT: " + str(df_GOITHAU["MA_TBMT"][i]) + '\n' +
                                                        "Giá gói thầu: " + f"{df_GOITHAU['GIA_GOITHAU'][i]:,}" + ' VNĐ' + '\n' +
                                                        "Kết quả ĐXKT: " + THONGTIN_NHATHAU
                                                        ],
                                        'Date_get_data': [datetime.now().strftime("%Y-%m-%d %H:%M:%S")]}
                        df_telegram = pd.concat([df_telegram, pd.DataFrame(data_telegram)], ignore_index=True)
                    else:
                        if df_GOITHAU["STATUS_BID"][i] == "IS_PUBLISH":
                            THONGTIN_NHATHAU = ""
                            row_count = 0
                            for row_mothau in range(len(df_KQLCNT_new)):
                                if df_KQLCNT_new["bidResult"][row_mothau] == 1:
                                    if df_KQLCNT_new["ventureName"][row_mothau] is not None:
                                        if df_KQLCNT_new["ventureName"][row_mothau] not in THONGTIN_NHATHAU:
                                            THONGTIN_NHATHAU = THONGTIN_NHATHAU + '\n' + str(row_count) + ". " + str(df_KQLCNT_new["ventureName"][row_mothau])
                                    else:
                                        if df_KQLCNT_new["orgFullname"][row_mothau] not in THONGTIN_NHATHAU:
                                            THONGTIN_NHATHAU = THONGTIN_NHATHAU + '\n' + str(row_count) + ". " + str(df_KQLCNT_new["orgFullname"][row_mothau])
                            if df_telegram.empty:
                                telegram_count = 0
                            else:
                                telegram_count = len(df_telegram)
                            data_telegram = {
                                            'No': [telegram_count + 1],
                                            'TextMessage': ["🌟 CÓ NHÀ THẦU TRÚNG THẦU 🌟" + '\n' + 
                                                            "Đơn vị: " + str(df_GOITHAU['TenDonVi'][i]).split(". ")[1] + '\n' +
                                                            "Mã TBMT: " + str(df_GOITHAU["MA_TBMT"][i]) + '\n' +
                                                            "Giá gói thầu: " + f"{df_GOITHAU['GIA_GOITHAU'][i]:,}" + ' VNĐ' + '\n' +
                                                            "Giá trúng thầu: " f"{GIATRUNGTHAU:,}" + ' VNĐ' + '\n' +
                                                            "Kết quả ĐXKT: " + THONGTIN_NHATHAU
                                                            ],
                                            'Date_get_data': [datetime.now().strftime("%Y-%m-%d %H:%M:%S")]}
                            df_telegram = pd.concat([df_telegram, pd.DataFrame(data_telegram)], ignore_index=True)
                        else:
                            if df_telegram.empty:
                                telegram_count = 0
                            else:
                                telegram_count = len(df_telegram)
                            data_telegram = {
                                            'No': [telegram_count + 1],
                                            'TextMessage': ["🌟 CHUYỂN TRẠNG THÁI GÓI THẦU 🌟" + '\n' + 
                                                            "Đơn vị: " + str(df_GOITHAU['TenDonVi'][i]).split(". ")[1] + '\n' +
                                                            "Mã TBMT: " + str(df_GOITHAU["MA_TBMT"][i]) + '\n' +
                                                            "Tình trạng gói thầu chuyển từ [OPEN_BID] thành [" + str(df_GOITHAU["STATUS_BID"][i]) + "]"
                                                            ],
                                            'Date_get_data': [datetime.now().strftime("%Y-%m-%d %H:%M:%S")]}
                            df_telegram = pd.concat([df_telegram, pd.DataFrame(data_telegram)], ignore_index=True)
                    
                    #LẤY THÔNG TIN LÀM RÕ
                    if data_get_by_id["biduClarifyReqInvAndContentViewList"] != []:
                        for row_json in range(0,len(data_get_by_id["biduClarifyReqInvAndContentViewList"])):
                            df_lamro_new = pd.DataFrame(json.loads(data_get_by_id["biduClarifyReqInvAndContentViewList"][row_json]["clarifyResContent"]))
                            df_lamro_new['ID_LAMRO'] = data_get_by_id["biduClarifyReqInvAndContentViewList"][row_json]["id"]
                            df_lamro_new['Ma_TBMT'] = data_get_by_id["biduClarifyReqInvAndContentViewList"][row_json]["notyfyNo"]
                            df_lamro_new['ID_TBMT'] = data_get_by_id["biduClarifyReqInvAndContentViewList"][row_json]["notyfyId"]
                            df_lamro_new['YEUCAU_LAMRO_TEN'] = data_get_by_id["biduClarifyReqInvAndContentViewList"][row_json]["reqName"]
                            df_lamro_new['YEUCAU_LAMRO_NGAY'] = data_get_by_id["biduClarifyReqInvAndContentViewList"][row_json]["reqDate"]
                            df_lamro_new['YEUCAU_LAMRO_NGAY_KY'] = data_get_by_id["biduClarifyReqInvAndContentViewList"][row_json]["signReqDate"]
                            df_lamro_new['YEUCAU_LAMRO_FILE_ID'] = data_get_by_id["biduClarifyReqInvAndContentViewList"][row_json]["clarify_file_id"]
                            df_lamro_new['YEUCAU_LAMRO_FILE_NAME'] = data_get_by_id["biduClarifyReqInvAndContentViewList"][row_json]["clarify_file_name"]
                            df_lamro_new['TRALOI_LAMRO_NGAY'] = data_get_by_id["biduClarifyReqInvAndContentViewList"][row_json]["signResDate"]
                            df_lamro_new['TRALOI_LAMRO_FILE_ID'] = data_get_by_id["biduClarifyReqInvAndContentViewList"][row_json]["clarifyResFileId"]
                            df_lamro_new['TRALOI_LAMRO_FILE_NAME'] = data_get_by_id["biduClarifyReqInvAndContentViewList"][row_json]["clarifyResFileName"]
                            df_lamro = pd.concat([df_lamro, df_lamro_new], ignore_index=True)

                    #LẤY THÔNG TIN KIẾN NGHỊ
                    if data_get_by_id["biduPetitionContractorDTOList"] != []:
                        for row_json in range(0,len(data_get_by_id["biduPetitionContractorDTOList"])):
                            df_kiennghi_new = pd.DataFrame(json.loads(data_get_by_id["biduPetitionContractorDTOList"][row_json]["content"]))
                            df_kiennghi_new['ID'] = len(df_kiennghi) + 1
                            df_kiennghi_new['ID_KIENNGHI'] = data_get_by_id["biduPetitionContractorDTOList"][row_json]["id"]
                            df_kiennghi_new['MA_KIENNGHI'] = data_get_by_id["biduPetitionContractorDTOList"][row_json]["reqNo"]
                            df_kiennghi_new['TEN_KIENNGHI'] = data_get_by_id["biduPetitionContractorDTOList"][row_json]["reqName"]
                            df_kiennghi_new['NGAY_KIENNGHI'] = data_get_by_id["biduPetitionContractorDTOList"][row_json]["reqDate"]
                            df_kiennghi_new['MA_DINHDANH_NHATHAU_KIENNGHI'] = data_get_by_id["biduPetitionContractorDTOList"][row_json]["contractorCode"]
                            df_kiennghi_new['NHATHAU_KIENNGHI'] = data_get_by_id["biduPetitionContractorDTOList"][row_json]["contractorName"]
                            df_kiennghi_new['NGUOI_XULY'] = data_get_by_id["biduPetitionContractorDTOList"][row_json]["processUserInfo"]
                            df_kiennghi_new['STATUS'] = data_get_by_id["biduPetitionContractorDTOList"][row_json]["status"]
                            df_kiennghi_new['MA_TBMT'] = Ma_TBMT
                            df_kiennghi_new['ID_TBMT'] = ID_TBMT
                            df_kiennghi = pd.concat([df_kiennghi, df_kiennghi_new], ignore_index=True)
                    
                    #ADD NEW DATA
                    data_newRow = {'MaDinhDanh': [df_GOITHAU['MaDinhDanh'][i]],
                        'TenDonVi': [df_GOITHAU['TenDonVi'][i]],
                        'ID_GOITHAU': [df_GOITHAU["ID_GOITHAU"][i]],
                        'MA_TBMT': [df_GOITHAU["MA_TBMT"][i]],
                        'ID_TBMT': [df_GOITHAU["ID_TBMT"][i]],
                        'planNo': [df_GOITHAU["planNo"][i]],
                        'TEN_GOITHAU': [df_GOITHAU["TEN_GOITHAU"][i]],
                        'LINH_VUC': [df_GOITHAU["LINH_VUC"][i]],
                        'GIA_GOITHAU': [df_GOITHAU["GIA_GOITHAU"][i]],
                        'CCY_GIA_GOITHAU': [df_GOITHAU["CCY_GIA_GOITHAU"][i]],
                        'DUTOAN_GOITHAU': [data_get_by_id['bidoNotifyContractorM']['bidEstimatePrice']],
                        'NGUON_VON': [df_GOITHAU["NGUON_VON"][i]],
                        'HINHTHUC_LCNT': [df_GOITHAU["HINHTHUC_LCNT"][i]],
                        'PHUONGTHUC_LCNT': [df_GOITHAU["PHUONGTHUC_LCNT"][i]],
                        'LOAI_HOPDONG': [df_GOITHAU["LOAI_HOPDONG"][i]],
                        'TRONGNUOC_QUOCTE': [df_GOITHAU["TRONGNUOC_QUOCTE"][i]],
                        'HINHTHUC_DUTHAU': [df_GOITHAU["HINHTHUC_DUTHAU"][i]],
                        'PHAN_LO': [df_GOITHAU["PHAN_LO"][i]],               
                        'NAM_BAOCAO': [df_GOITHAU["NAM_BAOCAO"][i]],
                        'THANG_BAOCAO': [df_GOITHAU["THANG_BAOCAO"][i]],
                        'TGIAN_BATDAU_TOCHUC_LCNT': [df_GOITHAU["TGIAN_BATDAU_TOCHUC_LCNT"][i]],
                        'TGIAN_BATDAU_TOCHUC_LCNT_CHITIET': [df_GOITHAU["TGIAN_BATDAU_TOCHUC_LCNT_CHITIET"][i]],
                        'THOIGIAN_THUCHIEN_HOPDONG': [df_GOITHAU["THOIGIAN_THUCHIEN_HOPDONG"][i]],
                        'STATUS_TBMT': [df_GOITHAU["STATUS_TBMT"][i]],
                        'STATUS_BID': [df_GOITHAU["STATUS_BID"][i]],
                        'NGAYDANGTAI_TBMT': [datetime.strptime(str(data_get_by_id['bidNoContractorResponse']['bidNotification']['publicDate']).split(".")[0], "%Y-%m-%dT%H:%M:%S").strftime("%d/%m/%Y")], 
                        'PHIENBAN_THAYDOI': [data_get_by_id['bidNoContractorResponse']['bidNotification']['notifyVersion']],
                        'DIADIEM_PHATHANH_HSMT': [data_get_by_id['bidNoContractorResponse']['bidNotification']['issueLocation']], 
                        'DIADIEM_NHAN_HSMT': [data_get_by_id['bidNoContractorResponse']['bidNotification']['receiveLocation']], 
                        'DIADIEM_MOTHAU': [data_get_by_id['bidNoContractorResponse']['bidNotification']['bidOpenLocation']], 
                        'THOIDIEM_MOTHAU': [data_get_by_id['bidNoContractorResponse']['bidNotification']['bidOpenDate']], 
                        'THOIDIEM_DONGTHAU': [data_get_by_id['bidNoContractorResponse']['bidNotification']['bidCloseDate']], 
                        'SOTIEN_DAMBAO_DUTHAU': [data_get_by_id['bidNoContractorResponse']['bidNotification']['guaranteeValue']], 
                        'HINHTHUC_DAMBAO_DUTHAU': [data_get_by_id['bidNoContractorResponse']['bidNotification']['guaranteeForm']], 
                        'HIEULUC_HSDT': [str(data_get_by_id['bidNoContractorResponse']['bidNotification']['bidValidityPeriod']) + " ngày"],
                        'HSMT_SOQD': [data_get_by_id['bidInvContractorOfflineDTO']['decisionNo']],
                        'HSMT_NGAYQD': [datetime.strptime(str(data_get_by_id['bidInvContractorOfflineDTO']['decisionDate']).split(".")[0], "%Y-%m-%dT%H:%M:%S").strftime("%d/%m/%Y")],
                        'HSMT_NOIBANHANH': [data_get_by_id['bidInvContractorOfflineDTO']['decisionAgency']],
                        'HSMT_FILE_NAME': [data_get_by_id['bidInvContractorOfflineDTO']['decisionFileName']],
                        'HSMT_FILE_ID': [data_get_by_id['bidInvContractorOfflineDTO']['decisionFileId']],
                        'BBMT_SO_NHATHAU_THAMDU': [df_GOITHAU["numBidderJoin"][i]], #NẾU 1_MTHS THÌ ĐÂY LÀ BIÊN BẢN MỞ THẦU, NẾU 1_HTHS THÌ ĐÂY LÀ BIÊN BẢN MỞ HSĐXKT
                        'THOI_DIEM_HOANTHANH_MOTHAU': [THOI_DIEM_HOANTHANH_MOTHAU], #NẾU 1_MTHS THÌ ĐÂY LÀ BIÊN BẢN MỞ THẦU, NẾU 1_HTHS THÌ ĐÂY LÀ BIÊN BẢN MỞ HSĐXKT
                        'HSDXKT_SO_QD': [HSDXKT_SO_QD],
                        'HSDXKT_NGAY_QD': [HSDXKT_NGAY_QD],
                        'HSDXKT_NOIBANHANH_QD': [HSDXKT_NOIBANHANH_QD],
                        'HSDXKT_FILE_ID': [HSDXKT_FILE_ID],
                        'HSDXKT_FILE_NAME': [HSDXKT_FILE_NAME],
                        'THOIDIEM_HOANTHANH_MO_HSDXTC': [THOIDIEM_HOANTHANH_MO_HSDXTC],
                        "KQLCNT_SO_QD" : [KQLCNT_SO_QD],
                        "KQLCNT_NGAY_QD" : [KQLCNT_NGAY_QD],
                        "KQLCNT_NOIBANHANH_QD" : [KQLCNT_NOIBANHANH_QD],
                        "KQLCNT_FILE_ID" : [KQLCNT_FILE_ID],
                        "KQLCNT_FILE_NAME" : [KQLCNT_FILE_NAME],
                        "BCDG_HSDT_FILE_ID" : [BCDG_HSDT_FILE_ID],
                        "BCDG_HSDT_FILE_NAME" : [BCDG_HSDT_FILE_NAME],
                        "HUYTHAU_LOAI": [HUYTHAU_LOAI],
                        "HUYTHAU_SO_QD": [HUYTHAU_SO_QD],
                        "HUYTHAU_NGAY_QD": [HUYTHAU_NGAY_QD],
                        "HUYTHAU_NOIBANHANH_QD": [HUYTHAU_NOIBANHANH_QD],
                        "HUYTHAU_FILE_ID": [HUYTHAU_FILE_ID],
                        "HUYTHAU_FILE_NAME": [HUYTHAU_FILE_NAME],
                        "HUYTHAU_NGAY": [HUYTHAU_NGAY],
                        "HUYTHAU_REASON": [HUYTHAU_REASON],
                        "GIATRUNGTHAU" : [GIATRUNGTHAU],
                        "NGAY_KY_HOPDONG" : [NGAY_KY_HOPDONG],
                        "TYLE_TIETKIEM": [TYLE_TIETKIEM],
                        "THONGTIN_CHIALO": [THONGTIN_CHIALO]
                    }

                    df_GOITHAU_ct = pd.concat([df_GOITHAU_ct,pd.DataFrame(data_newRow, index=[0])], ignore_index=True)
    
    # Record the end time
    end_time = time.time()
    # Calculate the elapsed time in seconds
    elapsed_time = end_time - start_time
    # Convert seconds into hours, minutes, and seconds
    hours, remainder = divmod(elapsed_time, 3600)
    minutes, seconds = divmod(remainder, 60)
    print(f"Tổng thời gian lấy dữ liệu: {int(hours)} hours, {int(minutes)} minutes, {int(seconds)} seconds")

if __name__ == "__main__":
    main()
