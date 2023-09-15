import requests
import json
import pandas as pd
from datetime import datetime
import calendar
import urllib3
import time

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning) #Disable notification of warning can't verify when send request

#FUNCTION TO UPLOAD DATAFRAME TO GOOGLESHEET
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread.exceptions import APIError
def is_dataframe_empty(data_frame, name="DataFrame"):
    if data_frame.empty:
        print(f"{name} is empty.")
        return True
    return False
def upload_dataframe_to_google_sheet(token_file, spreadsheet_id, worksheet_name, data_frame, replace_data=True):
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
            if replace_data:
                # Clear the existing data and insert new data
                worksheet.clear()
                data_to_insert = [data_frame.columns.tolist()] + data_frame.values.tolist()
                worksheet.insert_rows(data_to_insert, 1)
            else:
                # Append data to the worksheet
                data_to_insert = data_frame.values.tolist()
                num_rows = len(worksheet.get_all_values())
                worksheet.insert_rows(data_to_insert, num_rows + 1)
            return True, "Data uploaded successfully for worksheet: " + str(worksheet_name)
    except APIError as e:
        return False, f"API Error: {e}"
    except Exception as e:
        return False, f"An error occurred: {e}"
    
# Record the start time
start_time = time.time()

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
                    "vnz000023738": "7.CTTĐ Đồng Nai",
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
def extract_dataframe_to_Excel(dataframe, file_name_excel, sheet_excel, icluded_header):
    import xlwings as xw
    if icluded_header == False:
        # change first row to header
        dataframe = dataframe.rename(columns=dataframe.iloc[0])
        # drop first row
        dataframe.drop(index=dataframe.index[0], axis=0, inplace=True)
    sheet_df_mapping = {sheet_excel: dataframe}
    with xw.App(visible=False) as app:
        wb = app.books.open(file_name_excel)
        current_sheets = [sheet.name for sheet in wb.sheets]
        for sheet_name in sheet_df_mapping.keys():
            if sheet_name in current_sheets:
                wb.sheets(sheet_name).range('A1').value = sheet_df_mapping.get(sheet_name)
            else:
                new_sheet = wb.sheets.add(after=wb.sheets.count)
                new_sheet.range('A1').value = sheet_df_mapping.get(sheet_name)
                new_sheet.name = sheet_name
        wb.save()
headers = {
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

# LẤY ID CỦA MÃ KHLCNT TRONG DANH SÁCH KHI SEACH MA DINH DANH
#"vn5701662152 AND vnz000009825 AND vnz000005052 AND vnz000017073 AND vnz000005091 AND vnz000023752 AND vnz000013297 AND vnz000023738 AND vnz000016981 AND vn5800452036 AND vn5700434869 AND vn0101264520 AND vn0102379203"
df_KHLCNT = pd.DataFrame()
url_search = "https://muasamcong.mpi.gov.vn/o/egp-portal-contractor-selection-v2/services/smart/search"
payload_ds_KHLCNT = json.dumps([
  { "pageSize": 10000,
    "pageNumber": 0,
    "query": [
      {
        "index": "es-contractor-selection",
        "keyWord":"vn5701662152 AND vnz000009825 AND vnz000005052 AND vnz000017073 AND vnz000005091 AND vnz000023752 AND vnz000013297 AND vnz000023738 AND vnz000016981 AND vn5800452036 AND vn5700434869 AND vn0101264520 AND vn0102379203",
        "matchType": "all-1",
        "matchFields": ["planNo","name","investorName","procuringEntityName"],
        "filters": [{"fieldName": "type","searchType": "in","fieldValues": ["es-plan-project-p"]}]
      }]}
])
response = requests.request("POST", url_search, headers=headers, data=payload_ds_KHLCNT, verify=False, timeout=30)
data = json.loads(response.text)
df_KHLCNT = pd.concat([df_KHLCNT,pd.DataFrame(data['page']['content'])], ignore_index=True)

#LẤY CHI TIẾT KHLCNT
df_ct_KHLCNT = pd.DataFrame()
df_GOITHAU = pd.DataFrame()
for i in range(0, df_KHLCNT['id'].count()):
    id_KHLCNT = str(df_KHLCNT['id'][i])
    print('[KHLCNT] ' + str(i+1) + "/" + str(df_KHLCNT['id'].count()) + ": " + id_KHLCNT)
    data_ct_KHLCNT = json.loads(requests.request("POST", url="https://muasamcong.mpi.gov.vn/o/egp-portal-contractor-selection-v2/services/expose/lcnt/bid-po-bidp-plan-project-view/get-by-id", headers=headers, data=json.dumps({"id": id_KHLCNT}), verify=False, timeout=30).text)
    
    thoi_gian_thuc_hien_du_an = None
    pperiod = data_ct_KHLCNT['bidPoBidpPlanProjectDetailView']['pperiod']
    pperiod_unit = data_ct_KHLCNT['bidPoBidpPlanProjectDetailView']['pperiodUnit']
    if pperiod is not None:
        if pperiod_unit == "D":
            thoi_gian_thuc_hien_du_an = f"{pperiod} ngày"
        else:
            thoi_gian_thuc_hien_du_an = f"{pperiod} tháng"

    data_KHLCNT_ct = {
                        'MaDinhDanh': [str(data_ct_KHLCNT['bidpPlanDetailToProjectList'][0]['createdBy']).split("-")[0]],
                        'TenDonVi': [MAPPING_TENDONVI[str(data_ct_KHLCNT['bidpPlanDetailToProjectList'][0]['createdBy']).split("-")[0]]],
                        'planID': [data_ct_KHLCNT['bidPoBidpPlanProjectDetailView']['id']], 
                        'planNo': [data_ct_KHLCNT['bidPoBidpPlanProjectDetailView']['planNo']],
                        'planName': [data_ct_KHLCNT['bidPoBidpPlanProjectDetailView']['name']],
                        'planVersion': [data_ct_KHLCNT['bidPoBidpPlanProjectDetailView']['planVersion']],
                        'planStatus': [data_ct_KHLCNT['bidPoBidpPlanProjectDetailView']['status']],
                        'TenDuToanMuaSam': [data_ct_KHLCNT['bidPoBidpPlanProjectDetailView']['pname']],
                        'SoLuongGoiThau': [data_ct_KHLCNT['bidPoBidpPlanProjectDetailView']['bidPack']],
                        'DuToanMuaSam': [data_ct_KHLCNT['bidPoBidpPlanProjectDetailView']['investTotal']],
                        'CCY_DuToanMuaSam': [data_ct_KHLCNT['bidPoBidpPlanProjectDetailView']['investTotalUnit']],
                        'DuToanMuaSam_BangChu': [data_ct_KHLCNT['bidPoBidpPlanProjectDetailView']['investTotalUnit']],
                        'QD_KHLCNT_So': [data_ct_KHLCNT['bidPoBidpPlanProjectDetailView']['decisionNo']],
                        'QD_KHLCNT_Ngay': [datetime.strptime(data_ct_KHLCNT['bidPoBidpPlanProjectDetailView']['decisionDate'], "%Y-%m-%dT%H:%M:%S").strftime("%d/%m/%Y")],
                        'QD_KHLCNT_NoiBanHanh': [data_ct_KHLCNT['bidPoBidpPlanProjectDetailView']['decisionAgency']],
                        'QD_KHLCNT_FileID': [data_ct_KHLCNT['bidPoBidpPlanProjectDetailView']['decisionFileId']],
                        'QD_KHLCNT_FileName': [data_ct_KHLCNT['bidPoBidpPlanProjectDetailView']['decisionFileName']],
                        'NgayDangTai_KHLCNT': [datetime.strptime(str(data_ct_KHLCNT['bidPoBidpPlanProjectDetailView']['publicDate']).split(".")[0], "%Y-%m-%dT%H:%M:%S").strftime("%d/%m/%Y")],
                        'PHANLOAI_KHLCNT': [MAPPING_PHANLOAI_KHLCNT[data_ct_KHLCNT['bidPoBidpPlanProjectDetailView']['planType']]],
                        'MucTieuDauTu': [data_ct_KHLCNT['bidPoBidpPlanProjectDetailView']['investTarget']],
                        'SuDungVonODA': [data_ct_KHLCNT['bidPoBidpPlanProjectDetailView']['isOda']],
                        'DiaDiemThucHien': [data_ct_KHLCNT['bidPoBidpPlanProjectDetailView']['location']],
                        'HinhThucQLDA': [MAPPING_HINHTHUC_QLDA[data_ct_KHLCNT['bidPoBidpPlanProjectDetailView']['pform']]],
                        'NhomDuAn': [MAPPING_NHOM_DUAN[data_ct_KHLCNT['bidPoBidpPlanProjectDetailView']['pgroup']]],
                        'ThoiGianThucHienDuAn': [thoi_gian_thuc_hien_du_an]
                    }
    df_ct_KHLCNT = pd.concat([df_ct_KHLCNT,pd.DataFrame(data_KHLCNT_ct, index=[0])], ignore_index=True)

    df_GOITHAU_new = pd.DataFrame(data_ct_KHLCNT['lsBidpPlanDetailDTO'])
    for j in range(0, df_GOITHAU_new['id'].count()):
        print('[GET BID FROM PLAN] ' + str(j+1) + "/" + str(df_GOITHAU_new['id'].count()) + ": [ID_GOITHAU]" + df_GOITHAU_new['id'][j])
        #XỬ LÝ THỜI GIAN BẮT ĐẦU TỔ CHỨC LCNT
        giatri_quy = df_GOITHAU_new["bidStartQuarter"][j]
        giatri_nam = df_GOITHAU_new["bidStartYear"][j]
        if df_GOITHAU_new["bidStartUnit"][j] == "Q":    
            TGIAN_BATDAU_TOCHUC_LCNT = "Quý " + str(giatri_quy) + ", " + str(giatri_nam)
            if giatri_quy == "I":
                giatri_batdau_LCNT_ct = datetime(giatri_nam,3,31).strftime("%d/%m/%Y")
                giatri_thang = 3
            if giatri_quy == "II":
                giatri_batdau_LCNT_ct = datetime(giatri_nam,6,30).strftime("%d/%m/%Y")
                giatri_thang = 6
            if giatri_quy == "III":
                giatri_batdau_LCNT_ct = datetime(giatri_nam,9,30).strftime("%d/%m/%Y")
                giatri_thang = 9
            if giatri_quy == "IV":
                giatri_batdau_LCNT_ct = datetime(giatri_nam,12,31).strftime("%d/%m/%Y")
                giatri_thang = 12
            TGIAN_BATDAU_TOCHUC_LCNT_CHITIET = giatri_batdau_LCNT_ct
            THANG_BAOCAO = giatri_thang
            NAM_BAOCAO = giatri_nam
        else:
            giatri_thang = int(df_GOITHAU_new["bidStartMonth"][j])
            TGIAN_BATDAU_TOCHUC_LCNT = "Tháng " + str(giatri_thang) + ", " + str(giatri_nam)
            TGIAN_BATDAU_TOCHUC_LCNT_CHITIET =datetime(giatri_nam,giatri_thang,calendar.monthrange(giatri_nam,giatri_thang)[1]).strftime("%d/%m/%Y")
            THANG_BAOCAO = giatri_thang
            NAM_BAOCAO = giatri_nam
        Ma_TBMT = ""
        ID_TBMT = ""
        notifyVersion = ""
        numBidderJoin = ""
        bidRealityOpenDate = ""
        numBidderTech = ""
        numClarifyNotRes = ""
        numClarifyReq = ""
        numPetition = ""
        numPetitionHsmt = ""
        numPetitionKqlcnt = ""
        numPetitionLcnt = ""
        STATUS_BID = ""
        if df_GOITHAU_new["linkNotifyInfo"][j] != None:
            Ma_TBMT = str(df_GOITHAU_new["linkNotifyInfo"][j]).split('","')[0].split('":"')[1]
            ID_TBMT = str(df_GOITHAU_new["linkNotifyInfo"][j]).split('","')[2].split('":"')[1]
            payload_search_TBMT = json.dumps([
                                    {"pageSize": 10,"pageNumber": "0","query": [
                                        {
                                            "index": "es-contractor-selection",
                                            "keyWord": str(Ma_TBMT),
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
                                                "1"]}]}]}])
            response_search_TBMT = requests.request("POST", url_search, headers=headers, data=payload_search_TBMT, verify=False, timeout=30)
            data_search_TBMT = json.loads(response_search_TBMT.text)
            #ID_TBMT = data_search_TBMT['page']['content'][0]["notifyId"]
            print('[CHECK STATUS BID] MA_TBMT: ' + str(Ma_TBMT) + "; ID_TBMT: " + str(ID_TBMT))
            STATUS_BID = data_search_TBMT['page']['content'][0]["status"]
            STATUS_TBMT = MAPPING_STATUS_TBMT[data_search_TBMT['page']['content'][0]["statusForNotify"]]
            # notifyVersion = data_search_TBMT['page']['content'][0]["notifyVersion"]
            if "numBidderJoin" in data_search_TBMT['page']['content'][0]:
                numBidderJoin = data_search_TBMT['page']['content'][0]["numBidderJoin"]
            if "bidRealityOpenDate" in data_search_TBMT['page']['content'][0]:
                bidRealityOpenDate = data_search_TBMT['page']['content'][0]["bidRealityOpenDate"]
            numBidderTech = data_search_TBMT['page']['content'][0]["numBidderTech"]
            if "numClarifyNotRes" in data_search_TBMT['page']['content'][0]:
                numClarifyNotRes = data_search_TBMT['page']['content'][0]["numClarifyNotRes"]
            numClarifyReq = data_search_TBMT['page']['content'][0]["numClarifyReq"]
            numPetition = data_search_TBMT['page']['content'][0]["numPetition"]
            numPetitionHsmt = data_search_TBMT['page']['content'][0]["numPetitionHsmt"]
            numPetitionKqlcnt = data_search_TBMT['page']['content'][0]["numPetitionKqlcnt"]
            numPetitionLcnt = data_search_TBMT['page']['content'][0]["numPetitionLcnt"]
        else:
            STATUS_TBMT = MAPPING_STATUS_KHONG_TBMT[df_GOITHAU_new["bidForm"][j]]
            
        data_GoiThau = {
                            'MaDinhDanh': [str(data_ct_KHLCNT['bidpPlanDetailToProjectList'][0]['createdBy']).split("-")[0]],
                            'TenDonVi': [MAPPING_TENDONVI[str(data_ct_KHLCNT['bidpPlanDetailToProjectList'][0]['createdBy']).split("-")[0]]],
                            'ID_GOITHAU': [df_GOITHAU_new["id"][j]],
                            'planNo': [df_GOITHAU_new["planNo"][j]],
                            # 'planID': [df_GOITHAU_new["planId"][j]],
                            # 'planVersion': [df_GOITHAU_new["planVersion"][j]],
                            'TEN_GOITHAU': [df_GOITHAU_new["bidName"][j]],
                            'TRONGNUOC_QUOCTE': [MAPPING_TRONGNUOC_QUOCTE[df_GOITHAU_new["isDomestic"][j]]],
                            'HINHTHUC_DUTHAU': [MAPPING_HINHTHUC_DUTHAU[df_GOITHAU_new["isInternet"][j]]],
                            'LOAI_HOPDONG': [MAPPING_LOAI_HD[df_GOITHAU_new["ctype"][j]]],
                            'PHUONGTHUC_LCNT': [MAPPING_PHUONGTHUC_LCNT[df_GOITHAU_new["bidMode"][j]]],
                            'HINHTHUC_LCNT': [MAPPING_HINHTHUC_LCNT[df_GOITHAU_new["bidForm"][j]]],
                            'LINH_VUC': [MAPPING_LINHVUC[df_GOITHAU_new["bidField"][j]]],
                            'GIA_GOITHAU': [df_GOITHAU_new["bidPrice"][j]],
                            'CCY_GIA_GOITHAU': [df_GOITHAU_new["bidPriceUnit"][j]],
                            # 'bidPriceEx': [df_GOITHAU_new["bidPriceEx"][j]],
                            # 'recCapital': [df_GOITHAU_new["recCapital"][j]],
                            # 'CCY_recCapital': [df_GOITHAU_new["recCapitalUnit"][j]],
                            'PHAN_LO': [MAPPING_PHANLO[df_GOITHAU_new["isMultiLot"][j]]],
                            'NGUON_VON': [df_GOITHAU_new["capitalDetail"][j]],
                            'THOIGIAN_THUCHIEN_HOPDONG': [str(df_GOITHAU_new["cperiod"][j]) + " " + str(MAPPING_VIETTAT_THOIGIAN[df_GOITHAU_new["cperiodUnit"][j]])],
                            'NAM_BAOCAO': [NAM_BAOCAO],
                            'THANG_BAOCAO': [THANG_BAOCAO],
                            'TGIAN_BATDAU_TOCHUC_LCNT': [TGIAN_BATDAU_TOCHUC_LCNT],
                            'TGIAN_BATDAU_TOCHUC_LCNT_CHITIET': [TGIAN_BATDAU_TOCHUC_LCNT_CHITIET],
                            'MA_TBMT': [Ma_TBMT],
                            'ID_TBMT': [ID_TBMT],
                            'STATUS_TBMT': [STATUS_TBMT],
                            'STATUS_BID': [STATUS_BID],
                            'linkNotifyInfo': [df_GOITHAU_new["linkNotifyInfo"][j]],
                            # "notifyVersion": [notifyVersion],
                            "numBidderJoin": [numBidderJoin],
                            "bidRealityOpenDate": [bidRealityOpenDate]
                            # "numBidderTech": [numBidderTech],
                            # "numClarifyNotRes": [numClarifyNotRes],
                            # "numClarifyReq": [numClarifyReq],
                            # "numPetition": [numPetition],
                            # "numPetitionHsmt": [numPetitionHsmt],
                            # "numPetitionKqlcnt": [numPetitionKqlcnt],
                            # "numPetitionLcnt": [numPetitionLcnt]
                        }
        df_GOITHAU = pd.concat([df_GOITHAU, pd.DataFrame(data_GoiThau)], ignore_index=True)

df_giahan = pd.DataFrame()
df_lamro = pd.DataFrame()
df_kiennghi = pd.DataFrame()
df_phanLo = pd.DataFrame()
df_bidaInvChapterConfList = pd.DataFrame()
df_bidoInvBiddingDTO = pd.DataFrame()
df_bidoPreNotifyContractorResult = pd.DataFrame()
df_bidoPreBidConferenceList = pd.DataFrame()
df_bidContractorShortlistMDTO = pd.DataFrame()
df_mothau = pd.DataFrame()
df_DXKT = pd.DataFrame()
df_DXTC = pd.DataFrame()
df_KQLCNT = pd.DataFrame()
df_bidoNotifyContractorP = pd.DataFrame()
df_DanhMucHangHoa = pd.DataFrame()

#LẤY CHI TIẾT THÔNG BÁO MỜI THẦU CỦA GÓI THẦU
final_df_GOITHAU = pd.DataFrame()
for i in range(0, df_GOITHAU['ID_GOITHAU'].count()):
    print("[GET BID DETAILS]" + str(i+1) + "/" + str(df_GOITHAU['ID_GOITHAU'].count()) + ": " + str(df_GOITHAU['linkNotifyInfo'][i]))
    if df_GOITHAU['linkNotifyInfo'][i] is not None:
        Ma_TBMT = df_GOITHAU['MA_TBMT'][i]
        print("SEARCH TBMT TO GET ID")
        payload_search_ID = json.dumps([
        {
            "pageSize": 1,
            "pageNumber": 0,
            "query": [
            {
                "index": "es-contractor-selection",
                "keyWord": str(Ma_TBMT),
                "matchType": "exact",
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
                }
                ]
            }
            ]
        }
        ])
        data_search_ID = json.loads(requests.request("POST", url_search, headers=headers, data=payload_search_ID, verify=False, timeout=30).text)
        ID_TBMT = df_GOITHAU['ID_TBMT'][i]
        print("GET INFORMATION BY ID")
        response = requests.request("POST", url="https://muasamcong.mpi.gov.vn/o/egp-portal-contractor-selection-v2/services/expose/lcnt/bid-po-bido-notify-contractor-view/get-by-id", headers=headers, data=json.dumps({"id": str(ID_TBMT)}), verify=False, timeout=30)
        data = json.loads(response.text)
        if Ma_TBMT != "IB2300151471":
            #LẤY THÔNG TIN bidNotification
            if data['bidNoContractorResponse'] != None:
                if data['bidNoContractorResponse']['bidNotification'] != None:
                    if 'delayDTOList' in data['bidNoContractorResponse']['bidNotification']:
                        #LẤY THÔNG TIN GIA HẠN
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
                                                    'TAIKHOAN_GIAHAN': [data['bidNoContractorResponse']['bidNotification']['delayDTOList'][row_json]["createdBy"]],
                                                }
                                df_giahan = pd.concat([df_giahan, pd.DataFrame(data_giahan_new)], ignore_index=True)
                    #         del data['bidNoContractorResponse']['bidNotification']['delayDTOList']
                    # if 'getVersionDTOS' in data['bidNoContractorResponse']['bidNotification']:
                    #     del data['bidNoContractorResponse']['bidNotification']['getVersionDTOS']
                    if 'lotDTOList' in data['bidNoContractorResponse']['bidNotification']:
                        if df_GOITHAU["PHAN_LO"][i] == "Có chia phần/ lô":
                            df_phanLo = pd.concat([df_phanLo, pd.DataFrame(data['bidNoContractorResponse']['bidNotification']['lotDTOList'])], ignore_index=True)
                        # del data['bidNoContractorResponse']['bidNotification']['lotDTOList']
                    PHUONGTHUC_LCNT = data['bidNoContractorResponse']['bidNotification']["bidMode"]
                    PACK_TYPE = 1
                    if PHUONGTHUC_LCNT == "1_MTHS":
                        PACK_TYPE = 0

                #LẤY THÔNG TIN BIÊN BẢN MỞ THẦU
                print("[GET THÔNG TIN BIÊN BẢN MỜI THẦU")
                response_BBMT = requests.request("POST", url="https://muasamcong.mpi.gov.vn/o/egp-portal-contractor-selection-v2/services/expose/ldtkqmt/bid-notification-p/bid-open", headers=headers, data=json.dumps({"notifyNo": str(Ma_TBMT),"notifyId": str(ID_TBMT),"packType": PACK_TYPE}), verify=False, timeout=30)
                data_mothau = json.loads(response_BBMT.text)
                if data_mothau['bidSubmissionByContractorViewResponse'] != None:
                    if data_mothau['bidSubmissionByContractorViewResponse']['bidSubmissionDTOList'] != None:
                        df_mothau_new = pd.DataFrame(data_mothau['bidSubmissionByContractorViewResponse']['bidSubmissionDTOList'])
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
                        df_mothau = pd.concat([df_mothau, df_mothau_new], ignore_index=True)

                #LẤY THÔNG TIN PHÊ DUYỆT HSĐXKT
                HSDXKT_SO_QD = ""
                HSDXKT_NGAY_QD = ""
                HSDXKT_NOIBANHANH_QD = ""
                HSDXKT_FILE_ID = ""
                HSDXKT_FILE_NAME = ""
                if PHUONGTHUC_LCNT == "1_HTHS":
                    #CHECK XEM ĐÃ PHÊ DUYỆT KẾT QUẢ ĐẠT BƯỚC KỸ THUẬT HAY CHƯA
                    if 'techReqId' in data_search_ID['page']['content'][0]:
                        print("GET THÔNG TIN HỒ SƠ ĐỀ XUẤT KỸ THUẬT")
                        response_DXKT = requests.request("POST", url="https://muasamcong.mpi.gov.vn/o/egp-portal-contractor-selection-v2/services/ldtdsnt/tech-req-approval/get-by-id", headers=headers, data=json.dumps({"id": str(data_search_ID['page']['content'][0]['techReqId'])}), verify=False, timeout=30)
                        if response_DXKT.text != "":
                            data_DXKT = json.loads(response_DXKT.text)
                            HSDXKT_SO_QD = data_DXKT['bidrTechReqApprovalDTO']["bidReqApprovalDetail"]['decisionNo']
                            HSDXKT_NGAY_QD = datetime.strptime(str(data_DXKT['bidrTechReqApprovalDTO']["bidReqApprovalDetail"]['approvalDate']).split(".")[0], "%Y-%m-%dT%H:%M:%S").strftime("%d/%m/%Y")
                            HSDXKT_NOIBANHANH_QD = data_DXKT['bidrTechReqApprovalDTO']["bidReqApprovalDetail"]['decisionAgencyName']
                            HSDXKT_FILE_ID = data_DXKT['bidrTechReqApprovalDTO']["bidReqApprovalDetail"]['decisionFileId']
                            HSDXKT_FILE_NAME = data_DXKT['bidrTechReqApprovalDTO']["bidReqApprovalDetail"]['decisionFileName']
                            if data_DXKT["lsBideLotEvalResultViewDTO"] != []:
                                for row_json in range(0,len(data_DXKT["lsBideLotEvalResultViewDTO"])):
                                    data_DXKT_new = {
                                                        'MaDinhDanh': [df_GOITHAU['MaDinhDanh'][i]],
                                                        'TenDonVi': [df_GOITHAU['TenDonVi'][i]],
                                                        'MA_TBMT': [data_DXKT["lsBideLotEvalResultViewDTO"][row_json]['notifyNo']],
                                                        'ID_TBMT': [data_DXKT["lsBideLotEvalResultViewDTO"][row_json]['notifyId']],
                                                        'MADINHDANH': [data_DXKT["lsBideLotEvalResultViewDTO"][row_json]['contractorCode']],
                                                        'TEN_NHATHAU': [data_DXKT["lsBideLotEvalResultViewDTO"][row_json]['contractorName']],
                                                        'MA_LIENDANH': [data_DXKT["lsBideLotEvalResultViewDTO"][row_json]['ventureName']],
                                                        'TEN_LIENDANH': [data_DXKT["lsBideLotEvalResultViewDTO"][row_json]['ventureCode']],
                                                        'LOAI_DANHGIA': [MAPPING_EVALTYPE_HSDXKT[data_DXKT["lsBideLotEvalResultViewDTO"][row_json]['evalType']]],
                                                        'KETQUA_DANHGIA': [MAPPING_RESULT_HSDXKT[data_DXKT["lsBideLotEvalResultViewDTO"][row_json]['result']]],
                                                        'TECH_SCORE': [data_DXKT["lsBideLotEvalResultViewDTO"][row_json]['techScore']]
                                                        # 'evalContent': [data_DXKT["lsBideLotEvalResultViewDTO"][row_json]['evalContent']],
                                                        # 'content': [data_DXKT["lsBideLotEvalResultViewDTO"][row_json]['content']]
                                                    }
                                    df_DXKT = pd.concat([df_DXKT, pd.DataFrame(data_DXKT_new)], ignore_index=True)
                                
                                #df_DXKT = pd.concat([df_DXKT, pd.DataFrame(data_DXKT['lsBideLotEvalResultViewDTO'])], ignore_index=True)
                
                HUYTHAU_LOAI = ""
                HUYTHAU_SO_QD = ""
                HUYTHAU_NGAY_QD = ""
                HUYTHAU_NOIBANHANH_QD = ""
                HUYTHAU_FILE_ID = ""
                HUYTHAU_FILE_NAME = ""
                HUYTHAU_NGAY = ""
                HUYTHAU_REASON = ""

                #LẤY THÔNG TIN HUỶ THẦU
                if "bidCancelingResponse" in data:
                    if "cancelType" in data["bidCancelingResponse"]:
                        if data["bidCancelingResponse"]["cancelType"] != None:
                            if data["bidCancelingResponse"]["cancelType"] == '0':
                                HUYTHAU_LOAI = "Huỷ TBMT"
                                HUYTHAU_SO_QD = data["bidCancelingResponse"]["cancelDecisionNo"]
                                HUYTHAU_NGAY_QD = data["bidCancelingResponse"]["cancelDecisionDate"]
                                HUYTHAU_NOIBANHANH_QD = data["bidCancelingResponse"]["cancelDecisionAgency"]
                                HUYTHAU_FILE_ID = data["bidCancelingResponse"]["cancelFileAttachId"]
                                HUYTHAU_FILE_NAME = data["bidCancelingResponse"]["cancelFileAttachName"]
                                HUYTHAU_NGAY = data["bidCancelingResponse"]["cancelDate"]
                                HUYTHAU_REASON = data["bidCancelingResponse"]["cancelReason"]
                            else:
                                HUYTHAU_LOAI = "Huỷ thầu"
                                HUYTHAU_SO_QD = data["bidCancelingResponse"]["cancelDecisionNo"]
                                HUYTHAU_NGAY_QD = data["bidCancelingResponse"]["cancelDecisionDate"]
                                HUYTHAU_NOIBANHANH_QD = data["bidCancelingResponse"]["cancelDecisionAgency"]
                                HUYTHAU_FILE_ID = data["bidCancelingResponse"]["cancelFileAttachId"]
                                HUYTHAU_FILE_NAME = data["bidCancelingResponse"]["cancelFileAttachName"]
                                HUYTHAU_NGAY = data["bidCancelingResponse"]["cancelDate"]
                                HUYTHAU_REASON = data["bidCancelingResponse"]["cancelReason"]

                #LẤY THÔNG TIN HỒ SƠ ĐỀ XUẤT TÀI CHÍNH
                THOIDIEM_HOANTHANH_MO_HSDXTC = ""
                if PHUONGTHUC_LCNT == "1_HTHS":
                    print("GET THÔNG TIN HỒ SƠ ĐỀ XUẤT TÀI CHÍNH")
                    response_DXTC = requests.request("POST", url="https://muasamcong.mpi.gov.vn/o/egp-portal-contractor-selection-v2/services/expose/ldtkqmt/bid-notification-p/bid-open"
                                                , headers=headers, data=json.dumps({"notifyNo": Ma_TBMT,"type": "TBMT","packType": 2,"viewType": 0,"notifyId": ID_TBMT}), verify=False, timeout=30)
                    
                    if response.text != '{"bidNoContractorResponse":null,"bidoBidroundMngViewDTO":null,"bidSubmissionByContractorViewResponse":null}':
                        data_DXTC = json.loads(response_DXTC.text)
                        if 'bidSubmissionByContractorViewResponse' in data_DXTC:
                            if data_DXTC["bidSubmissionByContractorViewResponse"] is not None:
                                if 'bidSubmissionDTOList' in data_DXTC["bidSubmissionByContractorViewResponse"]:
                                    if data_DXTC["bidSubmissionByContractorViewResponse"]["bidSubmissionDTOList"] is not None and data_DXTC["bidSubmissionByContractorViewResponse"]["bidSubmissionDTOList"] != []:
                                        THOIDIEM_HOANTHANH_MO_HSDXTC = datetime.strptime(str(data_DXTC["bidSubmissionByContractorViewResponse"]["bidSubmissionDTOList"][0]["createdDateBidOpen"]).split(".")[0], "%Y-%m-%dT%H:%M:%S").strftime("%d/%m/%Y")
                                        df_DXTC_new = pd.DataFrame(data_DXTC["bidSubmissionByContractorViewResponse"]["bidSubmissionDTOList"])
                                        df_DXTC_new['Ma_TBMT'] = Ma_TBMT
                                        df_DXTC_new['ID_TBMT'] = ID_TBMT
                                        # Apply a lambda function to conditionally fill ventureName
                                        df_DXTC_new['contractorName_final'] = df_DXTC_new.apply(lambda row: row['contractorName'] if pd.isnull(row['ventureName']) else row['ventureName'], axis=1)
                                        df_DXTC_new['contractorCode_final'] = df_DXTC_new.apply(lambda row: row['contractorCode'] if pd.isnull(row['ventureCode']) else row['ventureCode'], axis=1)
                                        # Define a function to format the contract period
                                        def format_contract_period(row):
                                            if row['contractPeriodDTUnit'] == 'D':
                                                return f"{row['contractPeriodDT']} ngày"
                                            elif row['contractPeriodDTUnit'] == 'M':
                                                return f"{row['contractPeriodDT']} tháng"
                                            else:
                                                return None  # Handle other units if needed

                                        # Apply the function to create the 'NewContractPeriod' column
                                        df_DXTC_new['NewContractPeriod'] = df_DXTC_new.apply(format_contract_period, axis=1)
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

                #LẤY THÔNG TIN KẾT QUẢ LỰA CHỌN NHÀ THẦU
                KQLCNT_SO_QD = ""
                KQLCNT_NGAY_QD = ""
                KQLCNT_NOIBANHANH_QD = ""
                KQLCNT_FILE_ID = ""
                KQLCNT_FILE_NAME = ""
                BCDG_HSDT_FILE_ID = ""
                BCDG_HSDT_FILE_NAME = ""
                GIATRUNGTHAU = None
                NGAY_KY_HOPDONG = ""

                if 'inputResultId' in data_search_ID['page']['content'][0]:
                    print("GET THÔNG TIN KẾT QUẢ LỰA CHỌN NHÀ THẦU")
                    response_KQLCNT = requests.request("POST", url="https://muasamcong.mpi.gov.vn/o/egp-portal-contractor-selection-v2/services/expose/contractor-input-result/get", headers=headers, data=json.dumps({"id": str(data_search_ID['page']['content'][0]['inputResultId'])}), verify=False, timeout=30)
                    if response_KQLCNT.text != "":
                        data_KQLCNT = json.loads(response_KQLCNT.text)
                        if data_KQLCNT["bideContractorInputResultDTO"] != []:
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
                                        if df_GOITHAU["STATUS_TBMT"][i] == "04. Có nhà thầu trúng thầu":
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

                                        if data_KQLCNT["bideContractorInputResultDTO"]['lotResultDTO'][row_json]["goodsList"] is not None:
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
                THOI_DIEM_HOANTHANH_MOTHAU = ""
                if df_GOITHAU["bidRealityOpenDate"][i] != "":
                    THOI_DIEM_HOANTHANH_MOTHAU = datetime.strptime(str(df_GOITHAU["bidRealityOpenDate"][i]).split(".")[0], "%Y-%m-%dT%H:%M:%S").strftime("%d/%m/%Y")
                
                TYLE_TIETKIEM = None
                if df_GOITHAU["STATUS_TBMT"][i] == "04. Có nhà thầu trúng thầu":
                    if data['bidoNotifyContractorM']['bidEstimatePrice'] is not None:
                        TYLE_TIETKIEM = round((data['bidoNotifyContractorM']['bidEstimatePrice'] - GIATRUNGTHAU) * 100 / data['bidoNotifyContractorM']['bidEstimatePrice'],0)
                    else:
                        TYLE_TIETKIEM = round((df_GOITHAU["GIA_GOITHAU"][i] - GIATRUNGTHAU) * 100 / df_GOITHAU["GIA_GOITHAU"][i],0)

                data_newRow = {
                                'MaDinhDanh': [df_GOITHAU['MaDinhDanh'][i]],
                                'TenDonVi': [df_GOITHAU['TenDonVi'][i]],
                                'ID_GOITHAU': [df_GOITHAU["ID_GOITHAU"][i]],
                                'MA_TBMT': [df_GOITHAU["MA_TBMT"][i]],
                                'ID_TBMT': [df_GOITHAU["ID_TBMT"][i]],
                                'planNo': [df_GOITHAU["planNo"][i]],
                                # 'planVersion': [df_GOITHAU["planVersion"][i]],
                                'TEN_GOITHAU': [df_GOITHAU["TEN_GOITHAU"][i]],
                                'LINH_VUC': [df_GOITHAU["LINH_VUC"][i]],
                                'GIA_GOITHAU': [df_GOITHAU["GIA_GOITHAU"][i]],
                                'CCY_GIA_GOITHAU': [df_GOITHAU["CCY_GIA_GOITHAU"][i]],
                                'DUTOAN_GOITHAU': [data['bidoNotifyContractorM']['bidEstimatePrice']],
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
                                'NGAYDANGTAI_TBMT': [datetime.strptime(str(data['bidNoContractorResponse']['bidNotification']['publicDate']).split(".")[0], "%Y-%m-%dT%H:%M:%S").strftime("%d/%m/%Y")], 
                                'PHIENBAN_THAYDOI': [data['bidNoContractorResponse']['bidNotification']['notifyVersion']],
                                'DIADIEM_PHATHANH_HSMT': [data['bidNoContractorResponse']['bidNotification']['issueLocation']], 
                                'DIADIEM_NHAN_HSMT': [data['bidNoContractorResponse']['bidNotification']['receiveLocation']], 
                                'DIADIEM_MOTHAU': [data['bidNoContractorResponse']['bidNotification']['bidOpenLocation']], 
                                'THOIDIEM_MOTHAU': [data['bidNoContractorResponse']['bidNotification']['bidOpenDate']], 
                                'THOIDIEM_DONGTHAU': [data['bidNoContractorResponse']['bidNotification']['bidCloseDate']], 
                                'SOTIEN_DAMBAO_DUTHAU': [data['bidNoContractorResponse']['bidNotification']['guaranteeValue']], 
                                'HINHTHUC_DAMBAO_DUTHAU': [data['bidNoContractorResponse']['bidNotification']['guaranteeForm']], 
                                'HIEULUC_HSDT': [str(data['bidNoContractorResponse']['bidNotification']['bidValidityPeriod']) + " ngày"],
                                'HSMT_SOQD': [data['bidInvContractorOfflineDTO']['decisionNo']],
                                'HSMT_NGAYQD': [datetime.strptime(str(data['bidInvContractorOfflineDTO']['decisionDate']).split(".")[0], "%Y-%m-%dT%H:%M:%S").strftime("%d/%m/%Y")],
                                'HSMT_NOIBANHANH': [data['bidInvContractorOfflineDTO']['decisionAgency']],
                                'HSMT_FILE_NAME': [data['bidInvContractorOfflineDTO']['decisionFileName']],
                                'HSMT_FILE_ID': [data['bidInvContractorOfflineDTO']['decisionFileId']],
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
                                "TYLE_TIETKIEM": [TYLE_TIETKIEM]
                            }
                final_df_GOITHAU = pd.concat([final_df_GOITHAU,pd.DataFrame(data_newRow, index=[0])], ignore_index=True)
                    
            #LẤY THÔNG TIN bidaInvChapterConfList
            df_bidaInvChapterConfList = pd.concat([df_bidaInvChapterConfList, pd.DataFrame(data['bidaInvChapterConfList'])], ignore_index=True)

            #LẤY THÔNG TIN bidoInvBiddingDTO
            df_bidoInvBiddingDTO = pd.concat([df_bidoInvBiddingDTO, pd.DataFrame(data['bidoInvBiddingDTO'])], ignore_index=True)

            #LẤY THÔNG TIN bidoPreNotifyContractorResult
            if data['bidoPreNotifyContractorResult'] != None:
                df_bidoPreNotifyContractorResult = pd.concat([df_bidoPreNotifyContractorResult, pd.DataFrame(data['bidoPreNotifyContractorResult'])], ignore_index=True)

            #LẤY THÔNG TIN bidoPreNotifyContractorResult
            if data['bidContractorShortlistMDTO'] != None:
                df_bidContractorShortlistMDTO_new = pd.DataFrame(data['bidContractorShortlistMDTO']['contractorList'])
                df_bidContractorShortlistMDTO_new['Ma_TBMT'] = Ma_TBMT
                df_bidContractorShortlistMDTO_new['ID_GOITHAU'] = ID_TBMT
                df_bidContractorShortlistMDTO = pd.concat([df_bidContractorShortlistMDTO,df_bidContractorShortlistMDTO_new], ignore_index=True)

            #LẤY THÔNG TIN bidoPreNotifyContractorResult
            if data['bidoPreBidConferenceList'] != []:
                df_bidoPreBidConferenceList = pd.concat([df_bidoPreBidConferenceList, pd.DataFrame(data['bidoPreBidConferenceList'])], ignore_index=True)

            #LẤY THÔNG TIN LÀM RÕ
            if data["biduClarifyReqInvAndContentViewList"] != []:
                for row_json in range(0,len(data["biduClarifyReqInvAndContentViewList"])):
                    df_lamro_new = pd.DataFrame(json.loads(data["biduClarifyReqInvAndContentViewList"][row_json]["clarifyResContent"]))
                    df_lamro_new['ID_LAMRO'] = data["biduClarifyReqInvAndContentViewList"][row_json]["id"]
                    df_lamro_new['Ma_TBMT'] = data["biduClarifyReqInvAndContentViewList"][row_json]["notyfyNo"]
                    df_lamro_new['ID_TBMT'] = data["biduClarifyReqInvAndContentViewList"][row_json]["notyfyId"]
                    df_lamro_new['YEUCAU_LAMRO_TEN'] = data["biduClarifyReqInvAndContentViewList"][row_json]["reqName"]
                    df_lamro_new['YEUCAU_LAMRO_NGAY'] = data["biduClarifyReqInvAndContentViewList"][row_json]["reqDate"]
                    df_lamro_new['YEUCAU_LAMRO_NGAY_KY'] = data["biduClarifyReqInvAndContentViewList"][row_json]["signReqDate"]
                    df_lamro_new['YEUCAU_LAMRO_FILE_ID'] = data["biduClarifyReqInvAndContentViewList"][row_json]["clarify_file_id"]
                    df_lamro_new['YEUCAU_LAMRO_FILE_NAME'] = data["biduClarifyReqInvAndContentViewList"][row_json]["clarify_file_name"]
                    df_lamro_new['TRALOI_LAMRO_NGAY'] = data["biduClarifyReqInvAndContentViewList"][row_json]["signResDate"]
                    df_lamro_new['TRALOI_LAMRO_FILE_ID'] = data["biduClarifyReqInvAndContentViewList"][row_json]["clarifyResFileId"]
                    df_lamro_new['TRALOI_LAMRO_FILE_NAME'] = data["biduClarifyReqInvAndContentViewList"][row_json]["clarifyResFileName"]
                    
                    df_lamro = pd.concat([df_lamro, df_lamro_new], ignore_index=True)
                #df_lamro = pd.concat([df_lamro, pd.DataFrame(data['biduClarifyReqInvAndContentViewList'])], ignore_index=True)

            #LẤY THÔNG TIN KIẾN NGHỊ
            if data["biduPetitionContractorDTOList"] != []:
                for row_json in range(0,len(data["biduPetitionContractorDTOList"])):
                    df_kiennghi_new = pd.DataFrame(json.loads(data["biduPetitionContractorDTOList"][row_json]["content"]))
                    df_kiennghi_new['ID'] = len(df_kiennghi) + 1
                    df_kiennghi_new['ID_KIENNGHI'] = data["biduPetitionContractorDTOList"][row_json]["id"]
                    df_kiennghi_new['MA_KIENNGHI'] = data["biduPetitionContractorDTOList"][row_json]["reqNo"]
                    df_kiennghi_new['TEN_KIENNGHI'] = data["biduPetitionContractorDTOList"][row_json]["reqName"]
                    df_kiennghi_new['NGAY_KIENNGHI'] = data["biduPetitionContractorDTOList"][row_json]["reqDate"]
                    df_kiennghi_new['MA_DINHDANH_NHATHAU_KIENNGHI'] = data["biduPetitionContractorDTOList"][row_json]["contractorCode"]
                    df_kiennghi_new['NHATHAU_KIENNGHI'] = data["biduPetitionContractorDTOList"][row_json]["contractorName"]
                    df_kiennghi_new['NGUOI_XULY'] = data["biduPetitionContractorDTOList"][row_json]["processUserInfo"]
                    df_kiennghi_new['STATUS'] = data["biduPetitionContractorDTOList"][row_json]["status"]
                    df_kiennghi_new['MA_TBMT'] = Ma_TBMT
                    df_kiennghi_new['ID_TBMT'] = ID_TBMT

                    df_kiennghi = pd.concat([df_kiennghi, df_kiennghi_new], ignore_index=True)


#EXPORT TO EXCEL    
writer = pd.ExcelWriter("export.xlsx")
df_ct_KHLCNT.to_excel(writer, sheet_name='KHLCNT', index=False)
df_GOITHAU.to_excel(writer, sheet_name='GOITHAU', index=False)
final_df_GOITHAU.to_excel(writer, sheet_name='GOITHAU_CHITIET', index=False)
df_giahan.to_excel(writer, sheet_name='GIAHAN', index=False)
df_lamro.to_excel(writer, sheet_name='LAMRO', index=False)
df_kiennghi.to_excel(writer, sheet_name='KIENNGHI', index=False)
df_phanLo.to_excel(writer, sheet_name='PHANLO_GOITHAU', index=False)
df_mothau.to_excel(writer, sheet_name='BIENBAN_MOTHAU', index=False)
df_DXKT.to_excel(writer, sheet_name='HSDXKT', index=False)
df_DXTC.to_excel(writer, sheet_name='HSDXTC', index=False)
df_KQLCNT.to_excel(writer, sheet_name='KQLCNT', index=False)
df_DanhMucHangHoa.to_excel(writer, sheet_name='DANHMUC_HANGHOA', index=False)
df_bidaInvChapterConfList.to_excel(writer, sheet_name='InvChapterConfList', index=False)
df_bidoInvBiddingDTO.to_excel(writer, sheet_name='InvBiddingDTO', index=False)
df_bidoPreNotifyContractorResult.to_excel(writer, sheet_name='PreNotifyContractorResult', index=False)
df_bidoPreBidConferenceList.to_excel(writer, sheet_name='PreBidConferenceList', index=False)
df_bidContractorShortlistMDTO.to_excel(writer, sheet_name='ContractorShortlistMDTO', index=False)
df_bidoNotifyContractorP.to_excel(writer, sheet_name='NotifyContractorP', index=False)
writer.close()

#UPLOAD TO GOOGLE SHEET
success, message = upload_dataframe_to_google_sheet("token-qldt.json","1lfkxn5sPh1lqTmyvUe3jSyXNXu2ZG0CSiMEuyId6TaE","2.1.KHLCNT",df_ct_KHLCNT,True)
if success == True:
    print(message)
else:
    print(f"Upload failed. Error message: {message}")
success, message = upload_dataframe_to_google_sheet("token-qldt.json","1lfkxn5sPh1lqTmyvUe3jSyXNXu2ZG0CSiMEuyId6TaE","2.1.GOI_THAU",df_GOITHAU,True)
if success == True:
    print(message)
else:
    print(f"Upload failed. Error message: {message}")
success, message = upload_dataframe_to_google_sheet("token-qldt.json","1lfkxn5sPh1lqTmyvUe3jSyXNXu2ZG0CSiMEuyId6TaE","2.1.GOI_THAU_CT",final_df_GOITHAU,True)
if success == True:
    print(message)
else:
    print(f"Upload failed. Error message: {message}")
success, message = upload_dataframe_to_google_sheet("token-qldt.json","1lfkxn5sPh1lqTmyvUe3jSyXNXu2ZG0CSiMEuyId6TaE","2.1.GIA_HAN",df_giahan,True)
if success == True:
    print(message)
else:
    print(f"Upload failed. Error message: {message}")
success, message = upload_dataframe_to_google_sheet("token-qldt.json","1lfkxn5sPh1lqTmyvUe3jSyXNXu2ZG0CSiMEuyId6TaE","2.1.LAM_RO",df_lamro,True)
if success == True:
    print(message)
else:
    print(f"Upload failed. Error message: {message}")
success, message = upload_dataframe_to_google_sheet("token-qldt.json","1lfkxn5sPh1lqTmyvUe3jSyXNXu2ZG0CSiMEuyId6TaE","2.1.KIEN_NGHI",df_kiennghi,True)
if success == True:
    print(message)
else:
    print(f"Upload failed. Error message: {message}")
success, message = upload_dataframe_to_google_sheet("token-qldt.json","1lfkxn5sPh1lqTmyvUe3jSyXNXu2ZG0CSiMEuyId6TaE","2.1.PHAN_LO",df_phanLo,True)
if success == True:
    print(message)
else:
    print(f"Upload failed. Error message: {message}")
success, message = upload_dataframe_to_google_sheet("token-qldt.json","1lfkxn5sPh1lqTmyvUe3jSyXNXu2ZG0CSiMEuyId6TaE","2.1.BIENBAN_MOTHAU",df_mothau,True)
if success == True:
    print(message)
else:
    print(f"Upload failed. Error message: {message}")
success, message = upload_dataframe_to_google_sheet("token-qldt.json","1lfkxn5sPh1lqTmyvUe3jSyXNXu2ZG0CSiMEuyId6TaE","2.1.HSDXKT",df_DXKT,True)
if success == True:
    print(message)
else:
    print(f"Upload failed. Error message: {message}")
success, message = upload_dataframe_to_google_sheet("token-qldt.json","1lfkxn5sPh1lqTmyvUe3jSyXNXu2ZG0CSiMEuyId6TaE","2.1.HSDXTC",df_DXTC,True)
if success == True:
    print(message)
else:
    print(f"Upload failed. Error message: {message}")
success, message = upload_dataframe_to_google_sheet("token-qldt.json","1lfkxn5sPh1lqTmyvUe3jSyXNXu2ZG0CSiMEuyId6TaE","2.1.KQLCNT",df_KQLCNT,True)
if success == True:
    print(message)
else:
    print(f"Upload failed. Error message: {message}")
success, message = upload_dataframe_to_google_sheet("token-qldt.json","1lfkxn5sPh1lqTmyvUe3jSyXNXu2ZG0CSiMEuyId6TaE","2.1.SHORTLIST",df_bidContractorShortlistMDTO,True)
if success == True:
    print(message)
else:
    print(f"Upload failed. Error message: {message}")

# Record the end time
end_time = time.time()
# Calculate the elapsed time in seconds
elapsed_time = end_time - start_time
# Convert seconds into hours, minutes, and seconds
hours, remainder = divmod(elapsed_time, 3600)
minutes, seconds = divmod(remainder, 60)
print(f"Tổng thời gian lấy dữ liệu: {int(hours)} hours, {int(minutes)} minutes, {int(seconds)} seconds")
