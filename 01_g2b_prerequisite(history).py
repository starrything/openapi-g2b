 # -*- coding: utf-8 -*-

import csv
import json
from urllib.request import urlopen
from urllib import parse
import datetime as dt
from dateutil.relativedelta import relativedelta
import pandas as pd
import numpy as np
import re

today = dt.date.today().strftime('%Y%m%d')
today = today + "0000"
month_before = (dt.datetime.now() - relativedelta(months=1) ).strftime('%Y%m%d') + "0000"
# print("Today's date : " + today)
inqryBgnDt = month_before
inqryEndDt = today

# 조달청_사전규격정보
'''
> parameter(조회조건)
- inqryDiv :조회구분(접수일시)
- inqryBgnDt : 조회시작일시
- inqryEndDt : 조회종료일시
- swBizObjYn : SW사업대상여부
- dminsttCd : 수요기관코드 (B553931: 해양환경공단)
> numOfRows 는 totalCount 보다 작아야 함
'''
url = 'http://apis.data.go.kr/1230000/HrcspSsstndrdInfoService/getPublicPrcureThngInfoServcPPSSrch'
queryParams = '?' + parse.urlencode({ parse.quote_plus('serviceKey') : 'B1CsUiO26Y56VDOKIParM6z394FXvTQC0rafsREBzSnOl8Cc1PUFY98LOcqKq5OahD5s2AhvszA2AIIYj0KXvg==', 
                                     parse.quote_plus('pageNo') : '1', 
                                     parse.quote_plus('numOfRows') : 900, 
                                     parse.quote_plus('type') : 'json' ,
                                     parse.quote_plus('inqryDiv') : '1', 
                                     parse.quote_plus('inqryBgnDt') : '202101010000', 
                                     parse.quote_plus('inqryEndDt') : '202112312359', 
                                     parse.quote_plus('swBizObjYn') : 'Y',
                                     parse.quote_plus('dminsttCd') : 'B553931'
                                    })

# print(url + queryParams)
# set_API & get_data -> openAPI & parameters
response = urlopen(url + queryParams)
data = response.read()
print(data)
JSON_object = json.loads(data.decode('utf-8'))
# print(JSON_object["response"]["body"]["items"][100])
# print(JSON_object["response"]["body"]["items"][100]["prdctClsfcNoNm"])

'''
for object in JSON_object["response"]["body"]["items"]:
    print(object)
    print("\n")
'''

# Total Count
print("total Count : " + str(JSON_object["response"]["body"]["totalCount"]))

# 정렬(내림차순) -> 등록일시(rgstDt)
def sortFunction(value):
	return value["rgstDt"]

sortedList = sorted(JSON_object["response"]["body"]["items"], key=sortFunction, reverse=True)
#for value in sortedList:
#    print(value)
#    print("\n")

'''
> Columns
- 사업명 : prdctClsfcNoNm
- 배정예산액 : asignBdgtAmt
- 공개일시(등록일시) : rgstDt
- 의견등록마감일시 : opninRgstClseDt
- 수요기관 : rlDminsttNm
- 납품기한일시 : dlvrTmlmtDt
- 납품일수 : dlvrDaynum
- 규격문서파일URL1/2 : specDocFileUrl1/specDocFileUrl2
'''

df = pd.DataFrame(sortedList, columns = ["prdctClsfcNoNm",
                                         "asignBdgtAmt",
                                         "rgstDt",
                                         "opninRgstClseDt",
                                         "rlDminsttNm",
                                         "dlvrTmlmtDt",
                                         "dlvrDaynum",
                                         "specDocFileUrl1",
                                         "specDocFileUrl2"])
#df

# filter_1 : 아직 공고되지 않은 건 ("의견등록마감일시" > 현재시간 ) opninRgstClseDt
valid = df['opninRgstClseDt'] < str(dt.datetime.now().strftime('%Y-%m-%d %H:%m:%S'))
valid_info = df[valid]

# filter_2 : 공고명 특정 문자 포함 여부
# filter_2 = df['prdctClsfcNoNm'].str.contains('음식|생물|녹색|청소|하수처리|통합관제')
#~filter_2
# valid_info = valid_info[~filter_2]

# 구분 컬럼 추가 -> 운영/구축
valid_info['class'] = np.where(valid_info['prdctClsfcNoNm'].str.contains('유지관리|유지보수|위탁운영|통합운영|운영관리'), '운영(ITO)', '구축(SI)')

valid_info.rename(columns = {"prdctClsfcNoNm": "품명(사업명)",
                             "asignBdgtAmt": "배정예산액",
                             "rgstDt": "공개일시(등록일시)",
                             "opninRgstClseDt": "의견등록 마감일시",
                             "rlDminsttNm": "수요기관",
                             "dlvrTmlmtDt": "납품기한일시",
                             "dlvrDaynum": "납품일수",
                             "specDocFileUrl1": "규격문서파일URL1",
                             "specDocFileUrl2": "규격문서파일URL2",
                             "class": "구분"}
                  ,inplace=True)

#valid_info

for idx, row in valid_info.iterrows():
    s_word=row["품명(사업명)"]
    
    s_word = re.sub('\d{4}년~\d{4}년', '', s_word)
    s_word = re.sub('\d{4}~\d{4}년도', '', s_word)
    s_word = re.sub('\d{4}-\d{4}년도', '', s_word)
    s_word = re.sub('\d{4}~\d{4}년', '', s_word)
    s_word = re.sub('\d{4}~\d{2}년', '', s_word)
    s_word = re.sub('\d{4}년도', '', s_word)
    s_word = re.sub('\d{4}년', '', s_word)
    s_word = re.sub('\d{4}', '', s_word)
    s_word = re.sub('\d{2}~\d{2}년', '', s_word)
    s_word = re.sub('\d{4}~\d{2}년', '', s_word)
    s_word = re.sub('\d{4}-\d{2}년', '', s_word)
    s_word = re.sub('\d{2} ~ \d{2}년도', '', s_word)
    s_word = re.sub('\d{2}년-\d{2}년', '', s_word)
    s_word = re.sub('\d{2}년', '', s_word)
    #print(idx, s_word, "||", row["공고명"])
    
    # 직전 3년 이력 조회 ----- begin
    for i in range(3):
        hstrYear = str(dt.date.today().year -i -1)
        schDateFr = hstrYear + "01010000"
        schDateTo = hstrYear + "12312359"
        
        url = 'http://apis.data.go.kr/1230000/ScsbidInfoService/getScsbidListSttusServcPPSSrch'
        queryParams = '?' + parse.urlencode({ parse.quote_plus('serviceKey') : 'B1CsUiO26Y56VDOKIParM6z394FXvTQC0rafsREBzSnOl8Cc1PUFY98LOcqKq5OahD5s2AhvszA2AIIYj0KXvg==', 
                                         parse.quote_plus('pageNo') : '1', 
                                         parse.quote_plus('numOfRows') : 10, 
                                         parse.quote_plus('type') : 'json' ,
                                         parse.quote_plus('inqryDiv') : '1', 
                                         parse.quote_plus('inqryBgnDt') : schDateFr, 
                                         parse.quote_plus('inqryEndDt') : schDateTo, 
                                         parse.quote_plus('intrntnlDivCd') : '1',
                                         parse.quote_plus('bidNtceNm') : s_word
                                    })
        response = urlopen(url + queryParams)
        data = response.read()
        JSON_object = json.loads(data.decode('utf-8'))
        #print(JSON_object)
        searchedCnt = JSON_object["response"]["body"]["totalCount"]
        
        # 이력이 있으면
        if searchedCnt > 0:
            history = pd.DataFrame(JSON_object["response"]["body"]["items"], columns = ["bidNtceNo",
                                         "bidNtceNm",
                                         "bidwinnrNm",
                                         "bidwinnrBizno",
                                         "bidwinnrCeoNm",
                                         "sucsfbidAmt"])
            # 이력 추가
            for index, item in history.iterrows():
                valid_info.loc[idx, hstrYear + "(공고번호/공고명/낙찰업체/사업자번호/대표자명/입찰금액)"] = str(item['bidNtceNo']) + " / " + item['bidNtceNm'] + " / " + item['bidwinnrNm'] + " / " + str(item['bidwinnrBizno']) + " / " + item['bidwinnrCeoNm'] + " / " + str(item['sucsfbidAmt']) + " / "
    
    # 직전 3년 이력 조회 ----- end
    
#valid_info
# export to csv file
#df.to_csv("/home/jonghyun/workspace/open_api/g2b_in_advance.csv", header=True, index=True)
valid_info.to_csv("./g2b_in_advance(" + str(dt.date.today()) + ").csv", header=True, index=True)

