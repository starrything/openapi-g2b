import csv
import json
from urllib.request import urlopen
from urllib import parse
import datetime as dt
import pandas as pd
import numpy as np
import re

today = dt.date.today().strftime('%Y%m%d')
today = today + "0000"
# print("Today's date : " + today)
inqryBgnDt = '201701010000'
inqryEndDt = '201712312359'

print('worker start time : ' + str(dt.datetime.now().strftime('%Y-%m-%d %H:%m:%S')))
# 조달청_낙찰정보
'''
> parameter(조회조건)
- inqryDiv :조회구분(접수일시)
- inqryBgnDt : 조회시작일시
- inqryEndDt : 조회종료일시

> numOfRows 는 totalCount 보다 작아야 함
'''
url = 'http://apis.data.go.kr/1230000/ScsbidInfoService/getScsbidListSttusServcPPSSrch'
queryParams = '?' + parse.urlencode({ parse.quote_plus('serviceKey') : 'B1CsUiO26Y56VDOKIParM6z394FXvTQC0rafsREBzSnOl8Cc1PUFY98LOcqKq5OahD5s2AhvszA2AIIYj0KXvg==', 
                                     parse.quote_plus('pageNo') : '1', 
                                     parse.quote_plus('numOfRows') : 900, 
                                     parse.quote_plus('type') : 'json' ,
                                     parse.quote_plus('inqryDiv') : '1',
                                     parse.quote_plus('inqryBgnDt') : inqryBgnDt, 
                                     parse.quote_plus('inqryEndDt') : inqryEndDt, 
                                     parse.quote_plus('intrntnlDivCd') : '1'
                                    })

# set_API & get_data -> openAPI & parameters
response = urlopen(url + queryParams)
data = response.read()
JSON_object = json.loads(data.decode('utf-8'))

# merged DataFrames
# set data of page_1
result = pd.DataFrame(JSON_object["response"]["body"]["items"], columns = ["bidNtceNo",
                                         "bidNtceNm",
                                         "bidwinnrNm",
                                         "bidwinnrBizno",
                                         "bidwinnrCeoNm",
                                         "sucsfbidAmt"])
def get_bid_result(pageNo):
    url = 'http://apis.data.go.kr/1230000/ScsbidInfoService/getScsbidListSttusServcPPSSrch'
    queryParams = '?' + parse.urlencode({ parse.quote_plus('serviceKey') : 'B1CsUiO26Y56VDOKIParM6z394FXvTQC0rafsREBzSnOl8Cc1PUFY98LOcqKq5OahD5s2AhvszA2AIIYj0KXvg==', 
                                         parse.quote_plus('pageNo') : pageNo, 
                                         parse.quote_plus('numOfRows') : 900, 
                                         parse.quote_plus('type') : 'json' ,
                                         parse.quote_plus('inqryDiv') : '1', 
                                         parse.quote_plus('inqryBgnDt') : inqryBgnDt, 
                                         parse.quote_plus('inqryEndDt') : inqryEndDt, 
                                         parse.quote_plus('intrntnlDivCd') : '1'
                                    })
    response = urlopen(url + queryParams)
    data = response.read()
    jo = json.loads(data.decode('utf-8'))
    
    # return json obejct
    return pd.DataFrame(jo["response"]["body"]["items"], columns = ["bidNtceNo",
                                         "bidNtceNm",
                                         "bidwinnrNm",
                                         "bidwinnrBizno",
                                         "bidwinnrCeoNm",
                                         "sucsfbidAmt"])

# Total Count
print("total Count : " + str(JSON_object["response"]["body"]["totalCount"]))
totalCount = JSON_object["response"]["body"]["totalCount"]

# set pages
pages = []
for i in range(round(totalCount/900)):
    pages.append(i+1)

#print(pages)
for i in pages:
    if i != 1:
        print('page number : ' + str(i))
        # add pages
        result = pd.concat([result, get_bid_result(i+1)])


# 정렬(내림차순) -> 입찰번호(bidNtceNo)
def sortFunction(value):
	return value["bidNtceNo"]

# sortedList = sorted(JSON_object["response"]["body"]["items"], key=sortFunction, reverse=True)

'''
> Columns
- 입찰공고번호 : bidNtceNo
- 공고명 : bidNtceNm
- 업체명 : bidwinnrNm
- 사업자등록번호 : bidwinnrBizno
- 대표자명 : bidwinnrCeoNm
- 입찰금액 : sucsfbidAmt
'''

df = pd.DataFrame(result, columns = ["bidNtceNo",
                                         "bidNtceNm",
                                         "bidwinnrNm",
                                         "bidwinnrBizno",
                                         "bidwinnrCeoNm",
                                         "sucsfbidAmt"])

# filter_1 : 아직 공고되지 않은 건 ("의견등록마감일시" > 현재시간 )
valid = df['bidwinnrNm'].str.contains('서진테크놀로지|클라루소')
valid_info = df[valid]

# filter_1 : 아직 공고되지 않은 건 ("의견등록마감일시" > 현재시간 )
#valid = df['opninRgstClseDt'] > str(dt.datetime.now())
#valid_info = df[valid]

# filter_2 : 공고명 특정 문자 포함 여부
#filter_2 = df['prdctClsfcNoNm'].str.contains('음식|생물|녹색|청소|하수처리|통합관제')
#~filter_2
#valid_info = df[~filter_2]

# 구분 컬럼 추가 -> 운영/구축
#valid_info['class'] = np.where(valid_info['prdctClsfcNoNm'].str.contains('유지관리|유지보수|위탁운영'), '운영(ITO)', '구축(SI)')

# 중복제거
valid_info = valid_info.drop_duplicates(subset=['bidNtceNo'])


# add IndstrNm by bizNo - 2020.11.12
for idx, row in df.iterrows():
    bizNo = row['bidwinnrBizno']
    # 조달청_사용자정보서비스
    '''
    > parameter(조회조건)
    - bizno : 사업자등록번호
    
    > numOfRows 는 totalCount 보다 작아야 함'''
    
    url = 'http://apis.data.go.kr/1230000/UsrInfoService/getPrcrmntCorpIndstrytyInfo'
    queryParams = '?' + parse.urlencode({ parse.quote_plus('serviceKey') : 'B1CsUiO26Y56VDOKIParM6z394FXvTQC0rafsREBzSnOl8Cc1PUFY98LOcqKq5OahD5s2AhvszA2AIIYj0KXvg==', 
                                         parse.quote_plus('pageNo') : '1', 
                                         parse.quote_plus('numOfRows') : 100, 
                                         parse.quote_plus('type') : 'json' ,
                                         parse.quote_plus('bizno') : bizNo
                                        })
    
    try:
        # set_API & get_data -> openAPI & parameters
        response = urlopen(url + queryParams)
        data = response.read()
        JSON_object = json.loads(data.decode('utf-8'))
        result = pd.DataFrame(JSON_object["response"]["body"]["items"], 
                              columns = ["bizno",
                                         "indstrytyNm",
                                         "rgstDt",
                                         "vldPrdExprtDt"])
        # init Series
        s = []
        for index, item in result.iterrows():
            if len(item['vldPrdExprtDt']) > 0:
                s.append(re.sub('\(.*?\)', '', item['indstrytyNm']))
        # to remove duplicated
        res = []
        for i in s: 
            if i not in res: 
                res.append(i)
        # add Column
        valid_info.loc[index, 'indstryNm'] = '-'.join(res)
    except:
        print('Warning!!! - userService')
        print(data)
        print('\n')


# rename Column headers
valid_info.rename(columns = {"bidNtceNo": "입찰공고번호",
                     "bidNtceNm": "공고명",
                     "bidwinnrNm": "업체명",
                     "bidwinnrBizno": "사업자등록번호",
                     "bidwinnrCeoNm": "대표자명",
                     "sucsfbidAmt": "입찰금액",
                    "indstryNm": "업종"}
                  ,inplace=True)

#df

# export to csv file
# df.to_csv("/home/jonghyun/workspace/open_api/g2b_in_advance.csv", header=True, index=True)
valid_info.to_csv("./g2b_bid_winnr(2019).csv", header=True, index=True)

print('worker end time : ' + str(dt.datetime.now().strftime('%Y-%m-%d %H:%m:%S')))