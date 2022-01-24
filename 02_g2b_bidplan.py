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

# 조달청_입찰공고정보
'''
> parameter(조회조건)
- inqryDiv :조회구분(접수일시)
- inqryBgnDt : 조회시작일시
- inqryEndDt : 조회종료일시
- intrntnlDivCd : 1(국내)
- bidClseExcpYn : Y(입찰마감건 제외)
'''
url = 'http://apis.data.go.kr/1230000/BidPublicInfoService02/getBidPblancListInfoServcPPSSrch'
queryParams = '?' + parse.urlencode({ parse.quote_plus('serviceKey') : 'B1CsUiO26Y56VDOKIParM6z394FXvTQC0rafsREBzSnOl8Cc1PUFY98LOcqKq5OahD5s2AhvszA2AIIYj0KXvg==', 
                                     parse.quote_plus('pageNo') : '1', 
                                     parse.quote_plus('numOfRows') : 900, 
                                     parse.quote_plus('type') : 'json' ,
                                     parse.quote_plus('inqryDiv') : '1', 
                                     parse.quote_plus('inqryBgnDt') : inqryBgnDt, 
                                     parse.quote_plus('inqryEndDt') : inqryEndDt,
                                     parse.quote_plus('intrntnlDivCd') : '1',
                                     parse.quote_plus('bidClseExcpYn') : 'Y',
                                     parse.quote_plus('indstrytyNm') : '소프트웨어'
                                    })

# print(url + queryParams)
# set_API & get_data -> openAPI & parameters
response = urlopen(url + queryParams)
data = response.read()
print(data)
JSON_object = json.loads(data.decode('utf-8'))

'''
for object in JSON_object["response"]["body"]["items"]:
    print(object)
    print("\n")
'''

# Total Count
print("total Count : " + str(JSON_object["response"]["body"]["totalCount"]))

# 정렬(내림차순) -> 등록일시(rgstDt)
def sortFunction(value):
	return value["bidNtceDt"]

sortedList = sorted(JSON_object["response"]["body"]["items"], key=sortFunction, reverse=True)
#for value in sortedList:
#    print(value)
#    print("\n")

'''
> Columns
- 공고일시 : bidNtceDt
- 공고번호 : bidNtceNo
- 공고명 : bidNtceNm
- 수요기관 : dminsttNm
- 계약체결방법명 : cntrctCnclsMthdNm
- 입찰방식명 : bidMethdNm
- 낙찰방법명 : sucsfbidMthdNm
- 입찰마감일시 : bidClseDt
- 개찰일시 : opengDt
- 입찰공고 상세(URL) : bidNtceUrl
- 예가방법 : prearngPrceDcsnMthdNm
- 총예가건수 : totPrdprcNum
- 추첨예가건수 : drwtPrdprcNum
- 배정예산 : asignBdgtAmt
- 추정가격 : presmptPrce
'''

df = pd.DataFrame(sortedList, columns = ["bidNtceDt",
                                         "bidNtceNo",
                                         "bidNtceNm",
                                         "dminsttNm",
                                         "cntrctCnclsMthdNm",
                                         "bidMethdNm",
                                         "sucsfbidMthdNm",
                                         "bidClseDt",
                                         "opengDt",
                                         "bidNtceUrl",
                                         "prearngPrceDcsnMthdNm",
                                         "totPrdprcNum",
                                         "drwtPrdprcNum",
                                         "asignBdgtAmt",
                                         "presmptPrce"])

# filter_1 : 입찰참여 가능한 공고 ("입찰마감일시" > 현재시간 )
valid = df['bidClseDt'] > str(dt.datetime.now().strftime('%Y-%m-%d %H:%m:%S'))
valid_bid = df[valid]

# filter_2 : 공고명 특정 문자 포함 여부
#filter_2 = df['bidNtceNm'].str.contains('음식|생물|녹색|청소|하수처리')
#~filter_2
#valid_bid = valid_bid[~filter_2]

# 구분 컬럼 추가 -> 운영/구축
valid_bid['class'] = np.where(valid_bid['bidNtceNm'].str.contains('유지관리|유지보수|위탁운영|통합운영|운영관리'), '운영(ITO)', '구축(SI)')

valid_bid.rename(columns = {"bidNtceDt": "공고일시",
                            "bidNtceNo": "공고번호",
                            "bidNtceNm": "공고명",
                            "dminsttNm": "수요기관",
                            "cntrctCnclsMthdNm": "계약체결방법명",
                            "bidMethdNm": "입찰방식명",
                            "sucsfbidMthdNm": "낙찰방법명",
                            "bidClseDt": "입찰마감일시",
                            "opengDt": "개찰일시",
                            "bidNtceUrl": "입찰공고 상세(URL)",
                            "prearngPrceDcsnMthdNm": "예가방법",
                            "totPrdprcNum": "총예가건수",
                            "drwtPrdprcNum": "추첨예가건수",
                            "asignBdgtAmt": "배정예산",
                            "presmptPrce": "추정가격",
                            "class": "구분"}
                 ,inplace=True)

valid_bid

for idx, row in valid_bid.iterrows():
    s_word=row["공고명"]
    
    s_word = re.sub('\d{4}년~\d{4}년', '', s_word)
    s_word = re.sub('\d{4}~\d{4}년도', '', s_word)
    s_word = re.sub('\d{4}-\d{4}년도', '', s_word)
    s_word = re.sub('\d{4}-\d{4}', '', s_word)
    s_word = re.sub('\d{4}~\d{4}년', '', s_word)
    s_word = re.sub('\d{4}~\d{2}년', '', s_word)
    s_word = re.sub('\d{4}년도', '', s_word)
    s_word = re.sub('(\d{4}년)', '', s_word)
    s_word = re.sub('\d{4}년', '', s_word)
    s_word = re.sub('\d{4}', '', s_word)
    s_word = re.sub('\d{2}~\d{2}년', '', s_word)
    s_word = re.sub('\d{4}~\d{2}년', '', s_word)
    s_word = re.sub('\d{4}-\d{2}년', '', s_word)
    s_word = re.sub('\d{2} ~ \d{2}년도', '', s_word)
    s_word = re.sub('\d{2}년-\d{2}년', '', s_word)
    s_word = re.sub('\'\d{2}년도', '', s_word)
    s_word = re.sub('\d{2}년', '', s_word)
    #print(idx, s_word, "||", row["공고명"])
    
    if row["구분"] == '운영(ITO)':
        # 직전 3년 이력 조회(운영) ----- begin
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
                    valid_bid.loc[idx, hstrYear + "(공고번호/공고명/낙찰업체/사업자번호/대표자명/입찰금액)"] = str(item['bidNtceNo']) + " / " + item['bidNtceNm'] + " / " + item['bidwinnrNm'] + " / " + str(item['bidwinnrBizno']) + " / " + item['bidwinnrCeoNm'] + " / " + str(item['sucsfbidAmt']) + " / "
        
        # 직전 3년 이력 조회 ----- end
    else:
        '''
        # 구축 또는 개발 사업
        
        # 직전 3년 이력 조회(구축) ----- begin
        for i in range(3):
            hstrYear = str(dt.date.today().year -i -1)
            schDateFr = hstrYear + "01010000"
            schDateTo = hstrYear + "12312359"
            
            url = 'http://apis.data.go.kr/1230000/BidPublicInfoService/getBidPblancListInfoServcPPSSrch'
            queryParams = '?' + parse.urlencode({ parse.quote_plus('serviceKey') : 'B1CsUiO26Y56VDOKIParM6z394FXvTQC0rafsREBzSnOl8Cc1PUFY98LOcqKq5OahD5s2AhvszA2AIIYj0KXvg==', 
                                                 parse.quote_plus('pageNo') : '1', 
                                                 parse.quote_plus('numOfRows') : 10, 
                                                 parse.quote_plus('type') : 'json' ,
                                                 parse.quote_plus('inqryDiv') : '1', 
                                                 parse.quote_plus('inqryBgnDt') : schDateFr, 
                                                 parse.quote_plus('inqryEndDt') : schDateTo, 
                                                 parse.quote_plus('intrntnlDivCd') : '1',
                                                 parse.quote_plus('bidNtceNm') : '구축',
                                                 parse.quote_plus('dminsttNm') : row["수요기관"]
                                    })
            response = urlopen(url + queryParams)
            data = response.read()
            JSON_object = json.loads(data.decode('utf-8'))
            #print(JSON_object)
            searchedCnt = JSON_object["response"]["body"]["totalCount"]
            
            # 이력이 있으면
            if searchedCnt > 0:
                historyBid = pd.DataFrame(JSON_object["response"]["body"]["items"], columns = ["bidNtceNo"])
                
                # 이력 추가
                for index, item in historyBid.iterrows():
                    historyBidNo = item['bidNtceNo']
                    
                    url = 'http://apis.data.go.kr/1230000/ScsbidInfoService/getScsbidListSttusServcPPSSrch'
                    queryParams = '?' + parse.urlencode({ parse.quote_p
                    lus('serviceKey') : 'B1CsUiO26Y56VDOKIParM6z394FXvTQC0rafsREBzSnOl8Cc1PUFY98LOcqKq5OahD5s2AhvszA2AIIYj0KXvg==', 
                                                 parse.quote_plus('pageNo') : '1', 
                                                 parse.quote_plus('numOfRows') : 10, 
                                                 parse.quote_plus('type') : 'json' ,
                                                 parse.quote_plus('inqryDiv') : '1', 
                                                 parse.quote_plus('inqryBgnDt') : schDateFr, 
                                                 parse.quote_plus('inqryEndDt') : schDateTo, 
                                                 parse.quote_plus('intrntnlDivCd') : '1',
                                                 parse.quote_plus('bidNtceNo') : historyBidNo
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
                            valid_bid.loc[idx, hstrYear + "winnr"] = str(item['bidNtceNo']) + " / " + item['bidNtceNm'] + " / " + item['bidwinnrNm'] + " / " + str(item['bidwinnrBizno']) + " / " + item['bidwinnrCeoNm'] + " / " + str(item['sucsfbidAmt']) + " EoL "
        
        # 직전 3년 이력 조회 ----- end
    '''
    
#valid_bid   

# export to csv file
# df.to_csv("/home/jonghyun/workspace/open_api/g2b_bid_plan.csv", header=True, index=True)
valid_bid.to_csv("./g2b_bid_plan_software("+ str(dt.date.today()) +").csv", header=True, index=True)
