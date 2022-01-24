'''
사업자등록번호로 업종 확인
'''
import csv
import json
from urllib.request import urlopen
from urllib import parse
import datetime as dt
import pandas as pd
import numpy as np
import re

# 대신정보통신 4088118945
# 대보정보통신 1358119406
bizNo = '2118108009'
# 조달청_사용자정보서비스
'''
> parameter(조회조건)
- bizno : 사업자등록번호

> numOfRows 는 totalCount 보다 작아야 함
'''
url = 'http://apis.data.go.kr/1230000/UsrInfoService/getPrcrmntCorpIndstrytyInfo'
queryParams = '?' + parse.urlencode({ parse.quote_plus('serviceKey') : 'B1CsUiO26Y56VDOKIParM6z394FXvTQC0rafsREBzSnOl8Cc1PUFY98LOcqKq5OahD5s2AhvszA2AIIYj0KXvg==', 
                                     parse.quote_plus('pageNo') : '1', 
                                     parse.quote_plus('numOfRows') : 100, 
                                     parse.quote_plus('type') : 'json' ,
                                     parse.quote_plus('bizno') : bizNo
                                    })

# set_API & get_data -> openAPI & parameters
response = urlopen(url + queryParams)
data = response.read()
JSON_object = json.loads(data.decode('utf-8'))

'''
"bizno": "1048118820",
          "indstrytyNm": "엔지니어링사업(프로젝트매니지먼트)",
          "indstrytyCd": "7309",
          "rgstDt": "2011-10-31 00:00:00",
          "vldPrdExprtDt": "",
          "systmRgstDt": "2014-06-30 16:12:45",
          "chgDt": "",
          "indstrytyStatsNm": "",
          "rprsntIndstrytyYn": "N",
          "systmChgDt": "2014-06-30 16:12:45"
'''
result = pd.DataFrame(JSON_object["response"]["body"]["items"], columns = ["bizno",
                                         "indstrytyNm",
                                         "rgstDt",
                                         "vldPrdExprtDt"])

#result


# init Series
s = []
    
for index, item in result.iterrows():
    if len(item['vldPrdExprtDt']) > 0:
        print(item['vldPrdExprtDt'] + ' -> ' + str(len(item['vldPrdExprtDt'])))
        print(item['indstrytyNm'])
        print(re.sub('\(.*?\)', '', item['indstrytyNm']))
        print('\n')
        s.append(re.sub('\(.*?\)', '', item['indstrytyNm']))

#s

# using naive method 
# to remove duplicated  
# from list  
res = [] 
for i in s: 
    if i not in res: 
        res.append(i)

#res
# 
print('-'.join(res))