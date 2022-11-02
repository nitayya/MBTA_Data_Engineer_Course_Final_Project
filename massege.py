import json
import sys
import random
import requests
from pyhive import hive

#Hive Settings============================================================================#
hive_cnx = hive.Connection(host='Cnt7-naya-cdh63',
                           port=10000,
                           username='hdfs',
                           password="naya",
                           database="default",
                           auth='CUSTOM')

#Query Hive================================================================================#
cursor1 = hive_cnx.cursor()
cursor1.execute('show databases')
data = cursor1.fetchall()
cursor1.close()

# write the plain text part
text1 = """
Hi dor :)
you have database in hive named : \n
"""
# result1 = str(result1) + str(df_data) + '\n'
gg = str(data).\
    replace("[", "").replace("(", "").\
    replace("'", "").replace("]","").replace(")","").replace(",","")
massege_to_Dor=text1+gg
# print(text1+gg)



if __name__ == '__main__':
    url = " https://hooks.slack.com/services/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"
    message = (massege_to_Dor)
    title = ('daily news')
    slack_data = {
        "username": "naya",
        "icon_emoji": ":satellite:",
        #"channel" : "#somerandomcahnnel",
        "attachments": [
            {
                "color": "#9733EE",
                "fields": [
                    {
                        "title": title,
                        "value": message,
                        "short": "false",
                    }
                ]
            }
        ]
    }
    byte_length = str(sys.getsizeof(slack_data))
    headers = {'Content-Type': "application/json", 'Content-Length': byte_length}
    response = requests.post(url, data=json.dumps(slack_data), headers=headers)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)