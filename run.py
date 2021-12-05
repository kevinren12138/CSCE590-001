import methods as mthd
import auth
import json, pandas as pd, time, csv
from os.path import exists


#Creating header/labels for csv 
tweetFile = mthd.tweetFieldsFilename
placeFile = mthd.placeFieldsFilename
userFile = mthd.userFieldsFilename
mediaFile = mthd.mediaFieldsFilename
pollFile = mthd.pollFieldsFilename


max_results = 500
total_tweets = 100


bearer_token = auth.auth()
headers = mthd.create_headers(bearer_token)
english = " lang:en" 


#Comment this if overwriting existing files
if exists(tweetFile) == False:
    csvFile = open(tweetFile, "a", newline="", encoding='utf-8')
    writer = csv.DictWriter(csvFile, mthd.tweetFieldNames)
    writer.writeheader()
    csvFile.close()

if exists(placeFile) == False:
    csvFile = open(placeFile, "a", newline="", encoding='utf-8')
    writer = csv.DictWriter(csvFile, mthd.placeFieldNames)
    writer.writeheader()
    csvFile.close()

if exists(userFile) == False:
    csvFile = open(userFile, "a", newline="", encoding='utf-8')
    writer = csv.DictWriter(csvFile, mthd.userFieldNames)
    writer.writeheader()
    csvFile.close()

if exists(mediaFile) == False:
    csvFile = open(mediaFile, "a", newline="", encoding='utf-8')
    writer = csv.DictWriter(csvFile, mthd.mediaFieldNames)
    writer.writeheader()
    csvFile.close()

if exists(pollFile) == False:
    csvFile = open(pollFile, "a", newline="", encoding='utf-8')
    writer = csv.DictWriter(csvFile, mthd.pollFieldNames)
    writer.writeheader()
    csvFile.close()


def search(keyword, start_list, end_list, max_results, headers, bearer_token):
    global total_tweets
    for i in range(0,len(start_list)):
        # Inputs
        count = 0 # Counting tweets per time period
        max_count = 10000000 # Max tweets per time period
        flag = True
        next_token = None
        # Check if flag is true
        while flag:
            # Check if max_count reached
            if count >= max_count:
                break
            print("-------------------")
            print("Token: ", next_token)
            url = mthd.create_url(keyword, start_list[i],end_list[i], max_results)
            json_response = mthd.connect_to_endpoint(url[0], headers, url[1], next_token)
            with open('data.json', 'w', encoding='utf-8') as f:
                json.dump(json_response, f, ensure_ascii=False, indent=4)
            result_count = json_response['meta']['result_count']
            if 'next_token' in json_response['meta']:
                # Save the token to use for next call
                next_token = json_response['meta']['next_token']
                print("Next Token: ", next_token)
                if result_count is not None and result_count > 0 and next_token is not None:
                    print("Start Date: ", start_list[i])
                    mthd.append_to_csv(json_response)
                    count += result_count
                    total_tweets += result_count
                    print("Total # of Tweets added: ", total_tweets)
                    print("-------------------")             
            # If no next token exists
            else:
                time.sleep(5)
                if result_count is not None and result_count > 0:
                    print("-------------------")
                    print("Start Date: ", start_list[i])
                    mthd.append_to_csv(json_response)
                    count += result_count
                    total_tweets += result_count
                    print("Total # of Tweets added: ", total_tweets)
                    print("-------------------") 
                #Since this is the final request, turn flag to false to move to the next time period.
                flag = False
                next_token = None
    print("Total number of results: ", total_tweets)


start_list = ['2020-12-01T00:00:00.000Z']
end_list = ['2021-08-30T00:00:00.000Z']


from keywords.keywords import main_keywords as mk

for primary in mk:
    print("keyword search:",primary)
    time.sleep(5)
    search(primary, start_list, end_list, max_results, headers, bearer_token)
    print('***************************************')
