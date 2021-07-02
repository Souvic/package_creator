import json
import multiprocessing
import os
import requests

from requests_oauthlib import OAuth1
from time import sleep

import tweepy

def get_users(x,auth,output_folder):
    
    while(True):
        url=f"https://api.twitter.com/1.1/users/lookup.json?user_id={','.join([str(i) for i in x])}"
        if(type(auth)==str):
            headers = {"Authorization": "Bearer "+auth}
            r = requests.get(url = url,headers=headers)
        else:
            r = requests.get(url = url, auth=auth)
        if(r.status_code != 200):
            print("sleeping")
            url="https://api.twitter.com/1.1/application/rate_limit_status.json?resources=help,users,search,statuses"
            while(True):
                sleep(30)
                try:
                    if(type(auth)==str):
                        headers = {"Authorization": "Bearer "+auth}
                        l = requests.get(url = url,headers=headers).json()
                    else:
                        l = requests.get(url = url, auth=auth).json()
                    if(l["resources"]["users"]["/users/lookup"]["remaining"]!=0):
                        break;
                except:
                    pass;
            continue;
        else:
            l = r.json()
            return(l)
            break;
def get_users_mp_aux(x,index,auths,output_folder):
    n=100
    auth=auths[index]
    with open(f'{output_folder}/{index}.jsonl', 'w') as outfile:
        for i in range(0,len(x),n):
            json1=get_users(x[i:i+n],auth,output_folder)
            json.dump(json1, outfile)
            outfile.write('\n')
        
        





def get_users_mp(auths,user_ids,output_folder):
    if(not os.path.isdir(output_folder)):
        print(f"Not a directory: {output_folder}")
        return(None)
    if(len(auths)==0):
        return(None)
    if(type(auths[0])!=str):
        auths=[OAuth1(auths[i][0],auths[i][1],auths[i][2],auths[i][3]) for i in range(len(auths))]
    Process_jobs = []
    k=len(auths)
    n=(1+len(user_ids)//k)
    index=0
    for i in range(0,len(user_ids),n):
        p = multiprocessing.Process(target = get_users_mp_aux, args = (user_ids[i:i+n],index,auths,output_folder))
        index+=1
        
        Process_jobs.append(p)
        p.start()
    for p in Process_jobs:
        p.join()
        

def get_timeline(auth,user_id=None,screen_name=None,count=200,trim_user=True,exclude_replies=False,include_rts=True,max_id=None):
    l=[1]
    ans=[]
    while(len(l)!=0):
        if(user_id is not None):
            url=f"https://api.twitter.com/1.1/statuses/user_timeline.json?user_id={user_id}&count={count}&trim_user={trim_user}&exclude_replies={exclude_replies}&include_rts={include_rts}"
        else:
            url=f"https://api.twitter.com/1.1/statuses/user_timeline.json?screen_name={screen_name}&count={count}&trim_user={trim_user}&exclude_replies={exclude_replies}&include_rts={include_rts}"
        url+="&tweet_mode=extended"
        if(max_id is not None):
            #print(max_id,"here")
            url+=f"&max_id={max_id}"
        #r = requests.get(url = url, auth=auth)
        if(type(auth)==str):
            headers = {"Authorization": "Bearer "+auth}
            r = requests.get(url = url,headers=headers)
        else:
            r = requests.get(url = url, auth=auth)
        #print(url)
        if(r.status_code == 401):
            break;
        if(r.status_code != 200):
            print("sleeping")
            url="https://api.twitter.com/1.1/application/rate_limit_status.json?resources=help,users,search,statuses"
            while(True):
                sleep(30)
                
                try:
                    if(type(auth)==str):
                        l=requests.get(url = url,headers=headers).json()
                    else:
                        l=requests.get(url = url, auth=auth).json()
                    if(l["resources"]["statuses"]["/statuses/user_timeline"]["remaining"]!=0):
                        break;
                except Exception as e:
                    print(e)
                    
                    pass;
            continue;
        else:
            l = r.json()
            ans.extend(l)
            if(len(l)==0 or max_id==l[-1]["id_str"]):
                break;
            else:
                max_id=l[-1]["id_str"]
    return(ans)
            
            



def get_timeline_mp_aux(index,auths,users,output_folder):
    auth=auths[index]
    with open(f'{output_folder}/{index}.jsonl', 'w') as outfile:
        for user_id in users:
            json1=get_timeline(auth=auth,user_id=user_id)
            json.dump(json1, outfile)
            outfile.write('\n')
        
        
def get_timeline_mp(auths,users,output_folder):
    if(not os.path.isdir(output_folder)):
        print(f"Not a directory: {output_folder}")
        return(None)
    if(len(auths)==0):
        return(None)
    if(type(auths[0])!=str):
        auths=[OAuth1(auths[i][0],auths[i][1],auths[i][2],auths[i][3]) for i in range(len(auths))]
    Process_jobs = []
    k=len(auths)
    n=(1+len(users)//k)
    index=-1
    for i in range(0,len(users),n):
        
        p = multiprocessing.Process(target = get_timeline_mp_aux, args = (index,auths,users[i:i+n],output_folder))
        index+=1
        Process_jobs.append(p)
        p.start()
    for p in Process_jobs:
        p.join()


def get_followers_aux(auth,screen_name,cursor=-1):
    url="https://api.twitter.com/1.1/followers/ids.json"
    params={"screen_name":screen_name,"count":"5000","cursor":cursor}
    
    
    if(type(auth)==str):
        headers = {"Authorization": "Bearer "+auth}
        temp=requests.get(url=url,headers=headers,params=params).json()
    else:
        temp=requests.get(url=url,auth=auth,params=params).json()
    

    if(len(temp["ids"])==0):
        return(temp["ids"],None)
    else:
        return(temp["ids"],temp["next_cursor"])
        
def get_followers(auths,screen_name,max_num=10**9):
    cursor=-1
    if(len(auths)==0):
        return(None)
    if(type(auths[0])!=str):
        auths=[OAuth1(auths[i][0],auths[i][1],auths[i][2],auths[i][3]) for i in range(len(auths))]
    res=[]
    index=0
    auth=auths[index]
    flag=False
    while(cursor is not None and max_num>len(res)):
            try:
                a,cursor=get_followers_aux(auth,screen_name,cursor)
                flag=False
                res.extend(a)
                print(len(res))
            except Exception as e:
                print(e)
                print("done",len(res))
                if(flag):
                    sleep(30)
                else:
                    flag=True
                    index+=1
                    index%=len(auths)
                    auth=auths[index]
                pass;
    return(res)
            


