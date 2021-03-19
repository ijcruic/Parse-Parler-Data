# -*- coding: utf-8 -*-
"""
Created on Tue Feb  2 13:04:21 2021

@author: icruicks
"""
import re, pandas as pd
from bs4 import BeautifulSoup
from zipfile import ZipFile

def parse_parler_archive(zipped_files):
    with ZipFile(zipped_files, 'r') as f:
        file_list = f.namelist()
        for i in range(len(file_list)):
            post = BeautifulSoup(f.read(file_list[i]), 'html.parser')
            
            parsed_post = {}
            parsed_post['id'] = file_list[i]
            try:
                '''
                Check for a bad scrape
                '''
                parsed_post['author_user_name'] = post.find_all("span", {'class': "author--username"})[0].string
                parsed_post['author_name'] = post.find_all("span", {'class': "author--name"})[0].string
            except:
                continue
            
            parsed_post['timestamp'] = post.find_all("span", {'class': "post--timestamp"})[0].string
            
            echoed = post.find_all("div", {'class': "eb--col eb--statement"})
            if len(echoed) >0:
                parsed_post['echoed_by_author'] = list(echoed[0].children)[1].string[10:]
                parsed_post['echoed_by_author_user_name'] = post.title.get_text().split()[0]
                echoed_time = post.find_all("div", {'class': "eb--col eb--timestamp"})
                parsed_post['when_echoed'] = list(echoed_time[0].children)[1].string
                echo_comment = post.find("span", {'class': "reblock post show-under-echo"})
                if echo_comment is not None:
                    parsed_post['echo_comment'] = echo_comment.find("div", {"class": 'card--body'}).get_text().strip()
            else:
                parsed_post['echoed_by_author'] =None
                parsed_post['echoed_by_author_user_name'] = None
                parsed_post['when_echoed'] =None
                
            parsed_post['impressions_count'] = int(post.find_all("span", {'class': "impressions--count"})[0].string)
            main_body = post.find("div", {'class': "card--body"})
            parsed_post['text'] = main_body.p.get_text()
            parsed_post['hashtags'] = re.findall("#(\w+)", parsed_post['text'])
            parsed_post['mentions'] = re.findall("@([a-zA-Z0-9]{1,15})", parsed_post['text'])
            
            external_urls =[]
            externals = post.find_all("span", {'class': "mc-article--link"})
            externals = externals + post.find_all("span", {'class': "mc-iframe-embed--link"})
            externals = externals + post.find_all("span", {'class': "mc-website--link"})
            if len(externals) > 0:
                for url in externals:
                    external_urls.append(list(url.a.children)[-1].strip())
            parsed_post['external_urls'] = external_urls
                
            image_urls =[]
            images = post.find_all("div", {'class': "mc-image--wrapper"})
            if len(images) > 0:
                for image in images:
                    image_urls.append(image.img['src'])
            parsed_post['internal_image_urls'] = image_urls
            
            media_urls =[]
            video_hashes =[]
            medias = post.find_all("span", {'class': "mc-video--link"})
            if len(medias) > 0:
                for media in medias:
                    media_urls.append(list(media.a.children)[-1].strip())
                    video_hashes.append(media_urls[-1].split('/')[-1].replace('.mp4', ''))
            parsed_post['internal_media_urls'] = media_urls
            parsed_post['internal_video_hash'] = video_hashes
                
            footer =post.find("div", {'class': "post--actions--row pa--main-row p--flex pf--ac pf--jsb"})
            try:
                footer_counts = footer.find_all("span", {'class': "pa--item--count"})
                parsed_post['comments_count'] = int(footer_counts[0].string)
                parsed_post['echoes_count'] = int(footer_counts[1].string)
                parsed_post['upvotes_count'] = int(footer_counts[2].string)
            except:
                parsed_post['comments_count'] = None
                parsed_post['echoes_count'] = None
                parsed_post['upvotes_count'] = None
        
            yield parsed_post

   
zipped_files = '''Path to your Data (https://ddosecrets.com/wiki/Parler)'''
  
parler_data = pd.DataFrame(parse_parler_archive(zipped_files))

print("total number of processed posts: {}".format(parler_data.shape))
