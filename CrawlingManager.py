# imports section
import threading
from threading import Thread
import pandas as pd
import numpy as np
import scipy as sc
import bs4
from bs4 import BeautifulSoup
import requests
import json
import os
import selenium
from selenium import webdriver
import time
from time import sleep
import io
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By

from Crawler import crawler

MAX_THREAD_COUNT = 3
currentThreads = 0
baseURL = 'https://www.etsy.com/il-en/c/clothing/mens-clothing'
csvFilePath = 'C:\\Temp1'
csvFileName = 'data_from_site_shorts.csv'
csvFullPath = csvFilePath + "\\" + csvFileName
dataframesToWrite = []
finishedCrawling = False

def __init__(self):
    global currentThreads
    currentThreads = 0
def getNumCurrentThreads():
    global currentThreads
    return currentThreads

def increaseCurrentThreads():
    global currentThreads
    currentThreads = currentThreads + 1

def decreaseCurrentThreads():
    global currentThreads
    currentThreads -= 1
def keepWritingDfToCsv():
    global finishedCrawling, dataframesToWrite, csvFullPath
    while not finishedCrawling:
        while len(dataframesToWrite) > 0:
            df = pd.DataFrame(dataframesToWrite.pop(0))
            df.to_csv(csvFullPath, mode='a', index=False, header=False)
        sleep(4)

def writeHeadersToFile():
    df = pd.DataFrame({"category": [],
                       "subCategory": [],
                       "storeReview": [],
                       "productRating": [],
                       "productAmountOfBuy": [],
                       "productPrice": [],
                       "productHighlights": [],
                       "productDescription": [],
                       "StarSeller": [],
                       "numReviewers": []})
    df.to_csv(csvFullPath, index=False)
def runManager():
    global finishedCrawling
    writeHeadersToFile()
    writerThread = Thread(target=keepWritingDfToCsv)
    writerThread.start()
    options = webdriver.ChromeOptions()
    options.headless = True                                 #false
    mainPage = webdriver.Chrome(options=options)
    categoryPage = webdriver.Chrome(options=options)
    subCategoryPage = webdriver.Chrome(options=options)
    mainPage.get(baseURL)
    listCategories = mainPage.find_elements(By.CLASS_NAME, "parent-hover-underline")
    totalCategories = len(listCategories)
    currCategory = 1
    for category in listCategories:
        now = datetime.now()
        dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
        print("Current Date: {}, Total categories: {}, Current Category: {}".format(dt_string, totalCategories, currCategory))
        currCategory += 1
        catecorylink = category.get_attribute('href')
        categoryName = category.find_element(By.CLASS_NAME, "child-hover-underline").text
        categoryPage.get(catecorylink)
        subCategories = categoryPage.find_elements(By.CLASS_NAME, "parent-hover-underline")
        totalSubcategories = len(subCategories)
        currSubcategory = 1
        for subCategory in subCategories:
            now = datetime.now()
            dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
            print("Current Date: {}, Total Sub Categories: {}, Current Sub Category: {}".format(dt_string, totalSubcategories,
                                                                                        currSubcategory))
            currSubcategory += 1
            currPage = 1
            subCategorylink = subCategory.get_attribute('href')
            subCategoryName = subCategory.find_element(By.CLASS_NAME, "child-hover-underline").text
            subCategoryPage.get(subCategorylink)
            pagesElement = subCategoryPage.find_element(By.CLASS_NAME, "search-pagination") \
                .find_elements(By.CLASS_NAME, "wt-action-group__item-container")
            try:
                nextPageElement = pagesElement[-1].find_element(By.CLASS_NAME, "wt-btn").get_attribute('href')
            except:
                nextPageElement = None
            while nextPageElement is not None:
                items = subCategoryPage.find_element(By.CLASS_NAME, "tab-reorder-container").find_elements(
                    By.CLASS_NAME, "wt-list-unstyled")
                itemLinks = []
                for item in items:
                    itemLinks.append(item.find_element(By.CLASS_NAME, "listing-link").get_attribute('href'))
                items = None
                now = datetime.now()
                dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
                print("Current Time: {}, current page: {}".format(dt_string, currPage))
                currPage += 1
                if getNumCurrentThreads() < MAX_THREAD_COUNT:
                    increaseCurrentThreads()
                    itemCrawler = crawler(itemLinks, categoryName, subCategoryName)
                    itemCrawler.start()
                else:
                    while getNumCurrentThreads() >= MAX_THREAD_COUNT:
                        sleep(1)
                    increaseCurrentThreads()
                    itemCrawler = crawler(itemLinks, categoryName, subCategoryName)
                    itemCrawler.start()
                pagesElement = subCategoryPage.find_element(By.CLASS_NAME, "search-pagination") \
                    .find_elements(By.CLASS_NAME, "wt-action-group__item-container")
                try:
                    nextPageElement = pagesElement[-1].find_element(By.CLASS_NAME, "wt-btn").get_attribute('href')
                    print("next page element href is: " + nextPageElement)
                except:
                    nextPageElement = None
                else:
                    subCategoryPage.get(nextPageElement)
    subCategoryPage.close()
    categoryPage.close()
    mainPage.close()
    finishedCrawling = True











