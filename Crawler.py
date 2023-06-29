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
import io
from selenium import webdriver
from selenium.webdriver.common.by import By

import CrawlingManager


class crawler(Thread):

    def __init__(self, itemLinks, category, subCategory):
        Thread.__init__(self)
        self.myLinks = itemLinks
        self.category = category
        self.subCategory = subCategory

    def run(self):
        itemCategory = {}
        itemSubCategory = {}
        storeReview = {}
        productReview = {}
        productAmountOfSales = {}
        productPrice = {}
        productHighlights = {}
        productDescription = {}
        StarSeller = {}
        numReviewers = {}
        options = webdriver.ChromeOptions()
        options.headless = True
        loadedDriver = False
        try:
            itemPage = webdriver.Chrome(options=options)
            itemPage.minimize_window()
            loadedDriver = True
        except:
            loadedDriver = False
        if loadedDriver:
            try:
                for link in self.myLinks:
                    itemPage.get(link)
                    cartColObj = itemPage.find_element(By.CLASS_NAME, "cart-col")
                    itemTitle = cartColObj.find_element(By.CLASS_NAME, "wt-break-word").text  # item title

                    # itemStoreReview
                    try:
                        itemStoreReview = itemPage.find_element(By.ID, "reviews")\
                            .find_element(By.NAME, "rating").get_attribute("value")
                    except:
                        itemStoreReview = "N/A"

                    # itemProductAmountOfSales
                    try:
                        itemProductAmountOfSales = itemPage.find_element(By.ID, "same-listing-reviews-tab") \
                            .find_element(By.CLASS_NAME, "wt-badge") \
                            .text  # text
                    except:
                        itemProductAmountOfSales = "N/A"

                    # itemPrice
                    try:
                        itemPrice = cartColObj.find_element(By.XPATH, "//div[@data-buy-box-region]") \
                            .find_element(By.CLASS_NAME, "wt-text-title-03").text.replace("Price:\n", "")  # text
                    except:
                        itemPrice = "N/A"

                    # itemProductHighlights
                    try:
                        itemProductHighlights = itemPage.find_element(By.CLASS_NAME, "info-col") \
                            .find_element(By.ID, "product-details-content-toggle") \
                            .find_elements(By.CLASS_NAME,
                                           "wt-list-unstyled")  # highlights, for each element do find_element
                        listItemHighlights = []
                        for highlight in itemProductHighlights:
                            listItemHighlights.append(highlight.find_element(By.CLASS_NAME, "wt-ml-xs-2").text)
                        itemHighlightsString = ",".join(listItemHighlights)
                    except:
                        itemHighlightsString = "N/A"

                    # itemProductDescription
                    try:
                        itemProductDescription = itemPage.find_element(By.ID,
                                                                       "wt-content-toggle-product-details-read-more") \
                            .find_element(By.CLASS_NAME,
                                           "wt-text-body-01").text  # item text descriptions
                    except:
                        itemProductDescription = "N/A"

                    # itemIsStarSeller
                    try:
                        itemIsStarSeller = cartColObj.find_element(By.CLASS_NAME, "star-seller-badge-listing-page")  # is star seller
                    except:
                        itemIsStarSeller = None

                    # itemProductNumReviewers
                    try:
                        itemProductNumReviewers = itemPage.find_element(By.ID, "reviews-tab-list")\
                            .find_element(By.ID, "same-listing-reviews-tab")\
                            .find_element(By.CLASS_NAME, "wt-badge").text
                    except:
                        itemProductNumReviewers = "N/A"

                    # itemProductReview
                    try:
                        itemProductReviewCards = itemPage.find_element(By.ID, "same-listing-reviews-panel") \
                            .find_elements(By.CLASS_NAME, "review-card")
                        if len(itemProductReviewCards) > 0:
                            itemProductReview = itemProductReviewCards[0].find_element(By.NAME,
                                                                                       "rating").get_attribute(
                                "value")
                        else:
                            itemProductReview = "N/A"
                    except:
                        itemProductReview = "N/A"

                    itemCategory[itemTitle] = self.category
                    itemSubCategory[itemTitle] = self.subCategory
                    storeReview[itemTitle] = itemStoreReview
                    productReview[itemTitle] = itemProductReview
                    productAmountOfSales[itemTitle] = itemProductAmountOfSales
                    productPrice[itemTitle] = itemPrice
                    productHighlights[itemTitle] = itemHighlightsString
                    productDescription[itemTitle] = itemProductDescription
                    StarSeller[itemTitle] = (itemIsStarSeller is not None)
                    numReviewers[itemTitle] = itemProductNumReviewers
                    df = pd.DataFrame({"category": itemCategory,
                                       "subCategory": itemSubCategory,
                                       "storeReview": storeReview,
                                       "productReview": productReview,
                                       "productAmountOfSales": productAmountOfSales,
                                       "productPrice": productPrice,
                                       "productHighlights": productHighlights,
                                       "productDescription": productDescription,
                                       "StarSeller": StarSeller,
                                       "numReviewers": numReviewers})
                    CrawlingManager.dataframesToWrite.append(df)
            except Exception as ex:
                print("got exception for " + link)
                print(ex)
            finally:
                itemPage.quit()
                CrawlingManager.decreaseCurrentThreads()
