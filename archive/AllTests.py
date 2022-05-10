import unittest
import HtmlTestRunner
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

import os, sys, inspect
# fetch path to the directory in which current file is, from root directory or C:\ (or whatever driver number it is)
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
# extract the path to parent directory
parentdir = os.path.dirname(currentdir)
# insert path to the folder from parent directory from which the python module/ file is to be imported
sys.path.insert(0, parentdir+'\Resources')
sys.path.insert(0, parentdir+'\Resources\PO')



from Locators import Locators
from TestData import TestData
from Resources.PO import Pages
from Resources.PO.Pages import HomePage, SearchResultsPage, ProductDetailsPage, SubCartPage, CartPage, SignInPage


# Base Class for the tests
class Test_AMZN_Search_Base(unittest.TestCase):

    def setUp(self):
        # Setting up how we want Chrome to run
        chrome_options = webdriver.ChromeOptions()

        #self.driver = webdriver.Chrome(TestData.CHROME_EXECUTABLE_PATH, options=chrome_options)

        self.driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))

        # browser should be loaded in maximized window
        self.driver.maximize_window()

    def tearDown(self):
        # To do the cleanup after test has executed.
        self.driver.close()
        self.driver.quit()
        pass

class Test_AMZN_Search(Test_AMZN_Search_Base):

        def setUp(self):
        #to call the setUp() method of base class or super class.
            super().setUp()

        def test_home_page_loaded_successfully(self):
            # instantiate an object of HomePage class. Remember when the constructor of HomePage class is called
            # it opens up the browser and navigates to Home Page of the site under test.
            self.homePage = HomePage(self.driver)
            # assert if the title of Home Page contains Amazon.in
            self.assertIn(TestData.HOME_PAGE_TITLE, self.homePage.driver.title)

        def test_user_should_be_able_to_search(self):
            self.homePage = HomePage(self.driver)
            # search for the search term on Home Page. The search term would be picked from
            # test data file
            self.homePage.search()
            # instantiate an object of SearchResultsPage class passing in the driver as parameter.
            # this will allow the newly created object to have access to the browser and perform
            # operations further.
            #self.searchResultsPage = SearchResultsPage(self.homePage.driver)
            # assert if the search term is present in the title of the Amazon's Search Results Page
            #self.assertIn(TestData.SEARCH_TERM, self.searchResultsPage.driver.title)
            # assert that the search term indeed return some results.
            #self.assertNotIn(TestData.NO_RESULTS_TEXT, self.searchResultsPage.driver.page_source)





if __name__ == '__main__':
    # specify path where the HTML reports for testcase execution are to be generated
    unittest.main(testRunner=HtmlTestRunner.HTMLTestRunner(output='/Reports'))