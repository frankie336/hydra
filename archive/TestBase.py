import unittest
from selenium import webdriver
from Resources.Locators import Locators
from Resources.PO.Pages import HomePage, SearchResultsPage, ProductDetailsPage, SubCartPage, CartPage, SignInPage
from Resources.TestData import TestData


# Base Class for the tests
class Test_AMZN_Search_Base(unittest.TestCase):

    def setUp(self):
        # Setting up how we want Chrome to run
        chrome_options = webdriver.ChromeOptions()
        self.driver = webdriver.Chrome(TestData.CHROME_EXECUTABLE_PATH, options=chrome_options)
        # browser should be loaded in maximized window
        self.driver.maximize_window()

    def tearDown(self):
        # To do the cleanup after test has executed.
        self.driver.close()
        self.driver.quit()


# Test class containing methods corresponding to testcases.
class Test_AMZN_Search(Test_AMZN_Search_Base):
    def setUp(self):
        # to call the setUp() method of base class or super class.
        super().setUp()

    def test_home_page_loaded_successfully(self):
        # instantiate an object of HomePage class. Remember when the constructor of HomePage class is called
        # it opens up the browser and navigates to Home Page of the site under test.
        self.homePage = HomePage(self.driver)
        # assert if the title of Home Page contains Amazon.in
        self.assertIn(TestData.HOME_PAGE_TITLE, self.homePage.driver.title)