import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys

from Resources.Locators import Locators
from Resources.TestData import TestData


class BasePage():
    """This class is the parent class for all the pages in our application."""
    """It contains all common elements and functionalities available to all pages."""

    # this function is called every time a new object of the base class is created.
    def __init__(self, driver):
        self.driver = driver

    # this function performs click on web element whose locator is passed to it.
    def click(self, by_locator):
        WebDriverWait(self.driver, 10).until(EC.visibility_of_element_located(by_locator)).click()

    # this function asserts comparison of a web element's text with passed in text.
    def assert_element_text(self, by_locator, element_text):
        web_element = WebDriverWait(self.driver, 10).until(EC.visibility_of_element_located(by_locator))
        assert web_element.text == element_text

    # this function performs text entry of the passed in text, in a web element whose locator is passed to it.
    def enter_text(self, by_locator, text):
        return WebDriverWait(self.driver, 10).until(EC.visibility_of_element_located(by_locator)).send_keys(text)

    # this function checks if the web element whose locator has been passed to it, is enabled or not and returns
    # web element if it is enabled.
    def is_enabled(self, by_locator):
        return WebDriverWait(self.driver, 10).until(EC.visibility_of_element_located(by_locator))

    # this function checks if the web element whose locator has been passed to it, is visible or not and returns
    # true or false depending upon its visibility.
    def is_visible(self, by_locator):
        element = WebDriverWait(self.driver, 10).until(EC.visibility_of_element_located(by_locator))
        return bool(element)

    # this function moves the mouse pointer over a web element whose locator has been passed to it.
    def hover_to(self, by_locator):
        element = WebDriverWait(self.driver, 10).until(EC.visibility_of_element_located(by_locator))
        ActionChains(self.driver).move_to_element(element).perform()








class HomePage(BasePage):
    """Home Page of Amazon India"""

    def __init__(self, driver):
        super().__init__(driver)
        self.driver.get(TestData.BASE_URL)

    def search(self):
        self.driver.find_element(*Locators.SEARCH_TEXTBOX).clear()
        self.enter_text(Locators.SEARCH_TEXTBOX, TestData.SEARCH_TERM)
        #self.click(Locators.SEARCH_SUBMIT_BUTTON)


class SearchResultsPage(BasePage):
    """Search Results Page of Amazon India"""

    def __init__(self, driver):
        super().__init__(driver)

    def click_search_result(self):
        self.click(Locators.SEARCH_RESULT_LINK)


class ProductDetailsPage(BasePage):
    """Product Details Page for the clicked product on Amazon India"""

    def __init__(self, driver):
        super().__init__(driver)

    def click_add_to_cart_button(self):
        self.click(Locators.ADD_TO_CART_BUTTON)


class SubCartPage(BasePage):
    """Sub Cart Page on Amazon India"""

    def __init__(self, driver):
        super().__init__(driver)

    def click_cart_link(self):
        self.click(Locators.CART_LINK)


class CartPage(BasePage):
    """Cart Page on Amazon India"""

    def __init__(self, driver):
        super().__init__(driver)

    def delete_item(self):
        cartCount = int(self.driver.find_element(*Locators.CART_COUNT).text)
        # print ("Cart Count is"+ str(cartCount))
        if (cartCount < 1):
            print("Cart is empty")
            exit()
        if (self.driver.title.startswith("Amazon.in Shopping Cart")):
            # to delete an item from the Cart
            self.click(Locators.DELETE_ITEM_LINK)
            time.sleep(2)

    def click_proceed_to_checkout_button(self):
        self.click(Locators.PROCEED_TO_CHECKOUT_BUTTON)


class SignInPage(BasePage):
    """SignIn Page on Amazon India"""

    def __init__(self, driver):
        super().__init__(driver)